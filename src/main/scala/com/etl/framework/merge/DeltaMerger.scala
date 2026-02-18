package com.etl.framework.merge

import com.etl.framework.config.{LoadMode, LoadModeConfig}
import com.etl.framework.merge.MergeColumns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Handles merge of incremental data with existing data
  */
trait DeltaMerger {

  /** Merges new data with existing data according to the merge strategy
    *
    * @param newData
    *   The new data to merge
    * @param existingData
    *   Optional existing data (None for first load)
    * @return
    *   Merged DataFrame
    */
  def merge(newData: DataFrame, existingData: Option[DataFrame]): DataFrame
}

/** Factory for creating DeltaMerger instances based on load mode configuration.
  * Used only for the Parquet fallback pipeline (non-Iceberg).
  */
object DeltaMergerFactory {

  def create(loadMode: LoadModeConfig, primaryKey: Seq[String]): DeltaMerger = {
    loadMode.`type` match {
      case LoadMode.Delta =>
        new UpsertMerger(primaryKey, loadMode.updateTimestampColumn)

      case LoadMode.SCD2 =>
        require(
          loadMode.validFromColumn.isDefined,
          "validFromColumn is required for SCD2 mode"
        )
        require(
          loadMode.validToColumn.isDefined,
          "validToColumn is required for SCD2 mode"
        )
        require(
          loadMode.isCurrentColumn.isDefined,
          "isCurrentColumn is required for SCD2 mode"
        )
        require(primaryKey.nonEmpty, "primaryKey is required for SCD2 mode")
        require(
          loadMode.compareColumns.nonEmpty,
          "compareColumns are required for SCD2 mode"
        )

        val isActiveCol =
          if (loadMode.detectDeletes)
            Some(loadMode.isActiveColumn.getOrElse("is_active"))
          else
            loadMode.isActiveColumn

        new SCD2Merger(
          primaryKey,
          loadMode.compareColumns,
          loadMode.validFromColumn.get,
          loadMode.validToColumn.get,
          loadMode.isCurrentColumn.get,
          loadMode.detectDeletes,
          isActiveCol
        )

      case LoadMode.Full =>
        throw new IllegalArgumentException(
          "Full load mode does not use merge - merge is skipped entirely"
        )
    }
  }
}

/** Upsert merger - updates existing records and inserts new ones Supports merge
  * with and without timestamp
  */
class UpsertMerger(
    keyColumns: Seq[String],
    updateTimestampColumn: Option[String]
) extends DeltaMerger {

  require(keyColumns.nonEmpty, "keyColumns cannot be empty for upsert merge")

  override def merge(
      newData: DataFrame,
      existingData: Option[DataFrame]
  ): DataFrame = {
    existingData match {
      case None =>
        // First load - return new data as-is
        newData

      case Some(existing) =>
        updateTimestampColumn match {
          case Some(tsCol) =>
            // Merge with timestamp: take most recent record
            mergeWithTimestamp(newData, existing, tsCol)
          case None =>
            // Merge without timestamp: new records always overwrite
            mergeWithoutTimestamp(newData, existing)
        }
    }
  }

  /** Merges with timestamp - keeps the most recent record based on timestamp
    * column
    */
  private def mergeWithTimestamp(
      newData: DataFrame,
      existing: DataFrame,
      tsCol: String
  ): DataFrame = {
    // Union both datasets
    val combined = existing.unionByName(newData)

    // Create window partitioned by key columns, ordered by timestamp descending
    val windowSpec = Window
      .partitionBy(keyColumns.map(col): _*)
      .orderBy(col(tsCol).desc)

    // Add row number and keep only the first (most recent) record for each key
    combined
      .withColumn(ROW_NUM, row_number().over(windowSpec))
      .filter(col(ROW_NUM) === 1)
      .drop(ROW_NUM)
  }

  /** Merges without timestamp - new records always overwrite existing ones
    */
  private def mergeWithoutTimestamp(
      newData: DataFrame,
      existing: DataFrame
  ): DataFrame = {
    // Keep existing records that don't have a match in new data (left anti join)
    val unchangedRecords = existing.join(
      newData.select(keyColumns.map(col): _*),
      keyColumns,
      "left_anti"
    )

    // Union with all new records (which will overwrite any matching keys)
    unchangedRecords.unionByName(newData)
  }
}

/** SCD2 (Slowly Changing Dimension Type 2) merger Maintains history by closing
  * old versions and adding new versions
  */
class SCD2Merger(
    keyColumns: Seq[String],
    compareColumns: Seq[String],
    validFromCol: String,
    validToCol: String,
    isCurrentCol: String,
    detectDeletes: Boolean = false,
    isActiveCol: Option[String] = None
) extends DeltaMerger {

  require(keyColumns.nonEmpty, "keyColumns cannot be empty for SCD2 merge")
  require(
    compareColumns.nonEmpty,
    "compareColumns cannot be empty for SCD2 merge"
  )

  override def merge(
      newData: DataFrame,
      existingData: Option[DataFrame]
  ): DataFrame = {
    existingData match {
      case None =>
        // First load - all records are current and active
        val base = newData
          .withColumn(validFromCol, current_timestamp())
          .withColumn(validToCol, lit(null).cast("timestamp"))
          .withColumn(isCurrentCol, lit(true))
        isActiveCol.fold(base)(c => base.withColumn(c, lit(true)))

      case Some(existing) =>
        // Identify changed records, close old versions, add new versions
        historicizeChanges(newData, existing)
    }
  }

  /** Identifies changed records and performs SCD2 historicization
    */
  private def historicizeChanges(
      newData: DataFrame,
      existing: DataFrame
  ): DataFrame = {
    // Get current timestamp for this batch
    val batchTimestamp = current_timestamp()

    // Join new data with existing current records to identify changes
    val existingCurrent = existing.filter(col(isCurrentCol) === true)

    // Join on key columns to find matches
    val joinCondition = keyColumns
      .map(c => col(s"new.$c") === col(s"existing.$c"))
      .reduce(_ && _)

    val joined = newData
      .alias("new")
      .join(
        existingCurrent.alias("existing"),
        joinCondition,
        "full_outer"
      )

    // Null-safe change detection: at least one compare column differs
    val changeCondition = compareColumns
      .map(c => !col(s"new.$c").eqNullSafe(col(s"existing.$c")))
      .reduce(_ || _)

    // Presence detection based on first key column (PKs are non-null by definition)
    val newPresent = col(s"new.${keyColumns.head}").isNotNull
    val existingPresent = col(s"existing.${keyColumns.head}").isNotNull

    // 1. Changed records: key matches but at least one compare column differs
    val changedRecords = joined
      .filter(newPresent && existingPresent && changeCondition)
      .select(keyColumns.map(c => col(s"existing.$c").alias(c)): _*)

    // 2. New records: key doesn't exist in existing
    val baseNewRecords = joined
      .filter(!existingPresent)
      .select(
        newData.columns.map(c => col(s"new.$c").alias(c)): _*
      )
      .withColumn(validFromCol, batchTimestamp)
      .withColumn(validToCol, lit(null).cast("timestamp"))
      .withColumn(isCurrentCol, lit(true))
    val newRecords =
      isActiveCol.fold(baseNewRecords)(c =>
        baseNewRecords.withColumn(c, lit(true))
      )

    // 3. Unchanged records: key matches and all compare columns equal
    val unchangedKeys = joined
      .filter(newPresent && existingPresent && !changeCondition)
      .select(keyColumns.map(c => col(s"existing.$c").alias(c)): _*)

    // 4. Records in existing but missing from new source
    val missingFromSourceKeys = joined
      .filter(!newPresent && existingPresent)
      .select(
        keyColumns.map(c => col(s"existing.$c").alias(c)): _*
      )

    // Close old versions of changed records
    val closedRecords = existing
      .join(changedRecords, keyColumns, "inner")
      .filter(col(isCurrentCol) === true)
      .withColumn(validToCol, batchTimestamp)
      .withColumn(isCurrentCol, lit(false))

    // Add new versions of changed records
    val baseNewVersions = newData
      .join(changedRecords, keyColumns, "inner")
      .withColumn(validFromCol, batchTimestamp)
      .withColumn(validToCol, lit(null).cast("timestamp"))
      .withColumn(isCurrentCol, lit(true))
    val newVersions =
      isActiveCol.fold(baseNewVersions)(c =>
        baseNewVersions.withColumn(c, lit(true))
      )

    // Keep unchanged current records as-is
    val unchangedRecords = existing
      .join(unchangedKeys, keyColumns, "inner")
      .filter(col(isCurrentCol) === true)

    // Keep all historical (non-current) records
    val historicalRecords = existing.filter(col(isCurrentCol) === false)

    // Handle records missing from source
    val missingRecords = if (detectDeletes) {
      // Soft-delete: close them with is_active=false
      val closed = existing
        .join(missingFromSourceKeys, keyColumns, "inner")
        .filter(col(isCurrentCol) === true)
        .withColumn(validToCol, batchTimestamp)
        .withColumn(isCurrentCol, lit(false))
      isActiveCol.fold(closed)(c => closed.withColumn(c, lit(false)))
    } else {
      // Keep as-is (not deleted)
      existing
        .join(missingFromSourceKeys, keyColumns, "inner")
        .filter(col(isCurrentCol) === true)
    }

    // Union all parts together
    historicalRecords
      .unionByName(closedRecords)
      .unionByName(unchangedRecords)
      .unionByName(newVersions)
      .unionByName(newRecords)
      .unionByName(missingRecords)
  }
}
