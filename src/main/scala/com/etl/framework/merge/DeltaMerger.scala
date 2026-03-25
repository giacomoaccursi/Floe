package com.etl.framework.merge

import com.etl.framework.config.{LoadMode, LoadModeConfig}
import org.apache.spark.sql.DataFrame
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
        new UpsertMerger(primaryKey)

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

/** Upsert merger - updates existing records and inserts new ones
  */
class UpsertMerger(
    keyColumns: Seq[String]
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
        mergeWithoutTimestamp(newData, existing)
    }
  }

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

    // Cache existing: used twice (existingCurrent for join, historicalRecords for union)
    val cachedExisting = existing.cache()

    try {
      val existingCurrent = cachedExisting.filter(col(isCurrentCol) === true)

      // Join on key columns to find matches
      val joinCondition = keyColumns
        .map(c => col(s"new.$c") === col(s"existing.$c"))
        .reduce(_ && _)

      // Single full-outer join — all output parts are derived directly from here.
      // Cache since it is used 4-5 times for different filter+select operations.
      val joined = newData
        .alias("new")
        .join(
          existingCurrent.alias("existing"),
          joinCondition,
          "full_outer"
        )
        .cache()

      try {
        // Null-safe change detection: at least one compare column differs
        val changeCondition = compareColumns
          .map(c => !col(s"new.$c").eqNullSafe(col(s"existing.$c")))
          .reduce(_ || _)

        // Presence detection based on first key column (PKs are non-null by definition)
        val newPresent      = col(s"new.${keyColumns.head}").isNotNull
        val existingPresent = col(s"existing.${keyColumns.head}").isNotNull

        // Column selectors: project full rows from each side of the join
        val existingCols = existingCurrent.columns.map(c => col(s"existing.$c").alias(c))
        val newDataCols  = newData.columns.map(c => col(s"new.$c").alias(c))

        // 1. Close old versions of changed records (existing columns, updated SCD2 metadata)
        val closedRecords = joined
          .filter(newPresent && existingPresent && changeCondition)
          .select(existingCols: _*)
          .withColumn(validToCol, batchTimestamp)
          .withColumn(isCurrentCol, lit(false))

        // 2. New versions of changed records (new data columns, current SCD2 metadata)
        val baseNewVersions = joined
          .filter(newPresent && existingPresent && changeCondition)
          .select(newDataCols: _*)
          .withColumn(validFromCol, batchTimestamp)
          .withColumn(validToCol, lit(null).cast("timestamp"))
          .withColumn(isCurrentCol, lit(true))
        val newVersions =
          isActiveCol.fold(baseNewVersions)(c => baseNewVersions.withColumn(c, lit(true)))

        // 3. Brand-new records: key doesn't exist in existing
        val baseNewRecords = joined
          .filter(!existingPresent)
          .select(newDataCols: _*)
          .withColumn(validFromCol, batchTimestamp)
          .withColumn(validToCol, lit(null).cast("timestamp"))
          .withColumn(isCurrentCol, lit(true))
        val newRecords =
          isActiveCol.fold(baseNewRecords)(c => baseNewRecords.withColumn(c, lit(true)))

        // 4. Unchanged records: keep existing row as-is
        val unchangedRecords = joined
          .filter(newPresent && existingPresent && !changeCondition)
          .select(existingCols: _*)

        // 5. Records missing from source
        val missingRecords = if (detectDeletes) {
          // Soft-delete: close current version and mark inactive
          val closed = joined
            .filter(!newPresent && existingPresent)
            .select(existingCols: _*)
            .withColumn(validToCol, batchTimestamp)
            .withColumn(isCurrentCol, lit(false))
          isActiveCol.fold(closed)(c => closed.withColumn(c, lit(false)))
        } else {
          // Keep as-is (not deleted)
          joined
            .filter(!newPresent && existingPresent)
            .select(existingCols: _*)
        }

        // 6. Historical (non-current) records: not part of the join since only
        //    existingCurrent was joined; retrieved directly from cachedExisting
        val historicalRecords = cachedExisting.filter(col(isCurrentCol) === false)

        historicalRecords
          .unionByName(closedRecords)
          .unionByName(unchangedRecords)
          .unionByName(newVersions)
          .unionByName(newRecords)
          .unionByName(missingRecords)
      } finally {
        joined.unpersist()
      }
    } finally {
      cachedExisting.unpersist()
    }
  }
}
