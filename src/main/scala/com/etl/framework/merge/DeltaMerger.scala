package com.etl.framework.merge

import com.etl.framework.merge.MergeColumns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.etl.framework.config.LoadModeConfig

/**
 * Handles merge of incremental data with existing data
 */
trait DeltaMerger {
  /**
   * Merges new data with existing data according to the merge strategy
   * 
   * @param newData The new data to merge
   * @param existingData Optional existing data (None for first load)
   * @return Merged DataFrame
   */
  def merge(newData: DataFrame, existingData: Option[DataFrame]): DataFrame
}

/**
 * Factory for creating DeltaMerger instances based on load mode configuration
 */
object DeltaMergerFactory {
  
  /**
   * Creates a DeltaMerger based on the load mode configuration
   * 
   * @param loadMode The load mode configuration
   * @return A DeltaMerger instance
   */
  def create(loadMode: LoadModeConfig): DeltaMerger = {
    loadMode.`type` match {
      case "delta" =>
        loadMode.mergeStrategy match {
          case Some("upsert") =>
            new UpsertMerger(loadMode.keyColumns, loadMode.updateTimestampColumn)
          case Some("append") =>
            new AppendMerger()
          case other =>
            throw new IllegalArgumentException(
              s"Invalid merge strategy: $other. Valid values are: upsert, append"
            )
        }
      
      case "scd2" =>
        require(loadMode.validFromColumn.isDefined, "validFromColumn is required for SCD2 mode")
        require(loadMode.validToColumn.isDefined, "validToColumn is required for SCD2 mode")
        require(loadMode.isCurrentColumn.isDefined, "isCurrentColumn is required for SCD2 mode")
        require(loadMode.keyColumns.nonEmpty, "keyColumns are required for SCD2 mode")
        require(loadMode.compareColumns.nonEmpty, "compareColumns are required for SCD2 mode")
        
        new SCD2Merger(
          loadMode.keyColumns,
          loadMode.compareColumns,
          loadMode.validFromColumn.get,
          loadMode.validToColumn.get,
          loadMode.isCurrentColumn.get
        )
      
      case "full" =>
        new FullReplaceMerger()
      
      case other =>
        throw new IllegalArgumentException(
          s"Invalid load mode type: $other. Valid values are: full, delta, scd2"
        )
    }
  }
}

/**
 * Full replace merger - simply replaces all existing data with new data
 */
class FullReplaceMerger extends DeltaMerger {
  override def merge(newData: DataFrame, existingData: Option[DataFrame]): DataFrame = {
    newData
  }
}

/**
 * Upsert merger - updates existing records and inserts new ones
 * Supports merge with and without timestamp
 */
class UpsertMerger(
  keyColumns: Seq[String],
  updateTimestampColumn: Option[String]
) extends DeltaMerger {
  
  require(keyColumns.nonEmpty, "keyColumns cannot be empty for upsert merge")
  
  override def merge(newData: DataFrame, existingData: Option[DataFrame]): DataFrame = {
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
  
  /**
   * Merges with timestamp - keeps the most recent record based on timestamp column
   */
  private def mergeWithTimestamp(
    newData: DataFrame,
    existing: DataFrame,
    tsCol: String
  ): DataFrame = {
    // Union both datasets
    val combined = existing.union(newData)
    
    // Create window partitioned by key columns, ordered by timestamp descending
    val windowSpec = org.apache.spark.sql.expressions.Window
      .partitionBy(keyColumns.map(col): _*)
      .orderBy(col(tsCol).desc)
    
    // Add row number and keep only the first (most recent) record for each key
    combined
      .withColumn(ROW_NUM, row_number().over(windowSpec))
      .filter(col(ROW_NUM) === 1)
      .drop(ROW_NUM)
  }
  
  /**
   * Merges without timestamp - new records always overwrite existing ones
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
    unchangedRecords.union(newData)
  }
}

/**
 * Append merger - simply appends new records to existing data without modification
 */
class AppendMerger extends DeltaMerger {
  override def merge(newData: DataFrame, existingData: Option[DataFrame]): DataFrame = {
    existingData match {
      case None =>
        // First load - return new data as-is
        newData
      case Some(existing) =>
        // Append new records to existing data
        existing.union(newData)
    }
  }
}

/**
 * SCD2 (Slowly Changing Dimension Type 2) merger
 * Maintains history by closing old versions and adding new versions
 */
class SCD2Merger(
  keyColumns: Seq[String],
  compareColumns: Seq[String],
  validFromCol: String,
  validToCol: String,
  isCurrentCol: String
) extends DeltaMerger {
  
  require(keyColumns.nonEmpty, "keyColumns cannot be empty for SCD2 merge")
  require(compareColumns.nonEmpty, "compareColumns cannot be empty for SCD2 merge")
  
  override def merge(newData: DataFrame, existingData: Option[DataFrame]): DataFrame = {
    existingData match {
      case None =>
        // First load - all records are current
        newData
          .withColumn(validFromCol, current_timestamp())
          .withColumn(validToCol, lit(null).cast("timestamp"))
          .withColumn(isCurrentCol, lit(true))
      
      case Some(existing) =>
        // Identify changed records, close old versions, add new versions
        historicizeChanges(newData, existing)
    }
  }
  
  /**
   * Identifies changed records and performs SCD2 historicization
   */
  private def historicizeChanges(
    newData: DataFrame,
    existing: DataFrame
  ): DataFrame = {
    // Get current timestamp for this batch
    val batchTimestamp = current_timestamp()
    
    // Join new data with existing current records to identify changes
    val existingCurrent = existing.filter(col(isCurrentCol) === true)
    
    // Create a comparison key from compare columns
    val newDataWithCompareKey = newData.withColumn(
      "_compare_key",
      concat_ws("||", compareColumns.map(col): _*)
    )
    
    val existingWithCompareKey = existingCurrent.withColumn(
      "_compare_key",
      concat_ws("||", compareColumns.map(col): _*)
    )
    
    // Join on key columns to find matches
    val joinCondition = keyColumns
      .map(c => col(s"new.$c") === col(s"existing.$c"))
      .reduce(_ && _)
    
    val joined = newDataWithCompareKey.alias("new")
      .join(
        existingWithCompareKey.alias("existing"),
        joinCondition,
        "full_outer"
      )
    
    // Identify different scenarios:
    // 1. Changed records: key matches but compare key differs
    val changedRecords = joined
      .filter(
        col(s"new.$COMPARE_KEY").isNotNull &&
        col(s"existing.$COMPARE_KEY").isNotNull &&
        col(s"new.$COMPARE_KEY") =!= col(s"existing.$COMPARE_KEY")
      )
      .select(keyColumns.map(c => col(s"existing.$c").alias(c)): _*)
    
    // 2. New records: key doesn't exist in existing
    val newRecords = joined
      .filter(col(s"existing.$COMPARE_KEY").isNull)
      .select(
        newData.columns.map(c => col(s"new.$c").alias(c)): _*
      )
      .withColumn(validFromCol, batchTimestamp)
      .withColumn(validToCol, lit(null).cast("timestamp"))
      .withColumn(isCurrentCol, lit(true))
    
    // 3. Unchanged records: key matches and compare key matches
    val unchangedKeys = joined
      .filter(
        col(s"new.$COMPARE_KEY").isNotNull &&
        col(s"existing.$COMPARE_KEY").isNotNull &&
        col(s"new.$COMPARE_KEY") === col(s"existing.$COMPARE_KEY")
      )
      .select(keyColumns.map(c => col(s"existing.$c").alias(c)): _*)
    
    // Close old versions of changed records
    val closedRecords = existing
      .join(changedRecords, keyColumns, "inner")
      .filter(col(isCurrentCol) === true)
      .withColumn(validToCol, batchTimestamp)
      .withColumn(isCurrentCol, lit(false))
    
    // Add new versions of changed records
    val newVersions = newData
      .join(changedRecords, keyColumns, "inner")
      .withColumn(validFromCol, batchTimestamp)
      .withColumn(validToCol, lit(null).cast("timestamp"))
      .withColumn(isCurrentCol, lit(true))
    
    // Keep unchanged current records as-is
    val unchangedRecords = existing
      .join(unchangedKeys, keyColumns, "inner")
      .filter(col(isCurrentCol) === true)
    
    // Keep all historical (non-current) records
    val historicalRecords = existing.filter(col(isCurrentCol) === false)
    
    // Union all parts together
    historicalRecords
      .union(closedRecords)
      .union(unchangedRecords)
      .union(newVersions)
      .union(newRecords)
  }
}
