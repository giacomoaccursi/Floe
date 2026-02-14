package com.etl.framework.iceberg

import com.etl.framework.config.{FlowConfig, IcebergConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

case class WriteResult(
    recordsWritten: Long,
    snapshotId: Option[Long],
    icebergMetadata: Option[IcebergFlowMetadata] = None
)

class IcebergTableWriter(
    spark: SparkSession,
    icebergConfig: IcebergConfig,
    tableManager: IcebergTableManager
) {

  private val logger = LoggerFactory.getLogger(getClass)

  def writeFullLoad(
      df: DataFrame,
      flowConfig: FlowConfig
  ): WriteResult = {
    val tableName = tableManager.resolveTableName(flowConfig)
    tableManager.createOrUpdateTable(flowConfig, df.schema)

    val recordCount = df.count()
    logger.info(s"Writing full load to $tableName: $recordCount records")

    df.writeTo(tableName).overwritePartitions()

    val snapshotId = tableManager.getCurrentSnapshotId(flowConfig)
    snapshotId.foreach { sid =>
      logger.info(s"Full load complete on $tableName, snapshot: $sid")
    }

    WriteResult(recordCount, snapshotId)
  }

  def writeDeltaLoad(
      df: DataFrame,
      flowConfig: FlowConfig
  ): WriteResult = {
    val tableName = tableManager.resolveTableName(flowConfig)
    tableManager.createOrUpdateTable(flowConfig, df.schema)

    val recordCount = df.count()
    val pkColumns = flowConfig.validation.primaryKey

    if (pkColumns.isEmpty) {
      logger.warn(s"No primary key defined for $tableName, falling back to append")
      df.writeTo(tableName).append()
    } else {
      val mergeCondition = pkColumns
        .map(col => s"target.$col = source.$col")
        .mkString(" AND ")

      val updateCols = df.columns
        .filterNot(pkColumns.contains)

      val updateSetClause = if (flowConfig.loadMode.updateTimestampColumn.isDefined) {
        val tsCol = flowConfig.loadMode.updateTimestampColumn.get
        val timestampCondition = s"source.$tsCol > target.$tsCol"
        updateCols
          .map(col => s"target.$col = source.$col")
          .mkString(", ")
          .replaceFirst("^", s"WHEN MATCHED AND $timestampCondition THEN UPDATE SET ")
      } else {
        updateCols
          .map(col => s"target.$col = source.$col")
          .mkString(", ")
          .replaceFirst("^", "WHEN MATCHED THEN UPDATE SET ")
      }

      val insertCols = df.columns.mkString(", ")
      val insertVals = df.columns.map(c => s"source.$c").mkString(", ")
      val insertClause =
        s"WHEN NOT MATCHED THEN INSERT ($insertCols) VALUES ($insertVals)"

      df.createOrReplaceTempView("_iceberg_merge_source")

      val mergeSql =
        s"""MERGE INTO $tableName AS target
           |USING _iceberg_merge_source AS source
           |ON $mergeCondition
           |$updateSetClause
           |$insertClause""".stripMargin

      logger.info(s"Executing MERGE INTO on $tableName")
      logger.debug(s"Merge SQL: $mergeSql")
      spark.sql(mergeSql)

      spark.catalog.dropTempView("_iceberg_merge_source")
    }

    val snapshotId = tableManager.getCurrentSnapshotId(flowConfig)
    snapshotId.foreach { sid =>
      logger.info(
        s"Delta upsert complete on $tableName: $recordCount records, snapshot: $sid"
      )
    }

    WriteResult(recordCount, snapshotId)
  }

  def writeSCD2Load(
      df: DataFrame,
      flowConfig: FlowConfig
  ): WriteResult = {
    val tableName = tableManager.resolveTableName(flowConfig)
    val pkColumns = flowConfig.validation.primaryKey
    val compareColumns = flowConfig.loadMode.compareColumns
    val validFromCol =
      flowConfig.loadMode.validFromColumn.getOrElse("valid_from")
    val validToCol = flowConfig.loadMode.validToColumn.getOrElse("valid_to")
    val isCurrentCol =
      flowConfig.loadMode.isCurrentColumn.getOrElse("is_current")

    // For SCD2, the schema includes the SCD2 columns
    val scd2Schema = df.schema
      .add(validFromCol, "timestamp")
      .add(validToCol, "timestamp")
      .add(isCurrentCol, "boolean")

    tableManager.createOrUpdateTable(flowConfig, scd2Schema)

    val recordCount = df.count()

    // Check if table has data
    val existingCount = spark.sql(s"SELECT COUNT(*) FROM $tableName").first().getLong(0)

    if (existingCount == 0) {
      // First load: all records are current
      logger.info(s"SCD2 initial load to $tableName: $recordCount records")

      df.createOrReplaceTempView("_iceberg_scd2_source")

      val columns = df.columns.mkString(", ")
      val insertSql =
        s"""INSERT INTO $tableName
           |SELECT $columns, current_timestamp() AS $validFromCol,
           |  CAST(NULL AS TIMESTAMP) AS $validToCol,
           |  true AS $isCurrentCol
           |FROM _iceberg_scd2_source""".stripMargin

      spark.sql(insertSql)
      spark.catalog.dropTempView("_iceberg_scd2_source")
    } else {
      // Subsequent load: detect changes, close old versions, insert new
      logger.info(s"SCD2 change detection on $tableName: $recordCount new records")

      df.createOrReplaceTempView("_iceberg_scd2_source")

      val mergeCondition = pkColumns
        .map(col => s"target.$col = source.$col")
        .mkString(" AND ")

      val changeCondition = compareColumns
        .map(col => s"target.$col != source.$col OR (target.$col IS NULL AND source.$col IS NOT NULL) OR (target.$col IS NOT NULL AND source.$col IS NULL)")
        .mkString(" OR ")

      // Step 1: Close changed records
      val closeSql =
        s"""MERGE INTO $tableName AS target
           |USING _iceberg_scd2_source AS source
           |ON $mergeCondition AND target.$isCurrentCol = true
           |WHEN MATCHED AND ($changeCondition) THEN UPDATE SET
           |  target.$validToCol = current_timestamp(),
           |  target.$isCurrentCol = false""".stripMargin

      spark.sql(closeSql)

      // Step 2: Insert new versions of changed records + brand new records
      val sourceColsQualified = df.columns.map(c => s"source.$c").mkString(", ")
      val insertSql =
        s"""INSERT INTO $tableName
           |SELECT $sourceColsQualified,
           |  current_timestamp() AS $validFromCol,
           |  CAST(NULL AS TIMESTAMP) AS $validToCol,
           |  true AS $isCurrentCol
           |FROM _iceberg_scd2_source source
           |LEFT JOIN $tableName target
           |  ON $mergeCondition AND target.$isCurrentCol = true
           |WHERE target.${pkColumns.head} IS NULL
           |  OR ($changeCondition)""".stripMargin

      spark.sql(insertSql)
      spark.catalog.dropTempView("_iceberg_scd2_source")
    }

    val snapshotId = tableManager.getCurrentSnapshotId(flowConfig)
    snapshotId.foreach { sid =>
      logger.info(
        s"SCD2 load complete on $tableName: $recordCount records, snapshot: $sid"
      )
    }

    WriteResult(recordCount, snapshotId)
  }

  def tagBatchSnapshot(
      flowConfig: FlowConfig,
      writeResult: WriteResult,
      batchId: String
  ): WriteResult = {
    writeResult.snapshotId match {
      case Some(sid) =>
        tableManager.tagSnapshot(flowConfig, sid, batchId)
        val metadata = tableManager.getSnapshotMetadata(
          flowConfig,
          sid,
          writeResult.recordsWritten,
          batchId
        )
        writeResult.copy(icebergMetadata = metadata)
      case None =>
        writeResult
    }
  }
}
