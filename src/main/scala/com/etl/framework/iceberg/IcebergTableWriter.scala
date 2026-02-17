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
    val detectDeletes = flowConfig.loadMode.detectDeletes
    val isActiveCol: Option[String] =
      if (detectDeletes)
        Some(flowConfig.loadMode.isActiveColumn.getOrElse("is_active"))
      else
        flowConfig.loadMode.isActiveColumn

    // Build SCD2 schema
    val baseScd2Schema = df.schema
      .add(validFromCol, "timestamp")
      .add(validToCol, "timestamp")
      .add(isCurrentCol, "boolean")
    val scd2Schema = isActiveCol.fold(baseScd2Schema)(c =>
      baseScd2Schema.add(c, "boolean")
    )

    tableManager.createOrUpdateTable(flowConfig, scd2Schema)

    val recordCount = df.count()
    val existingCount =
      spark.sql(s"SELECT COUNT(*) FROM $tableName").first().getLong(0)

    if (existingCount == 0) {
      // First load: all records are current and active
      logger.info(s"SCD2 initial load to $tableName: $recordCount records")

      df.createOrReplaceTempView("_iceberg_scd2_source")

      val columns = df.columns.mkString(", ")
      val isActiveInsert =
        isActiveCol.map(c => s",\n  true AS $c").getOrElse("")

      spark.sql(
        s"""INSERT INTO $tableName
           |SELECT $columns, current_timestamp() AS $validFromCol,
           |  CAST(NULL AS TIMESTAMP) AS $validToCol,
           |  true AS $isCurrentCol$isActiveInsert
           |FROM _iceberg_scd2_source""".stripMargin
      )
      spark.catalog.dropTempView("_iceberg_scd2_source")
    } else {
      // Subsequent load: single atomic MERGE INTO with NULL merge_key trick
      logger.info(
        s"SCD2 change detection on $tableName: $recordCount records"
      )

      df.createOrReplaceTempView("_iceberg_scd2_source")

      // Build staged source: all records with _mk=pk UNION changed records with _mk=NULL
      val srcCols = df.columns.map(c => s"src.$c").mkString(", ")
      val mkFromPk = pkColumns.map(c => s"src.$c AS _mk_$c").mkString(", ")
      val mkNull = pkColumns
        .map { c =>
          val dataType = df.schema(c).dataType.sql
          s"CAST(NULL AS $dataType) AS _mk_$c"
        }
        .mkString(", ")

      val joinCond =
        pkColumns.map(c => s"src.$c = tgt.$c").mkString(" AND ")
      val changeCond = compareColumns
        .map(c =>
          s"src.$c != tgt.$c OR (src.$c IS NULL AND tgt.$c IS NOT NULL) OR (src.$c IS NOT NULL AND tgt.$c IS NULL)"
        )
        .mkString(" OR ")

      spark
        .sql(
          s"""SELECT $srcCols, $mkFromPk
           |FROM _iceberg_scd2_source src
           |UNION ALL
           |SELECT $srcCols, $mkNull
           |FROM _iceberg_scd2_source src
           |JOIN $tableName tgt ON $joinCond AND tgt.$isCurrentCol = true
           |WHERE $changeCond""".stripMargin
        )
        .createOrReplaceTempView("_iceberg_scd2_staged")

      // Build MERGE clauses
      val mergeOn =
        pkColumns.map(c => s"target.$c = source._mk_$c").mkString(" AND ") +
          s" AND target.$isCurrentCol = true"

      val targetChangeCond = compareColumns
        .map(c =>
          s"target.$c != source.$c OR (target.$c IS NULL AND source.$c IS NOT NULL) OR (target.$c IS NOT NULL AND source.$c IS NULL)"
        )
        .mkString(" OR ")

      // WHEN MATCHED: close old version of changed record
      val matchedClause =
        s"""WHEN MATCHED AND ($targetChangeCond) THEN UPDATE SET
           |  target.$validToCol = current_timestamp(),
           |  target.$isCurrentCol = false""".stripMargin

      // WHEN NOT MATCHED: insert new version or brand new record
      val insertColNames =
        (df.columns.toSeq ++ Seq(validFromCol, validToCol, isCurrentCol) ++ isActiveCol.toSeq)
          .mkString(", ")
      val isActiveVal = isActiveCol.map(_ => ", true").getOrElse("")
      val insertVals =
        df.columns.map(c => s"source.$c").mkString(", ") +
          s", current_timestamp(), CAST(NULL AS TIMESTAMP), true$isActiveVal"
      val notMatchedClause =
        s"WHEN NOT MATCHED THEN INSERT ($insertColNames) VALUES ($insertVals)"

      // WHEN NOT MATCHED BY SOURCE: soft delete (only if detectDeletes=true)
      val softDeleteClause =
        if (detectDeletes) {
          val isActiveUpdate =
            isActiveCol.map(c => s",\n  target.$c = false").getOrElse("")
          Seq(
            s"""WHEN NOT MATCHED BY SOURCE AND target.$isCurrentCol = true THEN UPDATE SET
             |  target.$validToCol = current_timestamp(),
             |  target.$isCurrentCol = false$isActiveUpdate""".stripMargin
          )
        } else Seq.empty

      val allClauses =
        Seq(matchedClause, notMatchedClause) ++ softDeleteClause
      val mergeSql =
        s"""MERGE INTO $tableName AS target
           |USING _iceberg_scd2_staged AS source
           |ON $mergeOn
           |${allClauses.mkString("\n")}""".stripMargin

      logger.info(s"Executing SCD2 MERGE INTO on $tableName")
      logger.debug(s"SCD2 Merge SQL: $mergeSql")
      spark.sql(mergeSql)

      spark.catalog.dropTempView("_iceberg_scd2_source")
      spark.catalog.dropTempView("_iceberg_scd2_staged")
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
