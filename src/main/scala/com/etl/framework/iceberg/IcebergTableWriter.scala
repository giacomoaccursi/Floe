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

  private def sanitizeViewName(flowName: String): String =
    flowName.replaceAll("[^a-zA-Z0-9_]", "_")

  def writeFullLoad(
      df: DataFrame,
      flowConfig: FlowConfig
  ): WriteResult = {
    val tableName = tableManager.resolveTableName(flowConfig)
    tableManager.createOrUpdateTable(flowConfig, df.schema)

    logger.info(s"Writing full load to $tableName")

    // Cache df: write populates the cache, count() reuses it avoiding a second scan
    val cachedDf = df.cache()
    try {
      cachedDf.writeTo(tableName).overwritePartitions()
      val recordCount = cachedDf.count()
      val snapshotId = tableManager.getCurrentSnapshotId(flowConfig)
      snapshotId.foreach { sid =>
        logger.info(s"Full load complete on $tableName: $recordCount records, snapshot: $sid")
      }
      WriteResult(recordCount, snapshotId)
    } finally {
      cachedDf.unpersist()
    }
  }

  def writeDeltaLoad(
      df: DataFrame,
      flowConfig: FlowConfig
  ): WriteResult = {
    val tableName = tableManager.resolveTableName(flowConfig)
    tableManager.createOrUpdateTable(flowConfig, df.schema)

    val pkColumns = flowConfig.validation.primaryKey

    // Cache df before write: write populates the cache, count() reuses it
    val cachedDf = df.cache()
    try {
      if (pkColumns.isEmpty) {
        logger.warn(s"No primary key defined for $tableName, falling back to append")
        cachedDf.writeTo(tableName).append()
      } else {
        val mergeCondition = pkColumns
          .map(col => s"target.$col = source.$col")
          .mkString(" AND ")

        val updateCols = cachedDf.columns
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

        val insertCols = cachedDf.columns.mkString(", ")
        val insertVals = cachedDf.columns.map(c => s"source.$c").mkString(", ")
        val insertClause =
          s"WHEN NOT MATCHED THEN INSERT ($insertCols) VALUES ($insertVals)"

        val mergeView = s"_iceberg_merge_${sanitizeViewName(flowConfig.name)}_source"
        val mergeSql =
          s"""MERGE INTO $tableName AS target
             |USING $mergeView AS source
             |ON $mergeCondition
             |$updateSetClause
             |$insertClause""".stripMargin

        logger.info(s"Executing MERGE INTO on $tableName")
        logger.debug(s"Merge SQL: $mergeSql")
        try {
          cachedDf.createOrReplaceTempView(mergeView)
          spark.sql(mergeSql)
        } finally {
          spark.catalog.dropTempView(mergeView)
        }
      }

      val recordCount = cachedDf.count()
      val snapshotId = tableManager.getCurrentSnapshotId(flowConfig)
      snapshotId.foreach { sid =>
        logger.info(
          s"Delta upsert complete on $tableName: $recordCount records, snapshot: $sid"
        )
      }
      WriteResult(recordCount, snapshotId)
    } finally {
      cachedDf.unpersist()
    }
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

    val existingCount =
      spark.sql(s"SELECT COUNT(*) FROM $tableName").first().getLong(0)

    val sn = sanitizeViewName(flowConfig.name)
    val sourceView = s"_iceberg_scd2_${sn}_source"
    val stagedView = s"_iceberg_scd2_${sn}_staged"

    // Cache df: the SQL operations reading the source view populate the cache,
    // count() reuses it avoiding a second scan of the input data
    val cachedDf = df.cache()

    try {

    if (existingCount == 0) {
      // First load: all records are current and active
      logger.info(s"SCD2 initial load to $tableName")

      cachedDf.createOrReplaceTempView(sourceView)

      val columns = cachedDf.columns.mkString(", ")
      val isActiveInsert =
        isActiveCol.map(c => s",\n  true AS $c").getOrElse("")

      try {
        spark.sql(
          s"""INSERT INTO $tableName
             |SELECT $columns, current_timestamp() AS $validFromCol,
             |  CAST(NULL AS TIMESTAMP) AS $validToCol,
             |  true AS $isCurrentCol$isActiveInsert
             |FROM $sourceView""".stripMargin
        )
      } finally {
        spark.catalog.dropTempView(sourceView)
      }
    } else {
      // Subsequent load: single atomic MERGE INTO with NULL merge_key trick
      logger.info(s"SCD2 change detection on $tableName")

      cachedDf.createOrReplaceTempView(sourceView)

      // Build staged source: all records with _mk=pk UNION changed records with _mk=NULL
      val srcCols = cachedDf.columns.map(c => s"src.$c").mkString(", ")
      val mkFromPk = pkColumns.map(c => s"src.$c AS _mk_$c").mkString(", ")
      val mkNull = pkColumns
        .map { c =>
          val dataType = cachedDf.schema(c).dataType.sql
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
           |FROM $sourceView src
           |UNION ALL
           |SELECT $srcCols, $mkNull
           |FROM $sourceView src
           |JOIN $tableName tgt ON $joinCond AND tgt.$isCurrentCol = true
           |WHERE $changeCond""".stripMargin
        )
        .createOrReplaceTempView(stagedView)

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
        (cachedDf.columns.toSeq ++ Seq(validFromCol, validToCol, isCurrentCol) ++ isActiveCol.toSeq)
          .mkString(", ")
      val isActiveVal = isActiveCol.map(_ => ", true").getOrElse("")
      val insertVals =
        cachedDf.columns.map(c => s"source.$c").mkString(", ") +
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
           |USING $stagedView AS source
           |ON $mergeOn
           |${allClauses.mkString("\n")}""".stripMargin

      logger.info(s"Executing SCD2 MERGE INTO on $tableName")
      logger.debug(s"SCD2 Merge SQL: $mergeSql")
      try {
        spark.sql(mergeSql)
      } finally {
        spark.catalog.dropTempView(sourceView)
        spark.catalog.dropTempView(stagedView)
      }
    }

    val recordCount = cachedDf.count()
    val snapshotId = tableManager.getCurrentSnapshotId(flowConfig)
    snapshotId.foreach { sid =>
      logger.info(
        s"SCD2 load complete on $tableName: $recordCount records, snapshot: $sid"
      )
    }

    WriteResult(recordCount, snapshotId)

    } finally {
      cachedDf.unpersist()
    }
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
