package com.etl.framework.iceberg

import com.etl.framework.config.{FlowConfig, IcebergConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
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

  /** Writes all source data to the Iceberg table, replacing existing content. */
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
      // overwrite(lit(true)) replaces ALL existing rows regardless of partitioning,
      // which is the correct semantic for a full load — even an empty source clears the table.
      // overwritePartitions() would be a no-op with an empty DataFrame (no partitions to replace).
      cachedDf.writeTo(tableName).overwrite(lit(true))
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

  /** Upserts source data into the Iceberg table using MERGE INTO on primary key. */
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

        val changeCondition = updateCols
          .map(c => s"NOT (source.$c <=> target.$c)")
          .mkString(" OR ")
        val updateSetClause =
          s"WHEN MATCHED AND ($changeCondition) THEN UPDATE SET " +
            updateCols.map(c => s"target.$c = source.$c").mkString(", ")

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

  /** Writes SCD2 versioned history. Initial load inserts all records as current. Subsequent loads detect changes, close
    * old versions, and insert new ones.
    */
  def writeSCD2Load(
      df: DataFrame,
      flowConfig: FlowConfig
  ): WriteResult = {
    val tableName = tableManager.resolveTableName(flowConfig)
    val cfg = SCD2Config.from(flowConfig)

    val scd2Schema = buildSCD2Schema(df, cfg)
    tableManager.createOrUpdateTable(flowConfig, scd2Schema)

    val isInitialLoad = tableManager.getCurrentSnapshotId(flowConfig).isEmpty
    val sn = sanitizeViewName(flowConfig.name)

    val cachedDf = df.cache()
    try {
      if (isInitialLoad)
        executeSCD2InitialLoad(cachedDf, tableName, sn, cfg)
      else
        executeSCD2MergeLoad(cachedDf, tableName, sn, cfg)

      val recordCount = cachedDf.count()
      val snapshotId = tableManager.getCurrentSnapshotId(flowConfig)
      snapshotId.foreach(sid => logger.info(s"SCD2 load complete on $tableName: $recordCount records, snapshot: $sid"))
      WriteResult(recordCount, snapshotId)
    } finally {
      cachedDf.unpersist()
    }
  }

  private case class SCD2Config(
      pkColumns: Seq[String],
      compareColumns: Seq[String],
      validFromCol: String,
      validToCol: String,
      isCurrentCol: String,
      detectDeletes: Boolean,
      isActiveCol: Option[String]
  )

  private object SCD2Config {
    def from(fc: FlowConfig): SCD2Config = {
      val detectDeletes = fc.loadMode.detectDeletes
      SCD2Config(
        pkColumns = fc.validation.primaryKey,
        compareColumns = fc.loadMode.compareColumns,
        validFromCol = fc.loadMode.validFromColumn.getOrElse("valid_from"),
        validToCol = fc.loadMode.validToColumn.getOrElse("valid_to"),
        isCurrentCol = fc.loadMode.isCurrentColumn.getOrElse("is_current"),
        detectDeletes = detectDeletes,
        isActiveCol =
          if (detectDeletes) Some(fc.loadMode.isActiveColumn.getOrElse("is_active"))
          else fc.loadMode.isActiveColumn
      )
    }
  }

  private def buildSCD2Schema(df: DataFrame, cfg: SCD2Config): StructType = {
    val base = df.schema
      .add(cfg.validFromCol, "timestamp")
      .add(cfg.validToCol, "timestamp")
      .add(cfg.isCurrentCol, "boolean")
    cfg.isActiveCol.fold(base)(c => base.add(c, "boolean"))
  }

  private def executeSCD2InitialLoad(
      df: DataFrame,
      tableName: String,
      sn: String,
      cfg: SCD2Config
  ): Unit = {
    logger.info(s"SCD2 initial load to $tableName")
    val sourceView = s"_iceberg_scd2_${sn}_source"
    df.createOrReplaceTempView(sourceView)
    try {
      val columns = df.columns.mkString(", ")
      val isActiveInsert = cfg.isActiveCol.map(c => s",\n  true AS $c").getOrElse("")
      spark.sql(
        s"""INSERT INTO $tableName
           |SELECT $columns, current_timestamp() AS ${cfg.validFromCol},
           |  CAST(NULL AS TIMESTAMP) AS ${cfg.validToCol},
           |  true AS ${cfg.isCurrentCol}$isActiveInsert
           |FROM $sourceView""".stripMargin
      )
    } finally {
      spark.catalog.dropTempView(sourceView)
    }
  }

  private def executeSCD2MergeLoad(
      df: DataFrame,
      tableName: String,
      sn: String,
      cfg: SCD2Config
  ): Unit = {
    logger.info(s"SCD2 change detection on $tableName")
    val sourceView = s"_iceberg_scd2_${sn}_source"
    val stagedView = s"_iceberg_scd2_${sn}_staged"

    df.createOrReplaceTempView(sourceView)
    try {
      buildStagedView(df, tableName, sourceView, stagedView, cfg)
      val mergeSql = buildMergeSql(df, tableName, stagedView, cfg)
      logger.info(s"Executing SCD2 MERGE INTO on $tableName")
      logger.debug(s"SCD2 Merge SQL: $mergeSql")
      spark.sql(mergeSql)
    } finally {
      spark.catalog.dropTempView(sourceView)
      spark.catalog.dropTempView(stagedView)
    }
  }

  private def buildStagedView(
      df: DataFrame,
      tableName: String,
      sourceView: String,
      stagedView: String,
      cfg: SCD2Config
  ): Unit = {
    val srcCols = df.columns.map(c => s"src.$c").mkString(", ")
    val mkFromPk = cfg.pkColumns.map(c => s"src.$c AS _mk_$c").mkString(", ")
    val mkNull = cfg.pkColumns
      .map { c =>
        val dataType = df.schema(c).dataType.sql
        s"CAST(NULL AS $dataType) AS _mk_$c"
      }
      .mkString(", ")
    val joinCond = cfg.pkColumns.map(c => s"src.$c = tgt.$c").mkString(" AND ")
    val changeCond = buildChangeCondition(cfg.compareColumns, "src", "tgt")

    spark
      .sql(
        s"""SELECT $srcCols, $mkFromPk
           |FROM $sourceView src
           |UNION ALL
           |SELECT $srcCols, $mkNull
           |FROM $sourceView src
           |JOIN $tableName tgt ON $joinCond AND tgt.${cfg.isCurrentCol} = true
           |WHERE $changeCond""".stripMargin
      )
      .createOrReplaceTempView(stagedView)
  }

  private def buildMergeSql(
      df: DataFrame,
      tableName: String,
      stagedView: String,
      cfg: SCD2Config
  ): String = {
    val mergeOn =
      cfg.pkColumns.map(c => s"target.$c = source._mk_$c").mkString(" AND ") +
        s" AND target.${cfg.isCurrentCol} = true"

    val matchedClause = {
      val changeCond = buildChangeCondition(cfg.compareColumns, "source", "target")
      s"""WHEN MATCHED AND ($changeCond) THEN UPDATE SET
         |  target.${cfg.validToCol} = current_timestamp(),
         |  target.${cfg.isCurrentCol} = false""".stripMargin
    }

    val notMatchedClause = {
      val insertColNames =
        (df.columns.toSeq ++ Seq(cfg.validFromCol, cfg.validToCol, cfg.isCurrentCol) ++ cfg.isActiveCol.toSeq)
          .mkString(", ")
      val isActiveVal = cfg.isActiveCol.map(_ => ", true").getOrElse("")
      val insertVals =
        df.columns.map(c => s"source.$c").mkString(", ") +
          s", current_timestamp(), CAST(NULL AS TIMESTAMP), true$isActiveVal"
      s"WHEN NOT MATCHED THEN INSERT ($insertColNames) VALUES ($insertVals)"
    }

    val softDeleteClause =
      if (cfg.detectDeletes) {
        val isActiveUpdate = cfg.isActiveCol.map(c => s",\n  target.$c = false").getOrElse("")
        Seq(
          s"""WHEN NOT MATCHED BY SOURCE AND target.${cfg.isCurrentCol} = true THEN UPDATE SET
             |  target.${cfg.validToCol} = current_timestamp(),
             |  target.${cfg.isCurrentCol} = false$isActiveUpdate""".stripMargin
        )
      } else Seq.empty

    val allClauses = Seq(matchedClause, notMatchedClause) ++ softDeleteClause
    s"""MERGE INTO $tableName AS target
       |USING $stagedView AS source
       |ON $mergeOn
       |${allClauses.mkString("\n")}""".stripMargin
  }

  private def buildChangeCondition(columns: Seq[String], leftAlias: String, rightAlias: String): String =
    columns
      .map(c =>
        s"$leftAlias.$c != $rightAlias.$c OR ($leftAlias.$c IS NULL AND $rightAlias.$c IS NOT NULL) OR ($leftAlias.$c IS NOT NULL AND $rightAlias.$c IS NULL)"
      )
      .mkString(" OR ")

  /** Tags the batch snapshot and collects Iceberg metadata for the batch metadata JSON. */
  def tagBatchSnapshot(
      flowConfig: FlowConfig,
      writeResult: WriteResult,
      batchId: String
  ): WriteResult = {
    writeResult.snapshotId match {
      case Some(sid) =>
        val tagged = tableManager.tagSnapshot(flowConfig, sid, batchId)
        val metadata = tableManager.getSnapshotMetadata(
          flowConfig,
          sid,
          writeResult.recordsWritten,
          batchId,
          tagCreated = tagged
        )
        writeResult.copy(icebergMetadata = metadata)
      case None =>
        writeResult
    }
  }
}
