package com.etl.framework.iceberg

import com.etl.framework.config.{FlowConfig, IcebergConfig}
import com.etl.framework.util.SqlIdentifier
import org.apache.iceberg.exceptions.CommitStateUnknownException
import org.apache.iceberg.spark.CommitMetadata
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.{UUID, Map => JMap}
import java.util.concurrent.Callable
import scala.collection.JavaConverters._

case class WriteResult(
    recordsProcessed: Long,
    snapshotId: Option[Long],
    resultingSnapshotId: Option[Long] = None,
    resultingSchemaJson: Option[String] = None,
    icebergMetadata: Option[IcebergFlowMetadata] = None,
    operationId: String = "",
    reconciled: Boolean = false
)

/** Writes DataFrames to Iceberg tables using the appropriate strategy (full, delta, SCD2). Handles MERGE INTO for
  * upserts, SCD2 versioning with change detection, and snapshot tagging.
  */
class IcebergTableWriter(
    spark: SparkSession,
    icebergConfig: IcebergConfig,
    tableManager: IcebergTableManager
) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC)

  /** Iceberg identifier fields are metadata, not a uniqueness constraint. Never feed an ambiguous source to MERGE. */
  private def validateMergeKeys(df: DataFrame, pkColumns: Seq[String]): Unit = {
    require(pkColumns.nonEmpty, "MERGE requires at least one primary-key column")
    val missing = pkColumns.filterNot(df.columns.contains)
    require(missing.isEmpty, s"MERGE key columns missing from source: ${missing.mkString(", ")}")

    val nullKey = pkColumns.map(c => col(c).isNull).reduce(_ || _)
    require(df.filter(nullKey).limit(1).count() == 0L, "MERGE source contains a NULL primary key")

    val duplicateCountCol = s"__floe_merge_key_count_${UUID.randomUUID().toString.replace("-", "")}"
    val hasDuplicateKey = df
      .groupBy(pkColumns.map(col): _*)
      .agg(count(lit(1)).as(duplicateCountCol))
      .filter(col(duplicateCountCol) > 1L)
      .limit(1)
      .count() > 0L
    require(
      !hasDuplicateKey,
      "MERGE source contains duplicate primary keys; define a deterministic resolution upstream"
    )
  }

  private def sanitizeViewName(flowName: String): String =
    flowName.replaceAll("[^a-zA-Z0-9_]", "_")

  private def uniqueViewName(prefix: String, flowName: String): String =
    s"${prefix}_${sanitizeViewName(flowName)}_${UUID.randomUUID().toString.replace("-", "").take(8)}"

  /** Writes all source data to the Iceberg table, replacing existing content. */
  def writeFullLoad(
      df: DataFrame,
      flowConfig: FlowConfig
  ): WriteResult =
    writeFullLoad(df, flowConfig, CommitContext.adHoc(flowConfig.name, "full"))

  def writeFullLoad(
      df: DataFrame,
      flowConfig: FlowConfig,
      commitContext: CommitContext,
      beforeDataCommit: () => Unit = () => ()
  ): WriteResult = {
    val tableName = tableManager.resolveTableName(flowConfig)
    val sqlTableName = SqlIdentifier.quoteMultipart(tableName)
    tableManager.prepareTable(flowConfig, df.schema)

    logger.info(s"Writing full load to $tableName")

    // Cache df: write populates the cache, count() reuses it avoiding a second scan
    val cachedDf = df.cache()
    try {
      // overwrite(lit(true)) replaces ALL existing rows regardless of partitioning,
      // which is the correct semantic for a full load — even an empty source clears the table.
      // overwritePartitions() would be a no-op with an empty DataFrame (no partitions to replace).
      val recordCount = cachedDf.count()
      val result = executeTrackedCommit(flowConfig, tableName, commitContext, recordCount, beforeDataCommit) {
        cachedDf.writeTo(sqlTableName).overwrite(lit(true))
      }
      result.snapshotId.foreach { sid =>
        logger.info(s"Full load complete on $tableName: $recordCount records, snapshot: $sid")
      }
      result
    } finally {
      cachedDf.unpersist()
    }
  }

  /** Upserts source data into the Iceberg table using MERGE INTO on primary key. */
  def writeDeltaLoad(
      df: DataFrame,
      flowConfig: FlowConfig
  ): WriteResult =
    writeDeltaLoad(df, flowConfig, CommitContext.adHoc(flowConfig.name, "delta"))

  def writeDeltaLoad(
      df: DataFrame,
      flowConfig: FlowConfig,
      commitContext: CommitContext,
      beforeDataCommit: () => Unit = () => ()
  ): WriteResult = {
    val pkColumns = flowConfig.validation.primaryKey
    require(
      pkColumns.nonEmpty,
      s"Delta flow '${flowConfig.name}' requires a primary key; implicit append is unsupported"
    )
    val tableName = tableManager.resolveTableName(flowConfig)
    val sqlTableName = SqlIdentifier.quoteMultipart(tableName)

    // Cache df before write: write populates the cache, count() reuses it
    val cachedDf = df.cache()
    try {
      validateMergeKeys(cachedDf, pkColumns)
      val recordCount = cachedDf.count()
      tableManager.prepareTable(flowConfig, df.schema)
      val result = executeTrackedCommit(flowConfig, tableName, commitContext, recordCount, beforeDataCommit) {
        val mergeCondition = pkColumns
          .map(c => s"${SqlIdentifier.qualified("target", c)} = ${SqlIdentifier.qualified("source", c)}")
          .mkString(" AND ")

        val updateCols = cachedDf.columns.filterNot(pkColumns.contains)
        val matchedClause = if (updateCols.nonEmpty) {
          val changeCondition = updateCols
            .map(c => s"NOT (${SqlIdentifier.qualified("source", c)} <=> ${SqlIdentifier.qualified("target", c)})")
            .mkString(" OR ")
          s"WHEN MATCHED AND ($changeCondition) THEN UPDATE SET " +
            updateCols
              .map(c => s"${SqlIdentifier.qualified("target", c)} = ${SqlIdentifier.qualified("source", c)}")
              .mkString(", ")
        } else ""

        val insertCols = cachedDf.columns.map(SqlIdentifier.quote).mkString(", ")
        val insertVals = cachedDf.columns.map(c => SqlIdentifier.qualified("source", c)).mkString(", ")
        val insertClause = s"WHEN NOT MATCHED THEN INSERT ($insertCols) VALUES ($insertVals)"
        val allClauses = Seq(matchedClause, insertClause).filter(_.nonEmpty).mkString("\n")
        val mergeView = uniqueViewName("_iceberg_merge", flowConfig.name)
        val mergeSql =
          s"""MERGE INTO $sqlTableName AS ${SqlIdentifier.quote("target")}
               |USING ${SqlIdentifier.quote(mergeView)} AS ${SqlIdentifier.quote("source")}
               |ON $mergeCondition
               |$allClauses""".stripMargin

        logger.info(s"Executing MERGE INTO on $tableName")
        logger.debug(s"Merge SQL: $mergeSql")
        try {
          cachedDf.createOrReplaceTempView(mergeView)
          val _ = spark.sql(mergeSql)
        } finally {
          val _ = spark.catalog.dropTempView(mergeView)
        }
      }
      result.snapshotId.foreach { sid =>
        logger.info(
          s"Delta upsert complete on $tableName: $recordCount records, snapshot: $sid"
        )
      }
      result
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
  ): WriteResult =
    writeSCD2Load(df, flowConfig, CommitContext.adHoc(flowConfig.name, "scd2"))

  def writeSCD2Load(
      df: DataFrame,
      flowConfig: FlowConfig,
      commitContext: CommitContext,
      beforeDataCommit: () => Unit = () => ()
  ): WriteResult = {
    val tableName = tableManager.resolveTableName(flowConfig)
    val cfg = SCD2Config.from(flowConfig)

    val scd2Schema = buildSCD2Schema(df, cfg)
    val cachedDf = df.cache()
    try {
      validateMergeKeys(cachedDf, cfg.pkColumns)
      val recordCount = cachedDf.count()
      tableManager.prepareTable(flowConfig, scd2Schema)
      val isInitialLoad = tableManager.getCurrentSnapshotId(flowConfig).isEmpty
      val result = executeTrackedCommit(flowConfig, tableName, commitContext, recordCount, beforeDataCommit) {
        if (isInitialLoad)
          executeSCD2InitialLoad(cachedDf, tableName, flowConfig.name, cfg, commitContext)
        else
          executeSCD2MergeLoad(cachedDf, tableName, flowConfig.name, cfg, commitContext)
      }
      result.snapshotId.foreach(sid =>
        logger.info(s"SCD2 load complete on $tableName: $recordCount records, snapshot: $sid")
      )
      result
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
      flowName: String,
      cfg: SCD2Config,
      commitContext: CommitContext
  ): Unit = {
    logger.info(s"SCD2 initial load to $tableName")
    val sourceView = uniqueViewName("_iceberg_scd2_src", flowName)
    df.createOrReplaceTempView(sourceView)
    try {
      val quotedColumns = df.columns.map(SqlIdentifier.quote).mkString(", ")
      val isActiveInsert = cfg.isActiveCol.map(c => s",\n  true AS ${SqlIdentifier.quote(c)}").getOrElse("")
      val _ = spark.sql(
        s"""INSERT INTO ${SqlIdentifier.quoteMultipart(tableName)}
           |SELECT $quotedColumns, ${timestampLiteral(commitContext)} AS ${SqlIdentifier.quote(cfg.validFromCol)},
           |  CAST(NULL AS TIMESTAMP) AS ${SqlIdentifier.quote(cfg.validToCol)},
           |  true AS ${SqlIdentifier.quote(cfg.isCurrentCol)}$isActiveInsert
           |FROM ${SqlIdentifier.quote(sourceView)}""".stripMargin
      )
    } finally {
      val _ = spark.catalog.dropTempView(sourceView)
    }
  }

  private def executeSCD2MergeLoad(
      df: DataFrame,
      tableName: String,
      flowName: String,
      cfg: SCD2Config,
      commitContext: CommitContext
  ): Unit = {
    logger.info(s"SCD2 change detection on $tableName")
    val sourceView = uniqueViewName("_iceberg_scd2_src", flowName)
    val stagedView = uniqueViewName("_iceberg_scd2_stg", flowName)

    df.createOrReplaceTempView(sourceView)
    try {
      buildStagedView(df, tableName, sourceView, stagedView, cfg)
      val mergeSql = buildMergeSql(df, tableName, stagedView, cfg, commitContext)
      logger.info(s"Executing SCD2 MERGE INTO on $tableName")
      logger.debug(s"SCD2 Merge SQL: $mergeSql")
      val _ = spark.sql(mergeSql)
    } finally {
      Seq(sourceView, stagedView).foreach { viewName =>
        val _ = spark.catalog.dropTempView(viewName)
      }
    }
  }

  private def buildStagedView(
      df: DataFrame,
      tableName: String,
      sourceView: String,
      stagedView: String,
      cfg: SCD2Config
  ): Unit = {
    val srcCols = df.columns.map(c => SqlIdentifier.qualified("src", c)).mkString(", ")
    val mkFromPk = cfg.pkColumns
      .map(c => s"${SqlIdentifier.qualified("src", c)} AS ${SqlIdentifier.quote(s"_mk_$c")}")
      .mkString(", ")
    val mkNull = cfg.pkColumns
      .map { c =>
        val dataType = df.schema(c).dataType.sql
        s"CAST(NULL AS $dataType) AS ${SqlIdentifier.quote(s"_mk_$c")}"
      }
      .mkString(", ")
    val joinCond = cfg.pkColumns
      .map(c => s"${SqlIdentifier.qualified("src", c)} = ${SqlIdentifier.qualified("tgt", c)}")
      .mkString(" AND ")
    val changeCond = buildChangeCondition(cfg.compareColumns, "src", "tgt")

    spark
      .sql(
        s"""SELECT $srcCols, $mkFromPk
           |FROM ${SqlIdentifier.quote(sourceView)} AS ${SqlIdentifier.quote("src")}
           |UNION ALL
           |SELECT $srcCols, $mkNull
           |FROM ${SqlIdentifier.quote(sourceView)} AS ${SqlIdentifier.quote("src")}
           |JOIN ${SqlIdentifier.quoteMultipart(tableName)} AS ${SqlIdentifier.quote(
            "tgt"
          )} ON $joinCond AND ${SqlIdentifier.qualified("tgt", cfg.isCurrentCol)} = true
           |WHERE $changeCond""".stripMargin
      )
      .createOrReplaceTempView(stagedView)
  }

  private def buildMergeSql(
      df: DataFrame,
      tableName: String,
      stagedView: String,
      cfg: SCD2Config,
      commitContext: CommitContext
  ): String = {
    val mergeOn =
      cfg.pkColumns
        .map(c => s"${SqlIdentifier.qualified("target", c)} = ${SqlIdentifier.qualified("source", s"_mk_$c")}")
        .mkString(" AND ") +
        s" AND ${SqlIdentifier.qualified("target", cfg.isCurrentCol)} = true"

    val matchedClause = {
      val changeCond = buildChangeCondition(cfg.compareColumns, "source", "target")
      s"""WHEN MATCHED AND ($changeCond) THEN UPDATE SET
         |  ${SqlIdentifier.qualified("target", cfg.validToCol)} = ${timestampLiteral(commitContext)},
         |  ${SqlIdentifier.qualified("target", cfg.isCurrentCol)} = false""".stripMargin
    }

    val notMatchedClause = {
      val insertColNames =
        (df.columns.toSeq ++ Seq(cfg.validFromCol, cfg.validToCol, cfg.isCurrentCol) ++ cfg.isActiveCol.toSeq)
          .map(SqlIdentifier.quote)
          .mkString(", ")
      val isActiveVal = cfg.isActiveCol.map(_ => ", true").getOrElse("")
      val insertVals =
        df.columns.map(c => SqlIdentifier.qualified("source", c)).mkString(", ") +
          s", ${timestampLiteral(commitContext)}, CAST(NULL AS TIMESTAMP), true$isActiveVal"
      s"WHEN NOT MATCHED THEN INSERT ($insertColNames) VALUES ($insertVals)"
    }

    val softDeleteClause =
      if (cfg.detectDeletes) {
        val isActiveUpdate =
          cfg.isActiveCol.map(c => s",\n  ${SqlIdentifier.qualified("target", c)} = false").getOrElse("")
        Seq(
          s"""WHEN NOT MATCHED BY SOURCE AND ${SqlIdentifier.qualified(
              "target",
              cfg.isCurrentCol
            )} = true THEN UPDATE SET
             |  ${SqlIdentifier.qualified("target", cfg.validToCol)} = ${timestampLiteral(commitContext)},
             |  ${SqlIdentifier.qualified("target", cfg.isCurrentCol)} = false$isActiveUpdate""".stripMargin
        )
      } else Seq.empty

    val allClauses = Seq(matchedClause, notMatchedClause) ++ softDeleteClause
    s"""MERGE INTO ${SqlIdentifier.quoteMultipart(tableName)} AS ${SqlIdentifier.quote("target")}
       |USING ${SqlIdentifier.quote(stagedView)} AS ${SqlIdentifier.quote("source")}
       |ON $mergeOn
       |${allClauses.mkString("\n")}""".stripMargin
  }

  private def buildChangeCondition(columns: Seq[String], leftAlias: String, rightAlias: String): String =
    columns
      .map(c => s"NOT (${SqlIdentifier.qualified(leftAlias, c)} <=> ${SqlIdentifier.qualified(rightAlias, c)})")
      .mkString(" OR ")

  private def timestampLiteral(context: CommitContext): String =
    s"TIMESTAMP '${timestampFormatter.format(context.effectiveAt)}'"

  private def executeTrackedCommit(
      flowConfig: FlowConfig,
      tableName: String,
      context: CommitContext,
      recordsProcessed: Long,
      beforeDataCommit: () => Unit
  )(commit: => Unit): WriteResult = {
    var reconciled = false
    try {
      beforeDataCommit()
      val properties: JMap[String, String] = context.snapshotProperties.asJava
      CommitMetadata.withCommitProperties(
        properties,
        new Callable[Unit] {
          override def call(): Unit = commit
        },
        classOf[RuntimeException]
      )
    } catch {
      case error: Throwable =>
        val matches = reconcileSnapshots(tableName, context, Some(error))
        if (matches.nonEmpty) reconciled = true
        else throw error
    }

    val matches = reconcileSnapshots(tableName, context, None)
    matches.headOption match {
      case Some(snapshot) =>
        val metadata = tableManager.getSnapshotMetadata(
          flowConfig,
          snapshot.snapshotId,
          recordsProcessed,
          context.batchId
        )
        WriteResult(
          recordsProcessed = recordsProcessed,
          snapshotId = Some(snapshot.snapshotId),
          resultingSnapshotId = Some(snapshot.snapshotId),
          resultingSchemaJson = Some(spark.table(SqlIdentifier.quoteMultipart(tableName)).schema.json),
          icebergMetadata = metadata,
          operationId = context.operationId,
          reconciled = reconciled
        )
      case None =>
        // A deterministic MERGE with no changes may legitimately create no snapshot.
        WriteResult(
          recordsProcessed,
          snapshotId = None,
          resultingSnapshotId = tableManager.getCurrentSnapshotId(flowConfig),
          resultingSchemaJson = Some(spark.table(SqlIdentifier.quoteMultipart(tableName)).schema.json),
          operationId = context.operationId,
          reconciled = reconciled
        )
    }
  }

  private def reconcileSnapshots(
      tableName: String,
      context: CommitContext,
      commitError: Option[Throwable]
  ): Seq[CommittedSnapshot] = {
    val snapshots =
      try tableManager.findSnapshotsByOperationId(tableName, context.operationId)
      catch {
        case lookupError: Throwable =>
          throw AmbiguousCommitException(context.operationId, tableName, commitError.getOrElse(lookupError))
      }

    if (snapshots.size > 1)
      throw DuplicateOperationCommitException(context.operationId, tableName, snapshots.map(_.snapshotId))

    if (snapshots.isEmpty && commitError.exists(hasCommitStateUnknown))
      throw AmbiguousCommitException(context.operationId, tableName, commitError.get)

    snapshots
  }

  private def hasCommitStateUnknown(error: Throwable): Boolean = {
    Iterator
      .iterate[Throwable](error)(_.getCause)
      .takeWhile(_ != null)
      .exists(_.isInstanceOf[CommitStateUnknownException])
  }

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
          writeResult.recordsProcessed,
          batchId,
          tagCreated = tagged
        )
        writeResult.copy(icebergMetadata = metadata, resultingSnapshotId = Some(sid))
      case None =>
        writeResult
    }
  }
}
