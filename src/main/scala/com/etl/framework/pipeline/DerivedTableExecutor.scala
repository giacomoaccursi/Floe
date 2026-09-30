package com.etl.framework.pipeline

import com.etl.framework.config.IcebergConfig
import com.etl.framework.iceberg.{
  AmbiguousCommitException,
  CommitContext,
  DuplicateOperationCommitException,
  IcebergTableManager
}
import com.etl.framework.util.SqlIdentifier
import com.etl.framework.orchestration.{DataOutcome, ExecutionRequest}
import org.apache.iceberg.exceptions.CommitStateUnknownException
import org.apache.iceberg.spark.CommitMetadata
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.time.Instant
import java.util.concurrent.Callable
import scala.collection.JavaConverters._

/** Executes derived table functions and writes results to Iceberg as full-load tables. */
class DerivedTableExecutor(
    icebergConfig: IcebergConfig
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val tableManager = new IcebergTableManager(spark, icebergConfig)

  /** Ad-hoc convenience API. Platform-managed execution should pass a complete ExecutionRequest. */
  def execute(
      derivedTables: Seq[(String, DerivedTableContext => DataFrame)],
      batchId: String,
      effectiveAt: Instant = Instant.now()
  ): Seq[DerivedTableResult] =
    execute(
      derivedTables,
      ExecutionRequest(
        pipelineId = "adhoc",
        logicalRunId = batchId,
        attemptId = batchId,
        effectiveAt = effectiveAt,
        codeVersion = "unversioned",
        configDigest = "0" * 64
      )
    )

  /** Executes all derived table functions and writes results to Iceberg. */
  def execute(
      derivedTables: Seq[(String, DerivedTableContext => DataFrame)],
      request: ExecutionRequest
  ): Seq[DerivedTableResult] = {
    val ctx = DerivedTableContext(
      spark,
      request.logicalRunId,
      request.attemptId,
      request.effectiveAt,
      icebergConfig.catalogName,
      icebergConfig.namespace
    )

    derivedTables.map { case (tableName, fn) =>
      executeSingle(tableName, fn, ctx, request)
    }
  }

  /** Executes a single derived table: computes the DataFrame, writes to Iceberg, tags the snapshot. */
  private def executeSingle(
      tableName: String,
      fn: DerivedTableContext => DataFrame,
      ctx: DerivedTableContext,
      request: ExecutionRequest
  ): DerivedTableResult = {
    val fullTableName = resolveTableName(tableName)
    var dataOutcome: DataOutcome = DataOutcome.NotAttempted
    logger.info(s"Computing derived table: $tableName")
    try {
      val df = fn(ctx)
      val commitContext = CommitContext.forFlow(request, tableName, "derived-full")
      val write = writeToIceberg(fullTableName, df, commitContext, () => dataOutcome = DataOutcome.Unknown)
      dataOutcome = if (write.snapshotId.isDefined) DataOutcome.Committed else DataOutcome.NoChange
      write.snapshotId.foreach(tagSnapshot(fullTableName, _, request.attemptId))
      logger.info(s"Derived table $tableName written: ${write.recordsWritten} records")
      DerivedTableResult(
        tableName,
        success = true,
        recordsWritten = write.recordsWritten,
        snapshotId = write.snapshotId,
        resultingSnapshotId = write.resultingSnapshotId,
        operationId = Some(commitContext.operationId),
        reconciled = write.reconciled,
        dataOutcome = dataOutcome
      )
    } catch {
      case e: Exception =>
        logger.error(s"Derived table $tableName failed: ${e.getMessage}", e)
        DerivedTableResult(tableName, success = false, error = Some(e.getMessage), dataOutcome = dataOutcome)
    }
  }

  private def resolveTableName(tableName: String): String =
    icebergConfig.fullTableName(tableName)

  private case class DerivedWrite(
      recordsWritten: Long,
      snapshotId: Option[Long],
      resultingSnapshotId: Option[Long],
      reconciled: Boolean
  )

  private def writeToIceberg(
      fullTableName: String,
      df: DataFrame,
      context: CommitContext,
      beforeDataCommit: () => Unit
  ): DerivedWrite = {
    createOrUpdateTable(fullTableName, df.schema)
    val cachedDf = df.cache()
    try {
      val recordsWritten = cachedDf.count()
      var reconciled = false
      beforeDataCommit()
      try {
        CommitMetadata.withCommitProperties(
          context.snapshotProperties.asJava,
          new Callable[Unit] {
            override def call(): Unit = {
              cachedDf.writeTo(SqlIdentifier.quoteMultipart(fullTableName)).overwrite(lit(true))
            }
          },
          classOf[RuntimeException]
        )
      } catch {
        case error: Throwable =>
          val matches = reconcileSnapshots(fullTableName, context, Some(error))
          if (matches.nonEmpty) reconciled = true else throw error
      }
      val matches = reconcileSnapshots(fullTableName, context, None)
      val committedSnapshot = matches.headOption.map(_.snapshotId)
      val resultingSnapshot = committedSnapshot.orElse(tableManager.getCurrentSnapshotId(fullTableName))
      DerivedWrite(recordsWritten, committedSnapshot, resultingSnapshot, reconciled)
    } finally {
      cachedDf.unpersist()
    }
  }

  private def tagSnapshot(fullTableName: String, snapshotId: Long, batchId: String): Unit = {
    if (!icebergConfig.enableSnapshotTagging) return

    try {
      if (tableManager.tagSnapshot(fullTableName, snapshotId, batchId))
        logger.info(s"Tagged derived table snapshot $snapshotId on $fullTableName")
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to tag snapshot on $fullTableName: ${e.getMessage}")
    }
  }

  private def reconcileSnapshots(
      tableName: String,
      context: CommitContext,
      commitError: Option[Throwable]
  ) = {
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

  private def hasCommitStateUnknown(error: Throwable): Boolean =
    Iterator
      .iterate[Throwable](error)(_.getCause)
      .takeWhile(_ != null)
      .exists(_.isInstanceOf[CommitStateUnknownException])

  /** Creates the Iceberg table if it doesn't exist, or adds missing columns if it does. */
  private def createOrUpdateTable(fullTableName: String, schema: StructType): Unit = {
    val exists =
      try {
        spark.sql(s"DESCRIBE TABLE ${SqlIdentifier.quoteMultipart(fullTableName)}")
        true
      } catch {
        case _: org.apache.spark.sql.AnalysisException => false
      }

    if (!exists) {
      val columns = schema.fields.map(f => s"${SqlIdentifier.quote(f.name)} ${f.dataType.sql}").mkString(", ")
      val sqlTableName = SqlIdentifier.quoteMultipart(fullTableName)
      spark.sql(s"CREATE TABLE IF NOT EXISTS $sqlTableName ($columns) USING iceberg")

      val props = Map(
        "format-version" -> icebergConfig.formatVersion.toString,
        "write.format.default" -> icebergConfig.fileFormat
      )
      props.foreach { case (k, v) =>
        spark.sql(
          s"ALTER TABLE $sqlTableName SET TBLPROPERTIES " +
            s"(${SqlIdentifier.stringLiteral(k)} = ${SqlIdentifier.stringLiteral(v)})"
        )
      }
      logger.info(s"Created Iceberg table $fullTableName")
    } else {
      val sqlTableName = SqlIdentifier.quoteMultipart(fullTableName)
      val currentColumns = spark.table(sqlTableName).schema.fieldNames.toSet
      schema.fields.filterNot(f => currentColumns.contains(f.name)).foreach { field =>
        spark.sql(s"ALTER TABLE $sqlTableName ADD COLUMN ${SqlIdentifier.quote(field.name)} ${field.dataType.sql}")
        logger.info(s"Added column ${field.name} to $fullTableName")
      }
    }
  }
}

case class DerivedTableResult(
    tableName: String,
    success: Boolean,
    recordsWritten: Long = 0,
    error: Option[String] = None,
    snapshotId: Option[Long] = None,
    resultingSnapshotId: Option[Long] = None,
    operationId: Option[String] = None,
    reconciled: Boolean = false,
    dataOutcome: DataOutcome = DataOutcome.NotAttempted
)
