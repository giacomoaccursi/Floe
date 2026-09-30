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
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.lit
import org.slf4j.LoggerFactory

import java.time.Instant
import java.util.Locale
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
      derivedTables: Seq[DerivedTableDefinition],
      batchId: String,
      inputTables: Map[String, DataFrame],
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
      ),
      inputTables
    )

  /** Executes derived tables in dependency order using only attempt-pinned input DataFrames. */
  def execute(
      derivedTables: Seq[DerivedTableDefinition],
      request: ExecutionRequest,
      inputTables: Map[String, DataFrame]
  ): Seq[DerivedTableResult] = {
    val errors = DerivedTableExecutor.validateDefinitions(derivedTables, inputTables.keySet)
    require(errors.isEmpty, s"Invalid derived table configuration: ${errors.mkString("; ")}")

    var available = inputTables
    var completed = Map.empty[String, DerivedTableResult]

    DerivedTableExecutor.orderDefinitions(derivedTables).map { definition =>
      val failedDependencies = definition.dependencies.filter(name => completed.get(name).exists(!_.success))
      val execution =
        if (failedDependencies.nonEmpty)
          DerivedExecution(
            DerivedTableResult(
              definition.name,
              success = false,
              error = Some(s"Blocked by failed derived dependencies: ${failedDependencies.mkString(", ")}")
            ),
            None
          )
        else {
          val resolvedInputs = definition.dependencies.map(name => name -> available(name)).toMap
          val ctx = DerivedTableContext(
            spark,
            request.logicalRunId,
            request.attemptId,
            request.effectiveAt,
            resolvedInputs
          )
          executeSingle(definition, ctx, request)
        }

      completed += definition.name -> execution.result
      execution.output.foreach(data => available += definition.name -> data)
      execution.result
    }
  }

  /** Executes a single derived table: computes the DataFrame, writes to Iceberg, tags the snapshot. */
  private def executeSingle(
      definition: DerivedTableDefinition,
      ctx: DerivedTableContext,
      request: ExecutionRequest
  ): DerivedExecution = {
    val tableName = definition.name
    val fullTableName = resolveTableName(tableName)
    var dataOutcome: DataOutcome = DataOutcome.NotAttempted
    logger.info(s"Computing derived table: $tableName")
    try {
      val df = definition.transform(ctx)
      val commitContext = CommitContext.forFlow(request, tableName, "derived-full")
      val write = writeToIceberg(fullTableName, df, commitContext, () => dataOutcome = DataOutcome.Unknown)
      dataOutcome = if (write.snapshotId.isDefined) DataOutcome.Committed else DataOutcome.NoChange
      write.snapshotId.foreach(tagSnapshot(fullTableName, _, request.attemptId))
      logger.info(s"Derived table $tableName written: ${write.recordsWritten} records")
      val result = DerivedTableResult(
        tableName,
        success = true,
        recordsWritten = write.recordsWritten,
        snapshotId = write.snapshotId,
        resultingSnapshotId = write.resultingSnapshotId,
        resultingSchemaJson = Some(write.resultingSchemaJson),
        operationId = Some(commitContext.operationId),
        reconciled = write.reconciled,
        dataOutcome = dataOutcome
      )
      val output = write.resultingSnapshotId match {
        case Some(snapshotId) =>
          spark.read.option("snapshot-id", snapshotId).table(SqlIdentifier.quoteMultipart(fullTableName))
        case None =>
          val schema = DataType.fromJson(write.resultingSchemaJson).asInstanceOf[StructType]
          spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      }
      DerivedExecution(result, Some(output))
    } catch {
      case e: Exception =>
        logger.error(s"Derived table $tableName failed: ${e.getMessage}", e)
        DerivedExecution(
          DerivedTableResult(tableName, success = false, error = Some(e.getMessage), dataOutcome = dataOutcome),
          None
        )
    }
  }

  private def resolveTableName(tableName: String): String =
    icebergConfig.fullTableName(tableName)

  private case class DerivedWrite(
      recordsWritten: Long,
      snapshotId: Option[Long],
      resultingSnapshotId: Option[Long],
      resultingSchemaJson: String,
      reconciled: Boolean
  )

  private case class DerivedExecution(result: DerivedTableResult, output: Option[DataFrame])

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
      val resultingSchemaJson = spark.table(SqlIdentifier.quoteMultipart(fullTableName)).schema.json
      DerivedWrite(recordsWritten, committedSnapshot, resultingSnapshot, resultingSchemaJson, reconciled)
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

object DerivedTableExecutor {
  def validateDefinitions(definitions: Seq[DerivedTableDefinition], primaryInputs: Set[String]): Seq[String] = {
    val names = definitions.map(_.name)
    val duplicateGroups = names
      .groupBy(_.toLowerCase(Locale.ROOT))
      .values
      .filter(_.size > 1)
      .map(_.sorted)
      .toSeq
      .sortBy(_.head)
    val derivedNames = names.toSet
    val known = primaryInputs ++ derivedNames
    val errors = scala.collection.mutable.ArrayBuffer.empty[String]

    duplicateGroups.foreach { duplicates =>
      errors += s"Derived table names collide case-insensitively: ${duplicates.mkString(", ")}"
    }
    definitions.foreach { definition =>
      if (definition.dependencies.distinct.size != definition.dependencies.size)
        errors += s"Derived table '${definition.name}' declares duplicate dependencies"
      if (definition.dependencies.contains(definition.name))
        errors += s"Derived table '${definition.name}' cannot depend on itself"
      definition.dependencies.filterNot(known).foreach { dependency =>
        errors += s"Derived table '${definition.name}' references unknown dependency '$dependency'"
      }
    }

    if (duplicateGroups.isEmpty && errors.isEmpty) {
      val ordered = scala.util.Try(orderDefinitions(definitions))
      ordered.failed.foreach(error => errors += error.getMessage)
    }
    errors.toSeq.distinct
  }

  private[pipeline] def orderDefinitions(definitions: Seq[DerivedTableDefinition]): Seq[DerivedTableDefinition] = {
    val derivedNames = definitions.map(_.name).toSet
    val remaining = scala.collection.mutable.ArrayBuffer(definitions: _*)
    val ordered = Vector.newBuilder[DerivedTableDefinition]
    var resolved = Set.empty[String]

    while (remaining.nonEmpty) {
      val ready = remaining.filter(definition => definition.dependencies.filter(derivedNames).forall(resolved))
      require(ready.nonEmpty, s"Derived table dependency cycle: ${remaining.map(_.name).mkString(" -> ")}")
      ready.foreach { definition =>
        ordered += definition
        resolved += definition.name
      }
      remaining --= ready
    }
    ordered.result()
  }
}

case class DerivedTableResult(
    tableName: String,
    success: Boolean,
    recordsWritten: Long = 0,
    error: Option[String] = None,
    snapshotId: Option[Long] = None,
    resultingSnapshotId: Option[Long] = None,
    resultingSchemaJson: Option[String] = None,
    operationId: Option[String] = None,
    reconciled: Boolean = false,
    dataOutcome: DataOutcome = DataOutcome.NotAttempted
)
