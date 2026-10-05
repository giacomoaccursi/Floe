package com.etl.framework.runner

import com.etl.framework.config.GlobalConfigLoader
import com.etl.framework.exceptions.ConfigurationException
import com.etl.framework.iceberg.{
  IcebergMaintenanceRunner,
  MaintenanceActionResult,
  MaintenanceResult,
  MaintenanceStatus
}
import com.etl.framework.iceberg.catalog.CatalogRuntime
import com.etl.framework.orchestration.{ExecutionRequestJson, ExecutionStatus}
import com.etl.framework.orchestration.recovery.AttemptInspection
import com.etl.framework.pipeline.IngestionPipeline
import com.etl.framework.util.SensitiveDataRedactor
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, render}

import java.io.PrintStream
import scala.util.control.NonFatal

/** Generic entry point for YAML-only pipelines. Applications with custom Scala extensions should call the same
  * IngestionPipeline API from their own main class and preserve these exit semantics.
  */
object FloeRunner {

  private val LogicalTableName = "[A-Za-z_][A-Za-z0-9_-]{0,127}".r

  object ExitCode {
    val Success = 0
    val Usage = 2
    val InvalidRequestOrConfig = 3
    val Failed = 10
    val FailedPartial = 11
    val Unknown = 12
    val InspectionIncomplete = 13
    val MaintenanceFailed = 20
    val MaintenanceUnknown = 21
    val Internal = 70

    def forStatus(status: ExecutionStatus): Int = status match {
      case ExecutionStatus.Succeeded | ExecutionStatus.SucceededWithWarnings => Success
      case ExecutionStatus.Failed                                            => Failed
      case ExecutionStatus.FailedPartial                                     => FailedPartial
      case ExecutionStatus.Unknown                                           => Unknown
    }

    def forMaintenanceStatus(status: MaintenanceStatus): Int = status match {
      case MaintenanceStatus.Succeeded => Success
      case MaintenanceStatus.Failed    => MaintenanceFailed
      case MaintenanceStatus.Unknown   => MaintenanceUnknown
    }
  }

  def main(args: Array[String]): Unit = {
    val exitCode = execute(args.toSeq)
    if (exitCode != ExitCode.Success) System.exit(exitCode)
  }

  def execute(
      args: Seq[String],
      out: PrintStream = System.out,
      err: PrintStream = System.err
  ): Int =
    RunnerArguments.parse(args) match {
      case Left(error) =>
        err.println(error)
        ExitCode.Usage
      case Right(command) => executeCommand(command, out, err)
    }

  private def executeCommand(command: RunnerCommand, out: PrintStream, err: PrintStream): Int =
    command match {
      case validate: RunnerCommand.Validate =>
        withSparkSession(err)(spark => validatePipeline(validate, spark, out, err))
      case run: RunnerCommand.Run         => runPipeline(run, out, err)
      case inspect: RunnerCommand.Inspect => inspectPipeline(inspect, out, err)
      case maintenance: RunnerCommand.Maintenance =>
        withSparkSession(err)(spark => maintainTable(maintenance, spark, out, err))
    }

  private def validatePipeline(
      command: RunnerCommand.Validate,
      spark: SparkSession,
      out: PrintStream,
      err: PrintStream
  ): Int =
    try {
      implicit val implicitSpark: SparkSession = spark
      val builder = IngestionPipeline
        .builder()
        .withConfigDirectory(command.configDirectory)
        .withPipelineId(command.pipelineId)
        .withCodeVersion(command.codeVersion)
      val definition = builder.build().executionDefinition
      out.println(
        json(
          "status" -> JString("STATIC_VALID"),
          "pipeline_id" -> JString(definition.pipelineId),
          "code_version" -> JString(definition.codeVersion),
          "config_digest" -> JString(definition.configDigest)
        )
      )
      ExitCode.Success
    } catch {
      case error: ConfigurationException   => invalid(error, err)
      case error: IllegalArgumentException => invalid(error, err)
      case NonFatal(error)                 => internal(error, err)
    }

  private def runPipeline(command: RunnerCommand.Run, out: PrintStream, err: PrintStream): Int = {
    withSparkSession(err) { spark =>
      ExecutionRequestJson.read(command.requestPath, spark.sparkContext.hadoopConfiguration) match {
        case Left(error) =>
          err.println(SensitiveDataRedactor.redact(error))
          ExitCode.InvalidRequestOrConfig
        case Right(request) =>
          try {
            implicit val implicitSpark: SparkSession = spark
            val pipeline = IngestionPipeline
              .builder()
              .withConfigDirectory(command.configDirectory)
              .withPipelineId(request.pipelineId)
              .withCodeVersion(request.codeVersion)
              .build()
            val result = pipeline.execute(request)
            out.println(
              json(
                "status" -> JString(result.status.name),
                "success" -> JBool(result.success),
                "pipeline_id" -> JString(result.pipelineId),
                "logical_run_id" -> JString(result.logicalRunId),
                "attempt_id" -> JString(result.attemptId),
                "warnings" -> JArray(SensitiveDataRedactor.redactAll(result.warnings).map(JString).toList),
                "error" -> result.error.map(value => JString(SensitiveDataRedactor.redact(value))).getOrElse(JNull)
              )
            )
            ExitCode.forStatus(result.status)
          } catch {
            case error: ConfigurationException   => invalid(error, err)
            case error: IllegalArgumentException => invalid(error, err)
            case NonFatal(error)                 => internal(error, err)
          }
      }
    }
  }

  private def inspectPipeline(command: RunnerCommand.Inspect, out: PrintStream, err: PrintStream): Int =
    withSparkSession(err) { spark =>
      ExecutionRequestJson.read(command.requestPath, spark.sparkContext.hadoopConfiguration) match {
        case Left(error) =>
          err.println(SensitiveDataRedactor.redact(error))
          ExitCode.InvalidRequestOrConfig
        case Right(request) =>
          try {
            implicit val implicitSpark: SparkSession = spark
            val inspection = IngestionPipeline
              .builder()
              .withConfigDirectory(command.configDirectory)
              .withPipelineId(request.pipelineId)
              .withCodeVersion(request.codeVersion)
              .build()
              .inspect(request)
            out.println(inspectionJson(inspection))
            if (inspection.complete) ExitCode.Success else ExitCode.InspectionIncomplete
          } catch {
            case error: ConfigurationException   => invalid(error, err)
            case error: IllegalArgumentException => invalid(error, err)
            case NonFatal(error)                 => internal(error, err)
          }
      }
    }

  private def maintainTable(
      command: RunnerCommand.Maintenance,
      spark: SparkSession,
      out: PrintStream,
      err: PrintStream
  ): Int =
    try {
      require(
        LogicalTableName.pattern.matcher(command.tableName).matches(),
        "--table must be one logical table name matching [A-Za-z_][A-Za-z0-9_-]{0,127}, without catalog or namespace"
      )
      val loader = new GlobalConfigLoader(spark.sparkContext.hadoopConfiguration)
      val globalConfig = loader.load(s"${command.configDirectory}/global.yaml") match {
        case Right(config) => config
        case Left(error)   => throw error
      }
      globalConfig.iceberg.maintenance.requireRunnable()
      CatalogRuntime.prepare(spark, globalConfig.iceberg)
      val result = new IcebergMaintenanceRunner(spark, globalConfig.iceberg)
        .run(globalConfig.iceberg.fullTableName(command.tableName), globalConfig.iceberg.maintenance)
      out.println(maintenanceJson(result))
      ExitCode.forMaintenanceStatus(result.status)
    } catch {
      case error: ConfigurationException   => invalid(error, err)
      case error: IllegalArgumentException => invalid(error, err)
      case NonFatal(error)                 => internal(error, err)
    }

  private def withSparkSession(err: PrintStream)(operation: SparkSession => Int): Int = {
    val existing = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    try {
      val spark = SparkSession
        .builder()
        .appName("Floe")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
      val ownsSession = existing.forall(_ ne spark)
      try operation(spark)
      finally if (ownsSession) spark.stop()
    } catch {
      case NonFatal(error) => internal(error, err)
    }
  }

  private def invalid(error: Throwable, err: PrintStream): Int = {
    err.println(SensitiveDataRedactor.message(error))
    ExitCode.InvalidRequestOrConfig
  }

  private def internal(error: Throwable, err: PrintStream): Int = {
    err.println(s"${error.getClass.getName}: ${SensitiveDataRedactor.message(error)}")
    ExitCode.Internal
  }

  private def json(fields: (String, JValue)*): String = compact(render(JObject(fields.toList)))

  private def inspectionJson(inspection: AttemptInspection): String =
    json(
      "status" -> JString(inspection.status),
      "assessment" -> JString(inspection.assessment),
      "complete" -> JBool(inspection.complete),
      "retry_safe" -> JBool(inspection.retrySafe),
      "pipeline_id" -> JString(inspection.request.pipelineId),
      "logical_run_id" -> JString(inspection.request.logicalRunId),
      "attempt_id" -> JString(inspection.request.attemptId),
      "report" -> JObject(
        "path" -> JString(inspection.report.path),
        "status" -> JString(inspection.report.status),
        "length_bytes" -> inspection.report.lengthBytes.map(JInt(_)).getOrElse(JNull),
        "error" -> inspection.report.error.map(value => JString(SensitiveDataRedactor.redact(value))).getOrElse(JNull)
      ),
      "targets" -> JArray(inspection.targets.map { target =>
        JObject(
          "target_name" -> JString(target.targetName),
          "table_name" -> JString(target.tableName),
          "operation_type" -> JString(target.operationType),
          "operation_id" -> JString(target.operationId),
          "status" -> JString(target.status),
          "table_uuid" -> target.tableUuid.map(JString).getOrElse(JNull),
          "current_main_snapshot_id" -> target.currentMainSnapshotId
            .map(value => JString(value.toString))
            .getOrElse(JNull),
          "matching_snapshots" -> JArray(target.matchingSnapshots.map { snapshot =>
            JObject(
              "snapshot_id" -> JString(snapshot.snapshotId.toString),
              "parent_snapshot_id" -> snapshot.parentSnapshotId.map(value => JString(value.toString)).getOrElse(JNull),
              "committed_at_ms" -> JInt(snapshot.committedAtMs),
              "relation_to_main" -> JString(snapshot.relationToMain)
            )
          }.toList),
          "error" -> target.error.map(value => JString(SensitiveDataRedactor.redact(value))).getOrElse(JNull)
        )
      }.toList)
    )

  private def maintenanceJson(result: MaintenanceResult): String =
    json(
      "contract_version" -> JInt(result.contractVersion),
      "status" -> JString(result.status.name),
      "success" -> JBool(result.success),
      "table_name" -> JString(result.tableName),
      "table_uuid" -> result.tableUuid.map(JString).getOrElse(JNull),
      "starting_snapshot_id" -> result.startingSnapshotId.map(value => JString(value.toString)).getOrElse(JNull),
      "actions" -> JArray(result.actions.map(maintenanceActionJson).toList),
      "error" -> result.error.map(value => JString(SensitiveDataRedactor.redact(value))).getOrElse(JNull)
    )

  private def maintenanceActionJson(result: MaintenanceActionResult): JValue =
    JObject(
      "action" -> JString(result.action.name),
      "status" -> JString(result.status.name),
      "parameters" -> JObject(
        result.parameters.toSeq.sortBy(_._1).map { case (name, value) => name -> JString(value) }.toList
      ),
      "metrics" -> JObject(
        result.metrics.toSeq.sortBy(_._1).map { case (name, value) => name -> JInt(value) }.toList
      ),
      "error" -> result.error.map(value => JString(SensitiveDataRedactor.redact(value))).getOrElse(JNull)
    )
}
