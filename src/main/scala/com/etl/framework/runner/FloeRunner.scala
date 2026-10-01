package com.etl.framework.runner

import com.etl.framework.exceptions.ConfigurationException
import com.etl.framework.orchestration.{ExecutionRequestJson, ExecutionStatus}
import com.etl.framework.pipeline.IngestionPipeline
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, render}

import java.io.PrintStream
import scala.util.control.NonFatal

/** Generic entry point for YAML-only pipelines. Applications with custom Scala extensions should call the same
  * IngestionPipeline API from their own main class and preserve these exit semantics.
  */
object FloeRunner {

  object ExitCode {
    val Success = 0
    val Usage = 2
    val InvalidRequestOrConfig = 3
    val Failed = 10
    val FailedPartial = 11
    val Unknown = 12
    val Internal = 70

    def forStatus(status: ExecutionStatus): Int = status match {
      case ExecutionStatus.Succeeded | ExecutionStatus.SucceededWithWarnings => Success
      case ExecutionStatus.Failed                                            => Failed
      case ExecutionStatus.FailedPartial                                     => FailedPartial
      case ExecutionStatus.Unknown                                           => Unknown
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
      case run: RunnerCommand.Run => runPipeline(run, out, err)
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
          err.println(error)
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
                "warnings" -> JArray(result.warnings.map(JString).toList),
                "error" -> result.error.map(JString).getOrElse(JNull)
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
    err.println(Option(error.getMessage).getOrElse(error.getClass.getName))
    ExitCode.InvalidRequestOrConfig
  }

  private def internal(error: Throwable, err: PrintStream): Int = {
    err.println(s"${error.getClass.getName}: ${Option(error.getMessage).getOrElse("no message")}")
    ExitCode.Internal
  }

  private def json(fields: (String, JValue)*): String = compact(render(JObject(fields.toList)))
}
