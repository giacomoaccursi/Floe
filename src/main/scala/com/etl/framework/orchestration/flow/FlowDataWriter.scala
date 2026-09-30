package com.etl.framework.orchestration.flow

import com.etl.framework.config.{FlowConfig, GlobalConfig, LoadMode}
import com.etl.framework.iceberg.{CommitContext, IcebergTableWriter, WriteResult}
import com.etl.framework.orchestration.ExecutionRequest
import com.etl.framework.util.TimingUtil
import com.etl.framework.validation.ValidationColumns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

/** Handles writing of validated, rejected, and additional table data
  */
class FlowDataWriter(
    flowConfig: FlowConfig,
    globalConfig: GlobalConfig,
    icebergTableWriter: IcebergTableWriter
) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Writes validated data to Iceberg
    */
  def writeValidated(
      validData: DataFrame,
      request: ExecutionRequest,
      beforeDataCommit: () => Unit
  ): WriteResult = {
    val operationType = flowConfig.loadMode.`type`.name
    val context = CommitContext.forFlow(request, flowConfig.name, operationType)
    val result = flowConfig.loadMode.`type` match {
      case LoadMode.Full  => icebergTableWriter.writeFullLoad(validData, flowConfig, context, beforeDataCommit)
      case LoadMode.Delta => icebergTableWriter.writeDeltaLoad(validData, flowConfig, context, beforeDataCommit)
      case LoadMode.SCD2  => icebergTableWriter.writeSCD2Load(validData, flowConfig, context, beforeDataCommit)
    }
    icebergTableWriter.tagBatchSnapshot(flowConfig, result, request.attemptId)
  }

  /** Writes rejected data
    */
  def writeRejected(rejectedData: DataFrame, request: ExecutionRequest): Unit = {
    val rejectedBasePath = flowConfig.output.rejectedPath.getOrElse(
      s"${globalConfig.paths.rejectedPath}/${flowConfig.name}"
    )
    val rejectedPath = artifactPath(rejectedBasePath, request)

    TimingUtil.timed(logger, s"Write rejected data to $rejectedPath") {
      val rejectedWithAudit = rejectedData
        .withColumn(BATCH_ID, lit(request.attemptId))

      rejectedWithAudit.write
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .save(rejectedPath)
    }
  }

  /** Writes validation warnings (PK + warning metadata) to a separate Parquet file.
    */
  def writeWarnings(warnedData: DataFrame, request: ExecutionRequest): Unit = {
    val basePath = globalConfig.paths.warningsPath.getOrElse(s"${globalConfig.paths.outputPath}/warnings")
    val warningsPath = artifactPath(s"$basePath/${flowConfig.name}", request)

    TimingUtil.timed(logger, s"Write warnings to $warningsPath") {
      warnedData
        .withColumn(BATCH_ID, lit(request.attemptId))
        .write
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .save(warningsPath)
    }
  }

  private def artifactPath(basePath: String, request: ExecutionRequest): String =
    s"$basePath/pipeline_id=${request.pipelineId}/logical_run_id=${request.logicalRunId}/attempt_id=${request.attemptId}"

}
