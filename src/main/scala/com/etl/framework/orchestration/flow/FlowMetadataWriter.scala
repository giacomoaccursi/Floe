package com.etl.framework.orchestration.flow

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.orchestration.ExecutionRequest
import com.etl.framework.util.{IcebergMetadataSerializer, JsonFileWriter}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class FlowMetadataWriter(
    flowConfig: FlowConfig,
    globalConfig: GlobalConfig
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Writes flow result metadata (record counts, Iceberg snapshot info) to a JSON file. */
  def writeFlowMetadata(result: FlowResult, request: ExecutionRequest): Unit = {
    val metadataPath = s"${globalConfig.paths.metadataPath}/${request.artifactKey}/flows/${flowConfig.name}.json"

    val baseMetadata = Map[String, Any](
      "flow_name" -> result.flowName,
      "pipeline_id" -> request.pipelineId,
      "logical_run_id" -> request.logicalRunId,
      "attempt_id" -> request.attemptId,
      "effective_at" -> request.effectiveAt.toString,
      "success" -> result.success,
      "load_mode" -> flowConfig.loadMode.`type`.name,
      "input_records" -> result.inputRecords,
      "merged_records" -> result.mergedRecords,
      "valid_records" -> result.validRecords,
      "rejected_records" -> result.rejectedRecords,
      "rejection_rate" -> result.rejectionRate,
      "execution_time_ms" -> result.executionTimeMs,
      "rejection_reasons" -> result.rejectionReasons,
      "error" -> result.error.getOrElse(""),
      "warnings" -> result.warnings,
      "data_outcome" -> result.dataOutcome.name,
      "resulting_snapshot_id" -> result.resultingSnapshotId.map(_.toString).getOrElse(""),
      "resulting_schema_json" -> result.resultingSchemaJson.getOrElse("")
    )

    val metadata = result.icebergMetadata match {
      case Some(iceberg) => baseMetadata + ("iceberg_metadata" -> IcebergMetadataSerializer.toMap(iceberg))
      case None          => baseMetadata
    }

    JsonFileWriter.write(metadata, metadataPath, spark.sparkContext.hadoopConfiguration)
    logger.debug(s"Flow metadata written: ${flowConfig.name} → $metadataPath")
  }
}
