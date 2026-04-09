package com.etl.framework.orchestration.flow

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.util.{IcebergMetadataSerializer, JsonFileWriter}
import org.slf4j.LoggerFactory

class FlowMetadataWriter(
    flowConfig: FlowConfig,
    globalConfig: GlobalConfig
) {

  private val logger = LoggerFactory.getLogger(getClass)

  def writeFlowMetadata(result: FlowResult, batchId: String): Unit = {
    val metadataPath = s"${globalConfig.paths.metadataPath}/$batchId/flows/${flowConfig.name}.json"

    val baseMetadata = Map[String, Any](
      "flow_name" -> result.flowName,
      "batch_id" -> result.batchId,
      "success" -> result.success,
      "load_mode" -> flowConfig.loadMode.`type`,
      "input_records" -> result.inputRecords,
      "merged_records" -> result.mergedRecords,
      "valid_records" -> result.validRecords,
      "rejected_records" -> result.rejectedRecords,
      "rejection_rate" -> result.rejectionRate,
      "execution_time_ms" -> result.executionTimeMs,
      "rejection_reasons" -> result.rejectionReasons,
      "error" -> result.error.getOrElse("")
    )

    val metadata = result.icebergMetadata match {
      case Some(iceberg) => baseMetadata + ("iceberg_metadata" -> IcebergMetadataSerializer.toMap(iceberg))
      case None          => baseMetadata
    }

    JsonFileWriter.write(metadata, metadataPath)
    logger.debug(s"Flow metadata written: ${flowConfig.name} → $metadataPath")
  }
}
