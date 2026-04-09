package com.etl.framework.orchestration.batch

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.iceberg.OrphanReport
import com.etl.framework.orchestration.flow.FlowResult
import com.etl.framework.util.{IcebergMetadataSerializer, JsonFileWriter}
import org.slf4j.LoggerFactory

import java.time.Instant

class BatchMetadataWriter(
    globalConfig: GlobalConfig,
    flowConfigs: Seq[FlowConfig]
) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Writes batch metadata
    */
  def writeBatchMetadata(
      batchId: String,
      flowResults: Seq[FlowResult],
      executionTimeMs: Long,
      success: Boolean,
      rolledBack: Boolean = false,
      orphanReports: Seq[OrphanReport] = Seq.empty,
      orphanDetectionError: Option[String] = None
  ): Unit = {
    val metadataPath = s"${globalConfig.paths.metadataPath}/$batchId/summary.json"

    val totalInput = flowResults.map(_.inputRecords).sum
    val totalValid = flowResults.map(_.validRecords).sum
    val totalRejected = flowResults.map(_.rejectedRecords).sum
    val overallRejectionRate = if (totalInput > 0) totalRejected.toDouble / totalInput else 0.0

    val orphanReportsData = orphanReports.map { report =>
      val base = Map[String, Any](
        "flow_name" -> report.flowName,
        "fk_name" -> report.fkName,
        "parent_flow_name" -> report.parentFlowName,
        "orphan_count" -> report.orphanCount,
        "removed_parent_key_count" -> report.removedParentKeyCount,
        "action_taken" -> report.actionTaken,
        "deleted_child_key_count" -> report.deletedChildKeyCount
      )
      report.cascadeSource match {
        case Some(src) => base + ("cascade_source" -> src)
        case None      => base
      }
    }

    val metadata = Map(
      "batch_id" -> batchId,
      "execution_start" -> Instant.now().toString,
      "execution_time_ms" -> executionTimeMs,
      "success" -> success,
      "rolled_back" -> rolledBack,
      "flows_processed" -> flowResults.size,
      "total_input_records" -> totalInput,
      "total_valid_records" -> totalValid,
      "total_rejected_records" -> totalRejected,
      "overall_rejection_rate" -> overallRejectionRate,
      "orphan_reports" -> orphanReportsData,
      "orphan_detection_error" -> orphanDetectionError.getOrElse(""),
      "flows" -> flowResults.map { result =>
        val baseFlowMetadata = Map[String, Any](
          "flow_name" -> result.flowName,
          "success" -> result.success,
          "load_mode" -> flowConfigs.find(_.name == result.flowName).map(_.loadMode.`type`).getOrElse("unknown"),
          "input_records" -> result.inputRecords,
          "merged_records" -> result.mergedRecords,
          "valid_records" -> result.validRecords,
          "rejected_records" -> result.rejectedRecords,
          "rejection_rate" -> result.rejectionRate,
          "execution_time_ms" -> result.executionTimeMs,
          "rejection_reasons" -> result.rejectionReasons,
          "error" -> result.error.getOrElse("")
        )

        result.icebergMetadata match {
          case Some(iceberg) =>
            baseFlowMetadata + ("iceberg_metadata" -> IcebergMetadataSerializer.toMap(iceberg))
          case None => baseFlowMetadata
        }
      }
    )

    JsonFileWriter.write(metadata, metadataPath)

    logger.debug(s"Batch metadata written - batchId: $batchId, path: $metadataPath")
  }

  def generateBatchId(): String = {
    val format = globalConfig.processing.batchIdFormat
    val timestamp = Instant.now()

    import java.time.format.DateTimeFormatter

    format match {
      case "timestamp" =>
        timestamp.toEpochMilli.toString

      case "datetime" =>
        DateTimeFormatter
          .ofPattern("yyyyMMdd_HHmmss")
          .withZone(java.time.ZoneId.systemDefault())
          .format(timestamp)

      case _ =>
        // Default to datetime format
        DateTimeFormatter
          .ofPattern("yyyyMMdd_HHmmss")
          .withZone(java.time.ZoneId.systemDefault())
          .format(timestamp)
    }
  }
}
