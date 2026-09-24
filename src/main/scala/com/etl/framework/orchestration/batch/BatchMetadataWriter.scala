package com.etl.framework.orchestration.batch

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.iceberg.{MaintenanceResult, OrphanReport}
import com.etl.framework.orchestration.flow.FlowResult
import com.etl.framework.pipeline.DerivedTableResult
import com.etl.framework.util.{IcebergMetadataSerializer, JsonFileWriter}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.time.Instant

class BatchMetadataWriter(
    globalConfig: GlobalConfig,
    flowConfigs: Seq[FlowConfig]
)(implicit spark: SparkSession) {

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
      orphanDetectionError: Option[String] = None,
      derivedTableResults: Seq[DerivedTableResult] = Seq.empty,
      maintenanceResults: Seq[MaintenanceResult] = Seq.empty
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
      "derived_tables_processed" -> derivedTableResults.size,
      "derived_tables" -> derivedTableResults.map { result =>
        Map[String, Any](
          "table_name" -> result.tableName,
          "success" -> result.success,
          "records_written" -> result.recordsWritten,
          "error" -> result.error.getOrElse("")
        )
      },
      "maintenance_status" -> (if (maintenanceResults.isEmpty) "skipped"
                               else if (maintenanceResults.forall(_.status.name == "QUEUED")) "queued"
                               else if (maintenanceResults.forall(_.success)) "succeeded"
                               else "failed"),
      "maintenance_success" -> (maintenanceResults.nonEmpty && maintenanceResults.forall(_.success)),
      "maintenance_results" -> maintenanceResults.map { result =>
        Map[String, Any](
          "target_name" -> result.targetName,
          "target_type" -> result.targetType,
          "status" -> result.status.name,
          "success" -> result.success,
          "error" -> result.error.getOrElse("")
        )
      },
      "flows" -> flowResults.map { result =>
        val baseFlowMetadata = Map[String, Any](
          "flow_name" -> result.flowName,
          "success" -> result.success,
          "load_mode" -> flowConfigs.find(_.name == result.flowName).map(_.loadMode.`type`.name).getOrElse("unknown"),
          "input_records" -> result.inputRecords,
          "merged_records" -> result.mergedRecords,
          "valid_records" -> result.validRecords,
          "rejected_records" -> result.rejectedRecords,
          "rejection_rate" -> result.rejectionRate,
          "execution_time_ms" -> result.executionTimeMs,
          "rejection_reasons" -> result.rejectionReasons,
          "error" -> result.error.getOrElse(""),
          "warnings" -> result.warnings,
          "write_attempted" -> result.writeAttempted,
          "retryable" -> result.retryable
        )

        result.icebergMetadata match {
          case Some(iceberg) =>
            baseFlowMetadata + ("iceberg_metadata" -> IcebergMetadataSerializer.toMap(iceberg))
          case None => baseFlowMetadata
        }
      }
    )

    JsonFileWriter.write(metadata, metadataPath, spark.sparkContext.hadoopConfiguration)

    logger.debug(s"Batch metadata written - batchId: $batchId, path: $metadataPath")
  }
}
