package com.etl.framework.orchestration.batch

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.orchestration.{ExecutionRequest, IngestionResult}
import com.etl.framework.util.{IcebergMetadataSerializer, JsonFileWriter}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class BatchMetadataWriter(
    globalConfig: GlobalConfig,
    flowConfigs: Seq[FlowConfig]
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Writes batch metadata
    */
  def writeBatchMetadata(
      request: ExecutionRequest,
      result: IngestionResult,
      executionTimeMs: Long
  ): Unit = {
    val metadataPath = s"${globalConfig.paths.metadataPath}/${request.artifactKey}/summary.json"
    val flowResults = result.flowResults

    val totalInput = flowResults.map(_.inputRecords).sum
    val totalValid = flowResults.map(_.validRecords).sum
    val totalRejected = flowResults.map(_.rejectedRecords).sum
    val overallRejectionRate = if (totalInput > 0) totalRejected.toDouble / totalInput else 0.0

    val orphanReportsData = result.orphanReports.map { report =>
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
      "contract_version" -> request.contractVersion,
      "pipeline_id" -> request.pipelineId,
      "logical_run_id" -> request.logicalRunId,
      "attempt_id" -> request.attemptId,
      "effective_at" -> request.effectiveAt.toString,
      "code_version" -> request.codeVersion,
      "config_digest" -> request.configDigest,
      "retry_profile" -> request.retryProfile.name,
      "data_interval" -> request.dataInterval
        .map(interval =>
          Map(
            "start_inclusive" -> interval.startInclusive.toString,
            "end_exclusive" -> interval.endExclusive.toString
          )
        )
        .getOrElse(Map.empty[String, String]),
      "input_references" -> request.inputReferences.map(reference =>
        Map(
          "dataset_id" -> reference.datasetId,
          "version" -> reference.version,
          "read_mode" -> reference.readMode
        )
      ),
      "platform_references" -> request.platformReferences,
      "execution_time_ms" -> executionTimeMs,
      "success" -> result.success,
      "status" -> result.status.name,
      "error" -> result.error.getOrElse(""),
      "warnings" -> result.warnings,
      "flows_processed" -> flowResults.size,
      "total_input_records" -> totalInput,
      "total_valid_records" -> totalValid,
      "total_rejected_records" -> totalRejected,
      "overall_rejection_rate" -> overallRejectionRate,
      "orphan_reports" -> orphanReportsData,
      "derived_tables_processed" -> result.derivedTableResults.size,
      "derived_tables" -> result.derivedTableResults.map { derived =>
        Map[String, Any](
          "table_name" -> derived.tableName,
          "success" -> derived.success,
          "records_written" -> derived.recordsWritten,
          "snapshot_id" -> derived.snapshotId.map(_.toString).getOrElse(""),
          "resulting_snapshot_id" -> derived.resultingSnapshotId.map(_.toString).getOrElse(""),
          "operation_id" -> derived.operationId.getOrElse(""),
          "data_outcome" -> derived.dataOutcome.name,
          "error" -> derived.error.getOrElse("")
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
          "data_outcome" -> result.dataOutcome.name,
          "resulting_snapshot_id" -> result.resultingSnapshotId.map(_.toString).getOrElse(""),
          "resulting_schema_json" -> result.resultingSchemaJson.getOrElse("")
        )

        result.icebergMetadata match {
          case Some(iceberg) =>
            baseFlowMetadata + ("iceberg_metadata" -> IcebergMetadataSerializer.toMap(iceberg))
          case None => baseFlowMetadata
        }
      }
    )

    JsonFileWriter.write(metadata, metadataPath, spark.sparkContext.hadoopConfiguration)

    logger.debug(s"Batch report written - attemptId: ${request.attemptId}, path: $metadataPath")
  }
}
