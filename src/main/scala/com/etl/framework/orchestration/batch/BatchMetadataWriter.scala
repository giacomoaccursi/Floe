package com.etl.framework.orchestration.batch

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.iceberg.OrphanReport
import com.etl.framework.orchestration.flow.FlowResult
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.Instant

/** Handles writing of batch execution metadata
  */
class BatchMetadataWriter(
    globalConfig: GlobalConfig,
    flowConfigs: Seq[FlowConfig]
) {

  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /** Writes batch metadata
    */
  def writeBatchMetadata(
      batchId: String,
      flowResults: Seq[FlowResult],
      executionTimeMs: Long,
      success: Boolean,
      rolledBack: Boolean = false,
      orphanReports: Seq[OrphanReport] = Seq.empty
  ): Unit = {
    val metadataPath = s"${globalConfig.paths.metadataPath}/$batchId/summary.json"

    val totalInput = flowResults.map(_.inputRecords).sum
    val totalValid = flowResults.map(_.validRecords).sum
    val totalRejected = flowResults.map(_.rejectedRecords).sum
    val overallRejectionRate = if (totalInput > 0) totalRejected.toDouble / totalInput else 0.0

    val orphanReportsData = orphanReports.map { report =>
      Map[String, Any](
        "flow_name" -> report.flowName,
        "fk_name" -> report.fkName,
        "parent_flow_name" -> report.parentFlowName,
        "orphan_count" -> report.orphanCount,
        "removed_parent_key_count" -> report.removedParentKeyCount,
        "action_taken" -> report.actionTaken,
        "deleted_child_key_count" -> report.deletedChildKeyCount
      )
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
            baseFlowMetadata + ("iceberg_metadata" -> Map(
              "table_name" -> iceberg.tableName,
              "snapshot_id" -> iceberg.snapshotId,
              "snapshot_tag" -> iceberg.snapshotTag.getOrElse(""),
              "parent_snapshot_id" -> iceberg.parentSnapshotId.getOrElse(""),
              "snapshot_timestamp_ms" -> iceberg.snapshotTimestampMs,
              "records_written" -> iceberg.recordsWritten,
              "manifest_list_location" -> iceberg.manifestListLocation,
              "summary" -> iceberg.summary
            ))
          case None => baseFlowMetadata
        }
      }
    )

    writeJsonToFile(metadata, metadataPath)
    createLatestSymlink(batchId)

    logger.debug(s"Batch metadata written - batchId: $batchId, path: $metadataPath")
  }

  /** Writes a map as JSON to a file
    */
  private def writeJsonToFile(data: Map[String, Any], filePath: String): Unit = {
    val jsonString = write(data)
    val path = Paths.get(filePath)
    Files.createDirectories(path.getParent)
    Files.write(path, jsonString.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  /** Creates a "latest" symlink pointing to the current batch
    */
  private def createLatestSymlink(batchId: String): Unit = {
    val latestPath = Paths.get(s"${globalConfig.paths.metadataPath}/latest")
    val targetPath = Paths.get(s"${globalConfig.paths.metadataPath}/$batchId")

    try {
      // Delete existing symlink if it exists
      if (Files.exists(latestPath)) {
        Files.delete(latestPath)
      }

      // Create new symlink
      Files.createSymbolicLink(latestPath, targetPath)
      logger.debug(s"Created 'latest' symlink for batch $batchId")
    } catch {
      case e: Exception =>
        logger.warn(s"Could not create 'latest' symlink: ${e.getMessage}")
    }
  }

  /** Generates a unique batch ID
    */
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
