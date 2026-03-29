package com.etl.framework.orchestration.flow

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths, StandardOpenOption}

/** Handles writing of flow and additional table metadata
  */
class FlowMetadataWriter(
    flowConfig: FlowConfig,
    globalConfig: GlobalConfig
) {

  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /** Writes flow execution metadata
    */
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
      case Some(iceberg) =>
        baseMetadata + ("iceberg_metadata" -> Map(
          "table_name" -> iceberg.tableName,
          "snapshot_id" -> iceberg.snapshotId,
          "snapshot_tag" -> iceberg.snapshotTag.getOrElse(""),
          "parent_snapshot_id" -> iceberg.parentSnapshotId.getOrElse(""),
          "snapshot_timestamp_ms" -> iceberg.snapshotTimestampMs,
          "records_written" -> iceberg.recordsWritten,
          "manifest_list_location" -> iceberg.manifestListLocation,
          "summary" -> iceberg.summary
        ))
      case None => baseMetadata
    }

    writeJsonToFile(metadata, metadataPath)
    logger.debug(s"Flow metadata written: ${flowConfig.name} → $metadataPath")
  }

  /** Writes a map as JSON to a file
    */
  private def writeJsonToFile(data: Map[String, Any], filePath: String): Unit = {
    val jsonString = write(data)
    val path = Paths.get(filePath)
    Files.createDirectories(path.getParent)
    Files.write(path, jsonString.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }
}
