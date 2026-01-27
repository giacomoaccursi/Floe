package com.etl.framework.orchestration

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.core.AdditionalTableMetadata
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import java.nio.file.{Files, Paths, StandardOpenOption}

/**
 * Handles writing of flow and additional table metadata
 */
class FlowMetadataWriter(
  flowConfig: FlowConfig,
  globalConfig: GlobalConfig
) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)
  
  /**
   * Writes flow execution metadata
   */
  def writeFlowMetadata(result: FlowResult, batchId: String): Unit = {
    val metadataPath = s"${globalConfig.paths.metadataPath}/$batchId/flows/${flowConfig.name}.json"
    
    val metadata = Map(
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
    
    writeJsonToFile(metadata, metadataPath)
    logger.debug(s"Flow metadata written: ${flowConfig.name} → $metadataPath")
  }
  
  /**
   * Writes metadata for an additional table
   */
  def writeAdditionalTableMetadata(
    tableName: String,
    data: DataFrame,
    outputPath: String,
    dagMetadata: Option[AdditionalTableMetadata],
    batchId: String
  ): Unit = {
    val metadataPath = s"${globalConfig.paths.metadataPath}/$batchId/additional_tables/${tableName}.json"

    val metadata = Map(
      "table_name" -> tableName,
      "table_type" -> "additional",
      "created_by_flow" -> flowConfig.name,
      "record_count" -> data.count(),
      "path" -> outputPath,
      "dag_metadata" -> dagMetadata.map { dm =>
        Map(
          "primary_key" -> dm.primaryKey,
          "join_keys" -> dm.joinKeys,
          "description" -> dm.description.getOrElse(""),
          "partition_by" -> dm.partitionBy
        )
      }.getOrElse(Map.empty),
      "schema" -> Map(
        "fields" -> data.schema.fields.map { field =>
          Map(
            "name" -> field.name,
            "type" -> field.dataType.typeName
          )
        }
      )
    )
    
    writeJsonToFile(metadata, metadataPath)
    logger.debug(s"Additional table metadata written: $tableName → $metadataPath")
  }
  
  /**
   * Writes a map as JSON to a file
   */
  private def writeJsonToFile(data: Map[String, Any], filePath: String): Unit = {
    val jsonString = write(data)
    val path = Paths.get(filePath)
    Files.createDirectories(path.getParent)
    Files.write(path, jsonString.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }
}
