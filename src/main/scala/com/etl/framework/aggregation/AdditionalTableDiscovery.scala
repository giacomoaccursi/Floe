package com.etl.framework.aggregation

import com.etl.framework.config.{DAGNode, GlobalConfig, JoinConfig}
import com.etl.framework.core.AdditionalTableMetadata
import org.slf4j.LoggerFactory

/**
 * Metadata about an additional table loaded from disk (for discovery)
 */
case class AdditionalTableMetadataFile(
  tableName: String,
  path: String,
  dagMetadata: AdditionalTableMetadata
)

/**
 * Discovers additional tables from metadata
 */
class AdditionalTableDiscovery(globalConfig: GlobalConfig) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Discovers additional tables from metadata (public for testing)
   */
  def discoverAdditionalTables(): Seq[DAGNode] = {
    val metadataPath = s"${globalConfig.paths.metadataPath}/latest"
    logger.info(s"Reading additional tables metadata from: $metadataPath")
    
    try {
      val additionalTables = loadAdditionalTablesMetadata(metadataPath)
      logger.info(s"Found ${additionalTables.size} additional tables with DAG metadata")
      
      additionalTables.map { table =>
        val node = DAGNode(
          id = s"${table.tableName}_node",
          description = table.dagMetadata.description.getOrElse(s"Auto-discovered table: ${table.tableName}"),
          sourceFlow = table.tableName,
          sourcePath = table.path,
          dependencies = inferDependencies(table),
          join = inferJoinConfig(table),
          select = Seq.empty,
          filters = Seq.empty
        )
        logger.info(s"Created DAG node for additional table: ${table.tableName}")
        node
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Could not discover additional tables: ${e.getMessage}")
        Seq.empty
    }
  }
  
  /**
   * Loads additional tables metadata from the metadata directory
   */
  private def loadAdditionalTablesMetadata(metadataPath: String): Seq[AdditionalTableMetadataFile] = {
    import java.nio.file.{Files, Paths}
    import scala.collection.JavaConverters._
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    
    val additionalTablesDir = Paths.get(s"$metadataPath/additional_tables")
    
    if (!Files.exists(additionalTablesDir)) {
      logger.info("No additional tables directory found")
      return Seq.empty
    }
    
    implicit val formats: Formats = DefaultFormats
    
    Files.list(additionalTablesDir)
      .iterator()
      .asScala
      .filter(p => p.toString.endsWith(".json"))
      .flatMap { metadataFile =>
        try {
          val jsonContent = new String(Files.readAllBytes(metadataFile))
          val json = parse(jsonContent)
          
          val tableName = (json \ "table_name").extract[String]
          val path = (json \ "path").extract[String]
          
          val dagMetadataOpt = (json \ "dag_metadata").toOption.map { dagJson =>
            AdditionalTableMetadata(
              primaryKey = (dagJson \ "primary_key").extract[Seq[String]],
              joinKeys = (dagJson \ "join_keys").extract[Map[String, Seq[String]]],
              description = (dagJson \ "description").extractOpt[String],
              partitionBy = (dagJson \ "partition_by").extractOpt[Seq[String]].getOrElse(Seq.empty)
            )
          }
          
          dagMetadataOpt.map { dagMetadata =>
            AdditionalTableMetadataFile(tableName, path, dagMetadata)
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Could not parse metadata file ${metadataFile}: ${e.getMessage}")
            None
        }
      }
      .toSeq
  }
  
  /**
   * Infers dependencies from join keys
   */
  private def inferDependencies(table: AdditionalTableMetadataFile): Seq[String] = {
    table.dagMetadata.joinKeys.keys.map(flow => s"${flow}_node").toSeq
  }
  
  /**
   * Infers join configuration from metadata
   */
  private def inferJoinConfig(table: AdditionalTableMetadataFile): Option[JoinConfig] = {
    table.dagMetadata.joinKeys.headOption.map { case (parentFlow, keys) =>
      JoinConfig(
        `type` = "left_outer",
        parent = s"${parentFlow}_node",
        on = keys.map(key => com.etl.framework.config.JoinCondition(left = key, right = key)),
        strategy = "nest",
        nestAs = Some(table.tableName),
        aggregations = Seq.empty
      )
    }
  }
}
