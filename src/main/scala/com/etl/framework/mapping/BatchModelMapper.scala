package com.etl.framework.mapping

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import scala.jdk.CollectionConverters._
import java.io.FileInputStream

/**
 * Maps DataFrame to typed Dataset[BatchModel]
 * Supports column mappings and expressions from YAML configuration
 */
class BatchModelMapper[T: Encoder](
  mappingConfig: MappingConfig
)(implicit spark: SparkSession) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Maps DataFrame to Dataset[T]
   */
  def map(df: DataFrame): Dataset[T] = {
    logger.info(s"Mapping DataFrame to Dataset with ${mappingConfig.mappings.size} field mappings")
    
    // Apply column mappings from YAML
    val mapped = applyMappings(df, mappingConfig)
    
    logger.info("Converting mapped DataFrame to typed Dataset")
    
    // Convert to Dataset
    try {
      mapped.as[T]
    } catch {
      case e: Exception =>
        logger.error(s"Failed to convert DataFrame to Dataset: ${e.getMessage}")
        logger.error(s"DataFrame schema: ${mapped.schema.treeString}")
        throw new DataFrameToDatasetMappingException(
          s"Failed to map DataFrame to Dataset: ${e.getMessage}. " +
          s"Ensure the DataFrame schema matches the case class structure.",
          e
        )
    }
  }
  
  /**
   * Applies field mappings from configuration
   */
  private def applyMappings(df: DataFrame, config: MappingConfig): DataFrame = {
    logger.debug(s"Applying ${config.mappings.size} field mappings")
    
    config.mappings.foldLeft(df) { (currentDf, mapping) =>
      mapping.expression match {
        case Some(exprStr) =>
          // Apply expression
          logger.debug(s"Applying expression for field ${mapping.targetField}: $exprStr")
          try {
            currentDf.withColumn(mapping.targetField, expr(exprStr))
          } catch {
            case e: Exception =>
              logger.error(s"Failed to apply expression '$exprStr' for field ${mapping.targetField}: ${e.getMessage}")
              throw new MappingExpressionException(
                s"Failed to apply expression '$exprStr' for field ${mapping.targetField}: ${e.getMessage}",
                e
              )
          }
        
        case None =>
          // Simple rename or keep as-is
          if (mapping.sourceField != mapping.targetField) {
            logger.debug(s"Renaming column ${mapping.sourceField} to ${mapping.targetField}")
            if (currentDf.columns.contains(mapping.sourceField)) {
              currentDf.withColumnRenamed(mapping.sourceField, mapping.targetField)
            } else {
              logger.warn(s"Source field ${mapping.sourceField} not found in DataFrame, skipping")
              currentDf
            }
          } else {
            // No change needed
            currentDf
          }
      }
    }
  }
}

/**
 * Mapping configuration loaded from YAML
 */
case class MappingConfig(
  mappings: Seq[FieldMapping]
)

/**
 * Field mapping specification
 */
case class FieldMapping(
  sourceField: String,
  targetField: String,
  expression: Option[String] = None
)

/**
 * Companion object for loading mapping configuration from YAML
 */
object BatchModelMapper {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Loads mapping configuration from YAML file
   */
  def loadMappingConfig(mappingFilePath: String): MappingConfig = {
    logger.info(s"Loading mapping configuration from: $mappingFilePath")
    
    try {
      val yaml = new Yaml()
      
      scala.util.Using(new FileInputStream(mappingFilePath)) { inputStream =>
        val data = yaml.load(inputStream).asInstanceOf[java.util.Map[String, Any]]
        
        // Parse mappings
        val mappingsData = data.get("mappings").asInstanceOf[java.util.List[java.util.Map[String, Any]]]
        mappingsData.asScala.map { mappingData =>
          val sourceField = mappingData.get("sourceField").asInstanceOf[String]
          val targetField = mappingData.get("targetField").asInstanceOf[String]
          val expression = Option(mappingData.get("expression")).map(_.asInstanceOf[String])
          
          FieldMapping(sourceField, targetField, expression)
        }.toSeq
      } match {
        case scala.util.Success(mappings) =>
          logger.info(s"Loaded ${mappings.size} field mappings")
          MappingConfig(mappings)
        case scala.util.Failure(e) =>
          logger.error(s"Failed to load mapping configuration from $mappingFilePath: ${e.getMessage}")
          throw new MappingConfigLoadException(
            s"Failed to load mapping configuration from $mappingFilePath: ${e.getMessage}",
            e
          )
      }
    } catch {
      case e: MappingConfigLoadException => throw e
      case e: Exception =>
        logger.error(s"Failed to load mapping configuration from $mappingFilePath: ${e.getMessage}")
        throw new MappingConfigLoadException(
          s"Failed to load mapping configuration from $mappingFilePath: ${e.getMessage}",
          e
        )
    }
  }
  
  /**
   * Creates a BatchModelMapper with mapping loaded from file
   */
  def fromFile[T: Encoder](mappingFilePath: String)(implicit spark: SparkSession): BatchModelMapper[T] = {
    val config = loadMappingConfig(mappingFilePath)
    new BatchModelMapper[T](config)
  }
  
  /**
   * Creates a BatchModelMapper with inline mapping configuration
   */
  def fromConfig[T: Encoder](config: MappingConfig)(implicit spark: SparkSession): BatchModelMapper[T] = {
    new BatchModelMapper[T](config)
  }
}

/**
 * Exception thrown when DataFrame to Dataset mapping fails
 */
class DataFrameToDatasetMappingException(message: String, cause: Throwable = null) 
  extends Exception(message, cause)

/**
 * Exception thrown when mapping expression fails
 */
class MappingExpressionException(message: String, cause: Throwable = null) 
  extends Exception(message, cause)

/**
 * Exception thrown when mapping configuration loading fails
 */
class MappingConfigLoadException(message: String, cause: Throwable = null) 
  extends Exception(message, cause)
