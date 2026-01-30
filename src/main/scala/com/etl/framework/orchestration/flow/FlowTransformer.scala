package com.etl.framework.orchestration.flow

import com.etl.framework.config.FlowConfig
import com.etl.framework.core.{AdditionalTableMetadata, TransformationContext}
import com.etl.framework.util.TimingUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Handles pre and post validation transformations
 */
class FlowTransformer(
  flowConfig: FlowConfig,
  additionalTables: mutable.Map[String, AdditionalTableInfo]
)(implicit spark: SparkSession) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Applies pre-validation transformations
   */
  def applyPreValidationTransformation(data: DataFrame, batchId: String): DataFrame = {
    flowConfig.preValidationTransformation match {
      case Some(transformation) =>
        TimingUtil.timed(logger, "Pre-validation transformation") {
          val inputCount = data.count()
          val context = TransformationContext(
            currentFlow = flowConfig.name,
            currentData = data,
            validatedFlows = Map.empty, // No validated flows available yet
            batchId = batchId,
            spark = spark
          )
          val transformed = transformation(context)
          val outputCount = transformed.count()
          logger.info(s"Pre-validation transformation: $inputCount → $outputCount records")
          transformed
        }
      
      case None =>
        data
    }
  }
  
  /**
   * Applies post-validation transformations
   */
  def applyPostValidationTransformation(
    data: DataFrame,
    batchId: String,
    validatedFlows: Map[String, DataFrame]
  ): DataFrame = {
    flowConfig.postValidationTransformation match {
      case Some(transformation) =>
        TimingUtil.timed(logger, "Post-validation transformation") {
          val inputCount = data.count()
          
          // Create context with custom addTable implementation
          val context = new TransformationContext(
            currentFlow = flowConfig.name,
            currentData = data,
            validatedFlows = validatedFlows,
            batchId = batchId,
            spark = spark
          ) {
            override def addTable(
              tableName: String,
              data: DataFrame,
              outputPath: Option[String] = None,
              dagMetadata: Option[AdditionalTableMetadata] = None
            ): Unit = {
              // Store table info for later writing
              additionalTables(tableName) = AdditionalTableInfo(
                tableName = tableName,
                data = data,
                outputPath = outputPath,
                dagMetadata = dagMetadata
              )
            }
          }
          
          val transformed = transformation(context)
          val outputCount = transformed.count()
          logger.info(s"Post-validation transformation: $inputCount → $outputCount records")
          transformed
        }
      
      case None =>
        data
    }
  }
}
