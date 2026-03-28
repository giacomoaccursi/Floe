package com.etl.framework.orchestration.flow

import com.etl.framework.config.FlowConfig
import com.etl.framework.core.{AdditionalTableInfo, TransformationContext}
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
          val context = TransformationContext(
            currentFlow = flowConfig.name,
            currentData = data,
            validatedFlows = Map.empty, // No validated flows available yet
            batchId = batchId,
            spark = spark
          )
          val result = transformation(context)

          result.getAdditionalTables.foreach { case (name, info) =>
            additionalTables(name) = info
          }

          result.currentData
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
          val context = TransformationContext(
            currentFlow = flowConfig.name,
            currentData = data,
            validatedFlows = validatedFlows,
            batchId = batchId,
            spark = spark
          )
          val result = transformation(context)

          result.getAdditionalTables.foreach { case (name, info) =>
            additionalTables(name) = info
          }

          result.currentData
        }
      
      case None =>
        data
    }
  }
}
