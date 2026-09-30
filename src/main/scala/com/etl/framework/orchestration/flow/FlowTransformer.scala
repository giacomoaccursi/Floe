package com.etl.framework.orchestration.flow

import com.etl.framework.config.FlowConfig
import com.etl.framework.pipeline.TransformationContext
import com.etl.framework.orchestration.ExecutionRequest
import com.etl.framework.util.TimingUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/** Handles pre and post validation transformations
  */
class FlowTransformer(flowConfig: FlowConfig)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Applies pre-validation transformations
    */
  def applyPreValidationTransformation(data: DataFrame, request: ExecutionRequest): DataFrame = {
    flowConfig.preValidationTransformation match {
      case Some(transformation) =>
        TimingUtil.timed(logger, "Pre-validation transformation") {
          val context = TransformationContext(
            currentFlow = flowConfig.name,
            currentData = data,
            validatedFlows = Map.empty,
            logicalRunId = request.logicalRunId,
            attemptId = request.attemptId,
            effectiveAt = request.effectiveAt,
            spark = spark
          )
          transformation(context).currentData
        }
      case None => data
    }
  }

  /** Applies post-validation transformations
    */
  def applyPostValidationTransformation(
      data: DataFrame,
      request: ExecutionRequest,
      validatedFlows: Map[String, DataFrame]
  ): DataFrame = {
    flowConfig.postValidationTransformation match {
      case Some(transformation) =>
        TimingUtil.timed(logger, "Post-validation transformation") {
          val context = TransformationContext(
            currentFlow = flowConfig.name,
            currentData = data,
            validatedFlows = validatedFlows,
            logicalRunId = request.logicalRunId,
            attemptId = request.attemptId,
            effectiveAt = request.effectiveAt,
            spark = spark
          )
          transformation(context).currentData
        }
      case None => data
    }
  }
}
