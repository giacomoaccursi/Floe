package com.etl.framework.core

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Immutable context available during transformations (both pre- and post-validation)
  */
final case class TransformationContext private (
    currentFlow: String,
    currentData: DataFrame,
    validatedFlows: Map[String, DataFrame],
    batchId: String,
    spark: SparkSession
) {

  /** Get DataFrame of another already validated flow
    */
  def getFlow(flowName: String): Option[DataFrame] =
    validatedFlows.get(flowName)

  /** Return a new context with a different DataFrame, preserving everything else.
    */
  def withData(newData: DataFrame): TransformationContext =
    copy(currentData = newData)
}

object TransformationContext {

  def apply(
      currentFlow: String,
      currentData: DataFrame,
      validatedFlows: Map[String, DataFrame],
      batchId: String,
      spark: SparkSession
  ): TransformationContext = new TransformationContext(
    currentFlow = currentFlow,
    currentData = currentData,
    validatedFlows = validatedFlows,
    batchId = batchId,
    spark = spark
  )
}
