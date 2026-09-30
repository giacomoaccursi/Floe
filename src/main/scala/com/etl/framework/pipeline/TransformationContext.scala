package com.etl.framework.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.Instant

/** Immutable context available during transformations (both pre- and post-validation)
  */
final case class TransformationContext private (
    currentFlow: String,
    currentData: DataFrame,
    validatedFlows: Map[String, DataFrame],
    logicalRunId: String,
    attemptId: String,
    effectiveAt: Instant,
    spark: SparkSession
) {

  /** Deprecated vocabulary retained as a read-only alias: batchId is the physical attempt ID. */
  def batchId: String = attemptId

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
      logicalRunId: String,
      attemptId: String,
      effectiveAt: Instant,
      spark: SparkSession
  ): TransformationContext = new TransformationContext(
    currentFlow = currentFlow,
    currentData = currentData,
    validatedFlows = validatedFlows,
    logicalRunId = logicalRunId,
    attemptId = attemptId,
    effectiveAt = effectiveAt,
    spark = spark
  )
}
