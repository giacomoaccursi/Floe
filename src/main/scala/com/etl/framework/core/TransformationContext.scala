package com.etl.framework.core

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Immutable context available during transformations (both pre- and post-validation)
 */
final case class TransformationContext private(
  currentFlow: String,
  currentData: DataFrame,
  validatedFlows: Map[String, DataFrame],
  batchId: String,
  spark: SparkSession,
  additionalTables: Map[String, AdditionalTableInfo]
) {

  /**
   * Get DataFrame of another already validated flow
   */
  def getFlow(flowName: String): Option[DataFrame] = 
    validatedFlows.get(flowName)

  /**
   * Add a new table and return a new context.
   * The table will be automatically saved and made available for DAG aggregation.
   */
  def addTable(
    tableName: String,
    data: DataFrame,
    outputPath: Option[String] = None,
    dagMetadata: Option[AdditionalTableMetadata] = None
  ): TransformationContext = {
    val tableInfo = AdditionalTableInfo(
      tableName = tableName,
      data = data,
      outputPath = outputPath,
      dagMetadata = dagMetadata
    )
    copy(additionalTables = additionalTables + (tableName -> tableInfo))
  }
  
  /**
   * Get all additional tables that were added during transformation
   */
  def getAdditionalTables: Map[String, AdditionalTableInfo] = additionalTables

  /**
   * Return a new context with a different DataFrame, preserving everything else.
   */
  def withData(newData: DataFrame): TransformationContext =
    copy(currentData = newData)
}

object TransformationContext {
  /**
   * Create a new transformation context
   */
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
    spark = spark,
    additionalTables = Map.empty
  )
}

/**
 * Metadata for integrating additional tables into DAG
 */
case class AdditionalTableMetadata(
  primaryKey: Seq[String],
  joinKeys: Map[String, Seq[String]] = Map.empty,
  description: Option[String] = None,
  partitionBy: Seq[String] = Seq.empty
)

/**
 * Information about an additional table created during transformations
 */
case class AdditionalTableInfo(
  tableName: String,
  data: DataFrame,
  outputPath: Option[String],
  dagMetadata: Option[AdditionalTableMetadata]
)
