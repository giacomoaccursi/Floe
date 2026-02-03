package com.etl.framework.core

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Context available during transformations (both pre- and post-validation)
 */
case class TransformationContext(
                                  currentFlow: String,                           // Current flow name
                                  currentData: DataFrame,                        // Current flow data
                                  validatedFlows: Map[String, DataFrame],        // All already validated flows
                                  batchId: String,                               // Current batch ID
                                  spark: SparkSession                            // Spark session
                                ) {

  /**
   * Get DataFrame of another already validated flow
   */
  def getFlow(flowName: String): Option[DataFrame] = {
    validatedFlows.get(flowName)
  }

  /**
   * Add a new table that will be automatically saved
   * and made available for Transformation (DAG aggregation)
   */
  def addTable(
                tableName: String,
                data: DataFrame,
                outputPath: Option[String] = None,
                dagMetadata: Option[AdditionalTableMetadata] = None
              ): Unit = {
    // Table will be automatically saved by framework
    // and made available for Transformation if dagMetadata is specified
    // Implementation will be handled by FlowExecutor
  }
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
