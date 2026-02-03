package com.etl.framework.core

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
 * Context available during transformations (both pre- and post-validation)
 */
case class TransformationContext(
                                  currentFlow: String,                           // Current flow name
                                  currentData: DataFrame,                        // Current flow data
                                  validatedFlows: Map[String, DataFrame],        // All already validated flows
                                  batchId: String,                               // Current batch ID
                                  spark: SparkSession,                           // Spark session
                                  private val additionalTablesMap: mutable.Map[String, AdditionalTableInfo] = mutable.Map.empty
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
    additionalTablesMap(tableName) = AdditionalTableInfo(
      tableName = tableName,
      data = data,
      outputPath = outputPath,
      dagMetadata = dagMetadata
    )
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
