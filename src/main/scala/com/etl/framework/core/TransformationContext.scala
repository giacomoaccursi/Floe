package com.etl.framework.core

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Context available during transformations (both pre- and post-validation)
 */
case class TransformationContext(
                                  currentFlow: String,
                                  currentData: DataFrame,
                                  validatedFlows: Map[String, DataFrame],
                                  batchId: String,
                                  spark: SparkSession
                                )
