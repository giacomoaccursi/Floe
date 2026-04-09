package com.etl.framework.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Immutable context available during derived table computation. Provides access to Iceberg tables (full history) and
  * the current batch ID.
  */
final case class DerivedTableContext(
    spark: SparkSession,
    batchId: String,
    catalogName: String,
    namespace: String = "default"
) {

  def table(name: String): DataFrame =
    spark.table(s"$catalogName.$namespace.$name")
}
