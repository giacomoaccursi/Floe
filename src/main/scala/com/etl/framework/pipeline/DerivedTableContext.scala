package com.etl.framework.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Immutable context available during derived table computation.
  * Provides access to Iceberg tables (full history) and the current batch ID.
  */
final case class DerivedTableContext(
    spark: SparkSession,
    batchId: String,
    catalogName: String
) {

  /** Reads a table from the Iceberg catalog (full history, not just the current batch delta).
    */
  def table(name: String): DataFrame =
    spark.table(s"$catalogName.default.$name")
}
