package com.etl.framework.pipeline

import com.etl.framework.util.SqlIdentifier
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Immutable context available during derived table computation. Provides access to the current state of Iceberg tables
  * and the current batch ID.
  */
final case class DerivedTableContext(
    spark: SparkSession,
    batchId: String,
    catalogName: String,
    namespace: String = "default"
) {

  def table(name: String): DataFrame =
    spark.table(Seq(catalogName, namespace, name).map(SqlIdentifier.quote).mkString("."))
}
