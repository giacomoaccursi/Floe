package com.etl.framework.pipeline

import com.etl.framework.util.SqlIdentifier
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.Instant

/** Immutable context available during derived table computation. Provides access to the current state of Iceberg tables
  * and the current batch ID.
  */
final case class DerivedTableContext(
    spark: SparkSession,
    logicalRunId: String,
    attemptId: String,
    effectiveAt: Instant,
    catalogName: String,
    namespace: String = "default"
) {

  def batchId: String = attemptId

  def table(name: String): DataFrame =
    spark.table(Seq(catalogName, namespace, name).map(SqlIdentifier.quote).mkString("."))
}
