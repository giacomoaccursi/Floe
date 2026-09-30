package com.etl.framework.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.Instant

/** One derived target and its declared inputs. */
final case class DerivedTableDefinition(
    name: String,
    dependencies: Seq[String],
    transform: DerivedTableContext => DataFrame
) {
  require(name != null && name.trim.nonEmpty, "Derived table name must not be blank")
  require(!name.contains("."), "Derived table name must be unqualified; configure catalog and namespace globally")
  require(dependencies != null, "Derived table dependencies must not be null")
  require(
    dependencies.forall(dependency => dependency != null && dependency.trim.nonEmpty && !dependency.contains(".")),
    "Derived table dependencies must be non-blank, unqualified names"
  )
  require(transform != null, "Derived table transform must not be null")
}

/** Immutable context available during derived table computation. `table` exposes only declared dependencies resolved to
  * the state produced by this attempt; it never falls back to catalog HEAD.
  */
final case class DerivedTableContext(
    spark: SparkSession,
    logicalRunId: String,
    attemptId: String,
    effectiveAt: Instant,
    private val resolvedInputs: Map[String, DataFrame]
) {

  def batchId: String = attemptId

  def table(name: String): DataFrame =
    resolvedInputs.getOrElse(
      name,
      throw new IllegalArgumentException(
        s"Derived input '$name' is not declared or was not produced successfully. " +
          s"Available inputs: ${resolvedInputs.keys.toSeq.sorted.mkString(", ")}"
      )
    )

  def availableTables: Set[String] = resolvedInputs.keySet
}
