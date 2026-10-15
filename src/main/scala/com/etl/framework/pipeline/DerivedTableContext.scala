package com.etl.framework.pipeline

import com.etl.framework.util.ManagedTableName
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.Instant

/** One derived target and its declared inputs. */
final case class DerivedTableDefinition(
    name: String,
    dependencies: Seq[String],
    transform: DerivedTableContext => DataFrame
) {
  require(name != null && name.trim.nonEmpty, "Derived table name must not be blank")
  require(name == name.trim, "Derived table name must not have leading or trailing whitespace")
  require(!name.contains("."), "Derived table name must be unqualified; configure catalog and namespace globally")
  require(
    !ManagedTableName.containsUnsafeCharacters(name),
    s"Derived table name ${ManagedTableName.unsafeCharactersMessage}"
  )
  require(dependencies != null, "Derived table dependencies must not be null")
  require(
    dependencies.forall(dependency => dependency != null && dependency.trim.nonEmpty),
    "Derived table dependencies must be non-blank names"
  )
  require(
    dependencies.forall(dependency => dependency == dependency.trim),
    "Derived table dependencies must not have leading or trailing whitespace"
  )
  require(
    dependencies.forall(dependency => !dependency.contains(".")),
    "Derived table dependencies must be unqualified names"
  )
  require(
    dependencies.forall(dependency => !ManagedTableName.containsUnsafeCharacters(dependency)),
    s"Derived table dependencies ${ManagedTableName.unsafeCharactersMessage}"
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
