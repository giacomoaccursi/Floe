package com.etl.framework.util

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

/** Resolves a configured name as one literal Spark column.
  *
  * Spark parses dots in ordinary `col(name)` and `dataFrame(name)` calls as nested-field separators. Quoting the full
  * name preserves literal dots, backticks, spaces, and reserved words while the DataFrame overload also retains the
  * relation qualifier needed by joins.
  */
object SparkColumn {

  def apply(name: String): Column = col(SqlIdentifier.quote(name))

  def apply(dataFrame: DataFrame, name: String): Column = dataFrame.col(SqlIdentifier.quote(name))
}
