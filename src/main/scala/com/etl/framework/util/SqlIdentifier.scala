package com.etl.framework.util

/** Centralized Spark SQL rendering for identifiers and string literals.
  *
  * Identifiers and values deliberately use separate methods: backticks quote identifiers, while apostrophes delimit
  * SQL string literals. Keeping the two paths separate prevents valid names (spaces, reserved words, backticks) from
  * producing invalid SQL and avoids treating configured values as SQL fragments.
  */
object SqlIdentifier {

  def quote(identifier: String): String = {
    require(identifier != null && identifier.nonEmpty, "SQL identifier must not be empty")
    s"`${identifier.replace("`", "``")}`"
  }

  def quoteMultipart(identifier: String): String = {
    require(identifier != null && identifier.nonEmpty, "SQL identifier must not be empty")
    identifier.split("\\.", -1).map(quote).mkString(".")
  }

  def qualified(relationAlias: String, column: String): String =
    s"${quote(relationAlias)}.${quote(column)}"

  def metadataTable(tableName: String, metadataName: String): String =
    s"${quoteMultipart(tableName)}.${quote(metadataName)}"

  def stringLiteral(value: String): String =
    s"'${Option(value).getOrElse("").replace("'", "''")}'"
}
