package com.etl.framework.util

/** Validation shared by logical names that Floe maps to both an Iceberg table and, for primary flows, an artifact-path
  * segment. SQL quoting makes spaces, reserved words, hyphens and backticks safe, but path separators and control
  * characters are not portable across catalogs or object-store/filesystem paths.
  */
object ManagedTableName {

  def containsUnsafeCharacters(name: String): Boolean =
    name != null && name.exists(character => character == '/' || character == '\\' || Character.isISOControl(character))

  val unsafeCharactersMessage: String =
    "must not contain path separators or control characters"
}
