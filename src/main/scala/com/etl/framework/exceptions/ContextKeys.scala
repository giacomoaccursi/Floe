package com.etl.framework.exceptions

/** Standard keys used in exception context maps
  */
/** Standard context keys used in FrameworkException for structured error reporting. */
object ContextKeys {
  // File and location information
  val FILE = "file"
  val LINE = "line"
  val COLUMN = "column"
  val FIELD = "field"
  val SECTION = "section"

  // Type information
  val EXPECTED_TYPE = "expectedType"
  val ACTUAL_TYPE = "actualType"

  // Reference information
  val REFERENCE_TYPE = "referenceType"
  val REFERENCE_NAME = "referenceName"

  // General information
  val MESSAGE = "message"
  val AVAILABLE_REFERENCES = "availableReferences"

  // Flow and validation
  val FLOW_NAME = "flowName"
  val MISSING_COLUMNS = "missingColumns"
  val TYPE_MISMATCHES = "typeMismatches"
  val KEY_COLUMNS = "keyColumns"

  // Primary key validation
  val DUPLICATE_COUNT = "duplicateCount"

  // Foreign key validation
  val ORPHAN_COUNT = "orphanCount"
  val FOREIGN_KEY_NAME = "foreignKeyName"
  val REFERENCED_FLOW = "referencedFlow"

  // Rejection rate validation
  val ACTUAL_RATE = "actualRate"
  val MAX_RATE = "maxRate"
  val REJECTED_COUNT = "rejectedCount"
  val TOTAL_COUNT = "totalCount"

  // Data invariant
  val INPUT_COUNT = "inputCount"
  val VALID_COUNT = "validCount"
  val DIFFERENCE = "difference"

  // Data source and output
  val SOURCE_TYPE = "sourceType"
  val SOURCE_PATH = "sourcePath"
  val OUTPUT_TYPE = "outputType"
  val OUTPUT_PATH = "outputPath"

  // Merge operations
  val MERGE_STRATEGY = "mergeStrategy"

  // DAG operations
  val GRAPH_TYPE = "graphType"
  val CYCLE = "cycle"
  val PARENT_NODE = "parentNode"
  val CHILD_NODE = "childNode"
  val JOIN_STRATEGY = "joinStrategy"

  // General
  val DETAILS = "details"
}
