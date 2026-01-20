package com.etl.framework.exceptions

/**
 * Base exception for all framework errors
 */
abstract class FrameworkException(
  message: String,
  cause: Throwable = null
) extends RuntimeException(message, cause) {
  
  /**
   * Error code for categorization and monitoring
   */
  def errorCode: String
  
  /**
   * Additional context for debugging
   */
  def context: Map[String, Any] = Map.empty
  
  /**
   * Formatted error message with context
   */
  override def getMessage: String = {
    val baseMessage = super.getMessage
    if (context.isEmpty) {
      s"[$errorCode] $baseMessage"
    } else {
      val contextStr = context.map { case (k, v) => s"$k=$v" }.mkString(", ")
      s"[$errorCode] $baseMessage (Context: $contextStr)"
    }
  }
}

/**
 * Configuration-related errors
 */
sealed abstract class ConfigurationException(
  message: String,
  cause: Throwable = null
) extends FrameworkException(message, cause)

/**
 * YAML syntax error in configuration file
 */
case class YAMLSyntaxException(
  file: String,
  line: Option[Int] = None,
  column: Option[Int] = None,
  details: String,
  cause: Throwable = null
) extends ConfigurationException(
  s"YAML syntax error in file '$file'${line.map(l => s" at line $l").getOrElse("")}${column.map(c => s", column $c").getOrElse("")}: $details",
  cause
) {
  override def errorCode: String = "CONFIG_YAML_SYNTAX"
  override def context: Map[String, Any] = Map(
    "file" -> file,
    "line" -> line.getOrElse("unknown"),
    "column" -> column.getOrElse("unknown"),
    "details" -> details
  )
}

/**
 * Missing required field in configuration
 */
case class MissingConfigFieldException(
  file: String,
  field: String,
  section: String
) extends ConfigurationException(
  s"Missing required field '$field' in section '$section' of configuration file '$file'"
) {
  override def errorCode: String = "CONFIG_MISSING_FIELD"
  override def context: Map[String, Any] = Map(
    "file" -> file,
    "field" -> field,
    "section" -> section
  )
}

/**
 * Invalid type in configuration
 */
case class InvalidConfigTypeException(
  file: String,
  field: String,
  expectedType: String,
  actualType: String
) extends ConfigurationException(
  s"Invalid type for field '$field' in configuration file '$file': expected $expectedType, got $actualType"
) {
  override def errorCode: String = "CONFIG_INVALID_TYPE"
  override def context: Map[String, Any] = Map(
    "file" -> file,
    "field" -> field,
    "expectedType" -> expectedType,
    "actualType" -> actualType
  )
}

/**
 * Reference to non-existent entity in configuration
 */
case class InvalidReferenceException(
  file: String,
  referenceType: String,
  referenceName: String,
  availableReferences: Seq[String] = Seq.empty
) extends ConfigurationException(
  s"Invalid reference to $referenceType '$referenceName' in configuration file '$file'${
    if (availableReferences.nonEmpty) s". Available: ${availableReferences.mkString(", ")}" else ""
  }"
) {
  override def errorCode: String = "CONFIG_INVALID_REFERENCE"
  override def context: Map[String, Any] = Map(
    "file" -> file,
    "referenceType" -> referenceType,
    "referenceName" -> referenceName,
    "availableReferences" -> availableReferences.mkString(", ")
  )
}

/**
 * Circular dependency detected in flow or DAG graph
 */
case class CircularDependencyException(
  graphType: String,
  cycle: Seq[String]
) extends ConfigurationException(
  s"Circular dependency detected in $graphType: ${cycle.mkString(" -> ")}"
) {
  override def errorCode: String = "CONFIG_CIRCULAR_DEPENDENCY"
  override def context: Map[String, Any] = Map(
    "graphType" -> graphType,
    "cycle" -> cycle.mkString(" -> ")
  )
}

/**
 * Validation-related errors
 */
sealed abstract class ValidationException(
  message: String,
  cause: Throwable = null
) extends FrameworkException(message, cause)

/**
 * Schema validation failure
 */
case class SchemaValidationException(
  flowName: String,
  missingColumns: Seq[String] = Seq.empty,
  typeMismatches: Map[String, (String, String)] = Map.empty
) extends ValidationException(
  s"Schema validation failed for flow '$flowName'${
    if (missingColumns.nonEmpty) s". Missing columns: ${missingColumns.mkString(", ")}" else ""
  }${
    if (typeMismatches.nonEmpty) {
      val mismatches = typeMismatches.map { case (col, (expected, actual)) =>
        s"$col (expected: $expected, got: $actual)"
      }.mkString(", ")
      s". Type mismatches: $mismatches"
    } else ""
  }"
) {
  override def errorCode: String = "VALIDATION_SCHEMA"
  override def context: Map[String, Any] = Map(
    "flowName" -> flowName,
    "missingColumns" -> missingColumns.mkString(", "),
    "typeMismatches" -> typeMismatches.toString
  )
}

/**
 * Primary key validation failure
 */
case class PrimaryKeyViolationException(
  flowName: String,
  duplicateCount: Long,
  keyColumns: Seq[String]
) extends ValidationException(
  s"Primary key violation in flow '$flowName': found $duplicateCount duplicate records on key columns [${keyColumns.mkString(", ")}]"
) {
  override def errorCode: String = "VALIDATION_PK_VIOLATION"
  override def context: Map[String, Any] = Map(
    "flowName" -> flowName,
    "duplicateCount" -> duplicateCount,
    "keyColumns" -> keyColumns.mkString(", ")
  )
}

/**
 * Foreign key validation failure
 */
case class ForeignKeyViolationException(
  flowName: String,
  orphanCount: Long,
  foreignKeyName: String,
  referencedFlow: String
) extends ValidationException(
  s"Foreign key violation in flow '$flowName': found $orphanCount orphan records for FK '$foreignKeyName' referencing flow '$referencedFlow'"
) {
  override def errorCode: String = "VALIDATION_FK_VIOLATION"
  override def context: Map[String, Any] = Map(
    "flowName" -> flowName,
    "orphanCount" -> orphanCount,
    "foreignKeyName" -> foreignKeyName,
    "referencedFlow" -> referencedFlow
  )
}

/**
 * Maximum rejection rate exceeded
 */
case class MaxRejectionRateExceededException(
  flowName: String,
  actualRate: Double,
  maxRate: Double,
  rejectedCount: Long,
  totalCount: Long
) extends ValidationException(
  f"Maximum rejection rate exceeded for flow '$flowName': ${actualRate * 100}%.2f%% (${rejectedCount}/${totalCount}) exceeds configured maximum of ${maxRate * 100}%.2f%%"
) {
  override def errorCode: String = "VALIDATION_MAX_REJECTION_RATE"
  override def context: Map[String, Any] = Map(
    "flowName" -> flowName,
    "actualRate" -> actualRate,
    "maxRate" -> maxRate,
    "rejectedCount" -> rejectedCount,
    "totalCount" -> totalCount
  )
}

/**
 * Data processing errors
 */
sealed abstract class DataProcessingException(
  message: String,
  cause: Throwable = null
) extends FrameworkException(message, cause)

/**
 * Record count invariant violation
 */
case class InvariantViolationException(
  flowName: String,
  inputCount: Long,
  validCount: Long,
  rejectedCount: Long
) extends DataProcessingException(
  s"Record count invariant violated for flow '$flowName': input ($inputCount) != valid ($validCount) + rejected ($rejectedCount). Difference: ${inputCount - (validCount + rejectedCount)}"
) {
  override def errorCode: String = "DATA_INVARIANT_VIOLATION"
  override def context: Map[String, Any] = Map(
    "flowName" -> flowName,
    "inputCount" -> inputCount,
    "validCount" -> validCount,
    "rejectedCount" -> rejectedCount,
    "difference" -> (inputCount - (validCount + rejectedCount))
  )
}

/**
 * Data source unavailable or unreadable
 */
case class DataSourceException(
  sourceType: String,
  sourcePath: String,
  details: String,
  cause: Throwable = null
) extends DataProcessingException(
  s"Failed to read from $sourceType source '$sourcePath': $details",
  cause
) {
  override def errorCode: String = "DATA_SOURCE_ERROR"
  override def context: Map[String, Any] = Map(
    "sourceType" -> sourceType,
    "sourcePath" -> sourcePath,
    "details" -> details
  )
}

/**
 * Failed to write output data
 */
case class DataWriteException(
  outputType: String,
  outputPath: String,
  details: String,
  cause: Throwable = null
) extends DataProcessingException(
  s"Failed to write $outputType to '$outputPath': $details",
  cause
) {
  override def errorCode: String = "DATA_WRITE_ERROR"
  override def context: Map[String, Any] = Map(
    "outputType" -> outputType,
    "outputPath" -> outputPath,
    "details" -> details
  )
}

/**
 * Merge operation failure
 */
case class MergeException(
  flowName: String,
  mergeStrategy: String,
  details: String,
  cause: Throwable = null
) extends DataProcessingException(
  s"Merge operation failed for flow '$flowName' with strategy '$mergeStrategy': $details",
  cause
) {
  override def errorCode: String = "DATA_MERGE_ERROR"
  override def context: Map[String, Any] = Map(
    "flowName" -> flowName,
    "mergeStrategy" -> mergeStrategy,
    "details" -> details
  )
}

/**
 * Transformation errors
 */
sealed abstract class TransformationException(
  message: String,
  cause: Throwable = null
) extends FrameworkException(message, cause)

/**
 * Pre-validation transformation failure
 */
case class PreValidationTransformationException(
  flowName: String,
  details: String,
  cause: Throwable = null
) extends TransformationException(
  s"Pre-validation transformation failed for flow '$flowName': $details",
  cause
) {
  override def errorCode: String = "TRANSFORM_PRE_VALIDATION"
  override def context: Map[String, Any] = Map(
    "flowName" -> flowName,
    "details" -> details
  )
}

/**
 * Post-validation transformation failure
 */
case class PostValidationTransformationException(
  flowName: String,
  details: String,
  cause: Throwable = null
) extends TransformationException(
  s"Post-validation transformation failed for flow '$flowName': $details",
  cause
) {
  override def errorCode: String = "TRANSFORM_POST_VALIDATION"
  override def context: Map[String, Any] = Map(
    "flowName" -> flowName,
    "details" -> details
  )
}

/**
 * DAG/Aggregation errors
 */
sealed abstract class AggregationException(
  message: String,
  cause: Throwable = null
) extends FrameworkException(message, cause)

/**
 * DAG node execution failure
 */
case class DAGNodeExecutionException(
  nodeId: String,
  details: String,
  cause: Throwable = null
) extends AggregationException(
  s"DAG node execution failed for node '$nodeId': $details",
  cause
) {
  override def errorCode: String = "DAG_NODE_EXECUTION"
  override def context: Map[String, Any] = Map(
    "nodeId" -> nodeId,
    "details" -> details
  )
}

/**
 * Join operation failure
 */
case class JoinException(
  parentNode: String,
  childNode: String,
  joinStrategy: String,
  details: String,
  cause: Throwable = null
) extends AggregationException(
  s"Join operation failed between parent '$parentNode' and child '$childNode' with strategy '$joinStrategy': $details",
  cause
) {
  override def errorCode: String = "DAG_JOIN_ERROR"
  override def context: Map[String, Any] = Map(
    "parentNode" -> parentNode,
    "childNode" -> childNode,
    "joinStrategy" -> joinStrategy,
    "details" -> details
  )
}

/**
 * Mapping errors
 */
sealed abstract class MappingException(
  message: String,
  cause: Throwable = null
) extends FrameworkException(message, cause)

/**
 * DataFrame to Dataset mapping failure
 */
case class DataFrameToDatasetMappingException(
  targetType: String,
  details: String,
  cause: Throwable = null
) extends MappingException(
  s"Failed to map DataFrame to Dataset[$targetType]: $details",
  cause
) {
  override def errorCode: String = "MAPPING_DF_TO_DS"
  override def context: Map[String, Any] = Map(
    "targetType" -> targetType,
    "details" -> details
  )
}

/**
 * Batch to Final model mapping failure
 */
case class ModelMappingException(
  batchModelType: String,
  finalModelType: String,
  details: String,
  cause: Throwable = null
) extends MappingException(
  s"Failed to map BatchModel[$batchModelType] to FinalModel[$finalModelType]: $details",
  cause
) {
  override def errorCode: String = "MAPPING_BATCH_TO_FINAL"
  override def context: Map[String, Any] = Map(
    "batchModelType" -> batchModelType,
    "finalModelType" -> finalModelType,
    "details" -> details
  )
}

/**
 * Plugin/Extension errors
 */
sealed abstract class PluginException(
  message: String,
  cause: Throwable = null
) extends FrameworkException(message, cause)

/**
 * Custom validator loading failure
 */
case class CustomValidatorLoadException(
  className: String,
  details: String,
  cause: Throwable = null
) extends PluginException(
  s"Failed to load custom validator '$className': $details",
  cause
) {
  override def errorCode: String = "PLUGIN_VALIDATOR_LOAD"
  override def context: Map[String, Any] = Map(
    "className" -> className,
    "details" -> details
  )
}

/**
 * Custom validator execution failure
 */
case class CustomValidatorExecutionException(
  className: String,
  details: String,
  cause: Throwable = null
) extends PluginException(
  s"Custom validator '$className' execution failed: $details",
  cause
) {
  override def errorCode: String = "PLUGIN_VALIDATOR_EXECUTION"
  override def context: Map[String, Any] = Map(
    "className" -> className,
    "details" -> details
  )
}

/**
 * Orchestration errors
 */
sealed abstract class OrchestrationException(
  message: String,
  cause: Throwable = null
) extends FrameworkException(message, cause)

/**
 * Flow execution failure
 */
case class FlowExecutionException(
  flowName: String,
  details: String,
  cause: Throwable = null
) extends OrchestrationException(
  s"Flow execution failed for '$flowName': $details",
  cause
) {
  override def errorCode: String = "ORCHESTRATION_FLOW_EXECUTION"
  override def context: Map[String, Any] = Map(
    "flowName" -> flowName,
    "details" -> details
  )
}

/**
 * Rollback operation failure
 */
case class RollbackException(
  batchId: String,
  completedFlows: Seq[String],
  details: String,
  cause: Throwable = null
) extends OrchestrationException(
  s"Rollback failed for batch '$batchId' (completed flows: ${completedFlows.mkString(", ")}): $details",
  cause
) {
  override def errorCode: String = "ORCHESTRATION_ROLLBACK"
  override def context: Map[String, Any] = Map(
    "batchId" -> batchId,
    "completedFlows" -> completedFlows.mkString(", "),
    "details" -> details
  )
}

/**
 * Unsupported operation
 */
case class UnsupportedOperationException(
  operation: String,
  details: String
) extends FrameworkException(
  s"Unsupported operation '$operation': $details"
) {
  override def errorCode: String = "UNSUPPORTED_OPERATION"
  override def context: Map[String, Any] = Map(
    "operation" -> operation,
    "details" -> details
  )
}
