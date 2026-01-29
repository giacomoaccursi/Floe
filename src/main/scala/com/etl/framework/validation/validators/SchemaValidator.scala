package com.etl.framework.validation.validators

import com.etl.framework.config.{FlowConfig, ValidationRule}
import com.etl.framework.validation.ValidationStepResult
import org.apache.spark.sql.DataFrame

/**
 * Validator for schema validation
 * Validates that all required columns are present
 */
class SchemaValidator(flowConfig: FlowConfig, flowName: Option[String] = None) 
  extends FlowConfigValidator(flowConfig, flowName) {
  
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    if (!flowConfig.schema.enforceSchema) {
      return ValidationStepResult(df, None)
    }
    
    val requiredColumns = flowConfig.schema.columns.map(_.name).toSet
    val actualColumns = df.columns.toSet
    
    // Check for missing required columns
    val missingColumns = requiredColumns -- actualColumns
    if (missingColumns.nonEmpty) {
      val rejectedDf = addRejectionMetadata(
        df,
        "SCHEMA_VALIDATION_FAILED",
        s"Missing required columns$flowContext: ${missingColumns.mkString(", ")}",
        "schema_validation"
      )
      
      return ValidationStepResult(df.limit(0), Some(rejectedDf))
    }
    
    // TODO: Add type validation
    ValidationStepResult(df, None)
  }
}
