package com.etl.framework.validation.validators

import com.etl.framework.config.{FlowConfig, ValidationRule}
import com.etl.framework.validation.{ValidationStepResult, ValidationUtils}
import org.apache.spark.sql.DataFrame

/**
 * Validator for schema validation
 * Validates that all required columns are present
 */
class SchemaValidator(flowConfig: FlowConfig, flowName: Option[String] = None) 
  extends FlowConfigValidator(flowConfig, flowName) {
  
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    if (!flowConfig.schema.enforceSchema) {
      return ValidationUtils.validResult(df)
    }
    
    val requiredColumns = flowConfig.schema.columns.map(_.name).toSet
    val actualColumns = df.columns.toSet
    
    // Check for missing required columns
    val missingColumns = requiredColumns -- actualColumns
    if (missingColumns.nonEmpty) {
      return ValidationUtils.resultWithRejections(
        df.limit(0),
        df,
        "SCHEMA_VALIDATION_FAILED",
        s"Missing required columns in flow $flowName: ${missingColumns.mkString(", ")}",
        "schema_validation"
      )
    }
    
    // TODO: Add type validation
    ValidationUtils.validResult(df)
  }
}
