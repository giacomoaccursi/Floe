package com.etl.framework.validation.validators

import com.etl.framework.config.{FlowConfig, ValidationRule}
import com.etl.framework.validation.{ValidationStepResult, ValidationUtils}
import org.apache.spark.sql.DataFrame

/** Validator for schema validation Validates that all required columns are present
  */
class SchemaValidator(flowConfig: FlowConfig, flowName: Option[String] = None)
    extends FlowConfigValidator(flowConfig, flowName) {

  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    if (!flowConfig.schema.enforceSchema) {
      ValidationUtils.validResult(df)
    } else {
      val requiredColumns = flowConfig.schema.columns.map(_.name).toSet
      val actualColumns = df.columns.toSet

      // 1. Check for missing required columns
      val missingColumns = requiredColumns -- actualColumns
      if (missingColumns.nonEmpty) {
        ValidationUtils.resultWithRejections(
          df.limit(0),
          df,
          "SCHEMA_VALIDATION_FAILED",
          s"Missing required columns in flow $flowName: ${missingColumns.mkString(", ")}",
          "schema_validation"
        )
      } else if (!flowConfig.schema.allowExtraColumns) {
        // 2. Check for extra columns (if not allowed)
        // Exclude internal validation columns (prefixed with _)
        val extraColumns = (actualColumns -- requiredColumns).filterNot(_.startsWith("_"))
        if (extraColumns.nonEmpty) {
          ValidationUtils.resultWithRejections(
            df.limit(0),
            df,
            "SCHEMA_EXTRA_COLUMNS",
            s"Unexpected extra columns in flow $flowName: ${extraColumns.mkString(", ")}",
            "schema_validation"
          )
        } else {
          ValidationUtils.validResult(df)
        }
      } else {
        ValidationUtils.validResult(df)
      }
    }
  }
}
