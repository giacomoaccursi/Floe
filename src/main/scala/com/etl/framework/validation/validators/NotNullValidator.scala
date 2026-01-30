package com.etl.framework.validation.validators

import com.etl.framework.config.{FlowConfig, ValidationRule}
import com.etl.framework.validation.ValidationStepResult
import org.apache.spark.sql.DataFrame

/**
 * Validator for not-null constraints
 * Validates that non-nullable columns don't contain null values
 */
class NotNullValidator(flowConfig: FlowConfig, flowName: Option[String] = None) 
  extends FlowConfigValidator(flowConfig, flowName) {
  
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val notNullColumns = flowConfig.schema.columns
      .filter(!_.nullable)
      .map(_.name)
    
    if (notNullColumns.isEmpty) {
      return ValidationStepResult(df, None)
    }
    
    // Build condition for null checks
    val nullCondition = notNullColumns
      .map(col => s"`$col` IS NULL")
      .mkString(" OR ")
    
    val rejectedDf = df.filter(nullCondition)
    val validDf = df.filter(s"NOT ($nullCondition)")
    
    if (rejectedDf.isEmpty) {
      ValidationStepResult(validDf, None)
    } else {
      val rejectedWithMetadata = addRejectionMetadata(
        rejectedDf,
        "NOT_NULL_VIOLATION",
        s"Null value in non-nullable column(s) in flow $flowName: ${notNullColumns.mkString(", ")}",
        "not_null_validation"
      )
      
      ValidationStepResult(validDf, Some(rejectedWithMetadata))
    }
  }
}
