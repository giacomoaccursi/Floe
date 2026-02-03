package com.etl.framework.validation.validators

import com.etl.framework.config.{FlowConfig, ValidationRule}
import com.etl.framework.validation.{ValidationStepResult, ValidationUtils}
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
      ValidationUtils.validResult(df)
    } else {
      // Build condition for null checks
      val nullCondition = notNullColumns
        .map(col => s"`$col` IS NULL")
        .mkString(" OR ")
      
      val rejectedDf = df.filter(nullCondition)
      val validDf = df.filter(s"NOT ($nullCondition)")
      
      if (rejectedDf.isEmpty) {
        ValidationUtils.validResult(validDf)
      } else {
        ValidationUtils.resultWithRejections(
          validDf,
          rejectedDf,
          "NOT_NULL_VIOLATION",
          s"Null value in non-nullable column(s) in flow $flowName: ${notNullColumns.mkString(", ")}",
          "not_null_validation"
        )
      }
    }
  }
}
