package com.etl.framework.validation.validators

import com.etl.framework.config.ValidationRule
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

/**
 * Validator for regex pattern matching
 */
class RegexValidator(flowName: Option[String] = None) extends BaseValidator(flowName) {
  
  override protected def validatorName: String = "Regex"
  
  override protected def rejectionCode: String = "REGEX_VALIDATION_FAILED"
  
  override protected def validationStep: String = "regex_validation"
  
  override protected def rejectionReason(rule: ValidationRule, column: String): String = {
    val pattern = rule.pattern.getOrElse("unknown")
    s"Value in column '$column' does not match pattern: $pattern"
  }
  
  override protected def buildValidationCondition(
    df: DataFrame, 
    rule: ValidationRule, 
    column: String
  ): Column = {
    val pattern = rule.pattern.getOrElse {
      throw new ValidationException(
        s"Regex validation error for column '$column'${flowContext}: 'pattern' field is required"
      )
    }
    
    // Validate pattern syntax
    try {
      pattern.r
    } catch {
      case e: Exception =>
        throw new ValidationException(
          s"Invalid regex pattern for column '$column'${flowContext}: '$pattern' - ${e.getMessage}",
          e
        )
    }
    
    col(column).rlike(pattern)
  }
}
