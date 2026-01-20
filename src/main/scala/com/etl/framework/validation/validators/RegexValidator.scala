package com.etl.framework.validation.validators

import com.etl.framework.config.ValidationRule
import com.etl.framework.validation.Validator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Validator for regex pattern matching
 */
class RegexValidator extends Validator {
  
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val column = rule.column.getOrElse(
      throw new ValidationException("Column not specified for regex validation")
    )
    
    val pattern = rule.pattern.getOrElse(
      throw new ValidationException("Pattern not specified for regex validation")
    )
    
    val skipNull = rule.skipNull.getOrElse(true)
    
    // Build condition for regex matching
    val matchCondition = if (skipNull) {
      col(column).isNull || col(column).rlike(pattern)
    } else {
      col(column).rlike(pattern)
    }
    
    val validDf = df.filter(matchCondition)
    val invalidDf = df.filter(!matchCondition)
    
    if (invalidDf.isEmpty) {
      ValidationStepResult(validDf, None)
    } else {
      val rejectedDf = invalidDf
        .withColumn("_rejection_code", lit("REGEX_VALIDATION_FAILED"))
        .withColumn("_rejection_reason", 
          lit(s"Value in column '$column' does not match pattern: $pattern"))
        .withColumn("_validation_step", lit("regex_validation"))
        .withColumn("_rejected_at", current_timestamp())
      
      ValidationStepResult(validDf, Some(rejectedDf))
    }
  }
}

/**
 * Exception thrown when validation configuration is invalid
 */
class ValidationException(message: String) extends RuntimeException(message)
