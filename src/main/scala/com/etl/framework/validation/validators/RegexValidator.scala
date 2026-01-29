package com.etl.framework.validation.validators

import com.etl.framework.config.ValidationRule
import com.etl.framework.validation.{ValidationStepResult, Validator}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Validator for regex pattern matching
 */
class RegexValidator(flowName: Option[String] = None) extends Validator {
  
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val flowContext = flowName.map(f => s" in flow '$f'").getOrElse("")
    
    val column = rule.column.getOrElse {
      throw new ValidationException(
        s"Regex validation configuration error$flowContext: 'column' field is required.\n"
      )
    }
    
    val pattern = rule.pattern.getOrElse {
      throw new ValidationException(
        s"Regex validation configuration error for column '$column'$flowContext: 'pattern' field is required.\n"
      )
    }
    
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
