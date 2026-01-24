package com.etl.framework.validation.validators

import com.etl.framework.config.ValidationRule
import com.etl.framework.validation.{ValidationStepResult, Validator}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Validator for range validation (numeric and date types)
 */
class RangeValidator extends Validator {
  
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val column = rule.column.getOrElse(
      throw new ValidationException("Column not specified for range validation")
    )
    
    val skipNull = rule.skipNull.getOrElse(true)
    
    // Build range condition
    var rangeCondition = lit(true)
    
    if (rule.min.isDefined) {
      val minValue = rule.min.get
      rangeCondition = rangeCondition && (col(column) >= lit(minValue))
    }
    
    if (rule.max.isDefined) {
      val maxValue = rule.max.get
      rangeCondition = rangeCondition && (col(column) <= lit(maxValue))
    }
    
    // Apply skipNull logic
    val finalCondition = if (skipNull) {
      col(column).isNull || rangeCondition
    } else {
      rangeCondition
    }
    
    val validDf = df.filter(finalCondition)
    val invalidDf = df.filter(!finalCondition)
    
    if (invalidDf.isEmpty) {
      ValidationStepResult(validDf, None)
    } else {
      val rangeDesc = (rule.min, rule.max) match {
        case (Some(min), Some(max)) => s"between $min and $max"
        case (Some(min), None) => s">= $min"
        case (None, Some(max)) => s"<= $max"
        case _ => "valid range"
      }
      
      val rejectedDf = invalidDf
        .withColumn("_rejection_code", lit("RANGE_VALIDATION_FAILED"))
        .withColumn("_rejection_reason", 
          lit(s"Value in column '$column' is not $rangeDesc"))
        .withColumn("_validation_step", lit("range_validation"))
        .withColumn("_rejected_at", current_timestamp())
      
      ValidationStepResult(validDf, Some(rejectedDf))
    }
  }
}
