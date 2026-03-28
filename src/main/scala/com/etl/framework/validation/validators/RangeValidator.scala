package com.etl.framework.validation.validators

import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.config.ValidationRule
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DoubleType

/** Validator for range validation (numeric and date types)
  */
class RangeValidator(flowName: Option[String] = None) extends BaseValidator(flowName) {

  override protected def validatorName: String = "Range"

  override protected def rejectionCode: String = "RANGE_VALIDATION_FAILED"

  override protected def validationStep: String = "range_validation"

  override protected def rejectionReason(rule: ValidationRule, column: String): String = {
    val rangeDesc = (rule.min, rule.max) match {
      case (Some(min), Some(max)) => s"between $min and $max"
      case (Some(min), None)      => s">= $min"
      case (None, Some(max))      => s"<= $max"
      case _                      => "valid range"
    }
    s"Value in column '$column' is not $rangeDesc"
  }

  override protected def buildValidationCondition(
      df: DataFrame,
      rule: ValidationRule,
      column: String
  ): Column = {
    // Validate that at least one of min or max is specified
    if (rule.min.isEmpty && rule.max.isEmpty) {
      throw ValidationConfigException(
        s"Range validation error for column '$column'${flowContext}: at least one of 'min' or 'max' is required"
      )
    }

    // Validate that min <= max
    (rule.min, rule.max) match {
      case (Some(min), Some(max)) =>
        if (min.toDouble > max.toDouble) {
          throw ValidationConfigException(
            s"Range validation error for column '$column'${flowContext}: 'min' ($min) > 'max' ($max)"
          )
        }
      case _ => // OK
    }

    // Build conditions using numeric literals and a DoubleType cast on the column.
    // This ensures numeric comparison regardless of the column's declared type:
    // string columns containing numbers are cast correctly, and lexicographic
    // ordering (e.g. "9" > "10") is avoided.
    val minCondition = rule.min
      .map { minValue =>
        validateNumericValue(minValue, "min", column)
        col(column).cast(DoubleType) >= lit(minValue.toDouble)
      }
      .getOrElse(lit(true))

    val maxCondition = rule.max
      .map { maxValue =>
        validateNumericValue(maxValue, "max", column)
        col(column).cast(DoubleType) <= lit(maxValue.toDouble)
      }
      .getOrElse(lit(true))

    minCondition && maxCondition
  }

  /** Validates that a value is numeric
    */
  private def validateNumericValue(value: String, fieldName: String, column: String): Unit = {
    try {
      value.toDouble
    } catch {
      case e: NumberFormatException =>
        throw ValidationConfigException(
          s"Range validation error for column '$column'${flowContext}: '$fieldName' value '$value' is not numeric",
          e
        )
    }
  }
}
