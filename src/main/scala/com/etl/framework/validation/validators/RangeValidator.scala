package com.etl.framework.validation.validators

import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.config.ValidationRule
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DecimalType, StringType}

/** Validator for numeric range validation.
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

    val parsedMin = rule.min.map(parseFiniteNumericValue(_, "min", column))
    val parsedMax = rule.max.map(parseFiniteNumericValue(_, "max", column))
    val parsedBounds = Seq(parsedMin, parsedMax).flatten

    // Validate that min <= max
    (parsedMin, parsedMax) match {
      case (Some(min), Some(max)) =>
        if (min > max) {
          throw ValidationConfigException(
            s"Range validation error for column '$column'${flowContext}: 'min' ($min) > 'max' ($max)"
          )
        }
      case _ => // OK
    }

    // Decimal literals preserve exact integral and fixed-point bounds. Spark otherwise
    // promotes String/Decimal comparisons to Double, so numeric strings are explicitly
    // cast to the widest Decimal that can still represent the configured bounds.
    val valueColumn = df.schema(column).dataType match {
      case StringType => col(column).cast(decimalTypeFor(parsedBounds))
      case _          => col(column)
    }

    val minCondition = parsedMin
      .map { minValue =>
        valueColumn >= lit(minValue.bigDecimal)
      }
      .getOrElse(lit(true))

    val maxCondition = parsedMax
      .map { maxValue =>
        valueColumn <= lit(maxValue.bigDecimal)
      }
      .getOrElse(lit(true))

    minCondition && maxCondition
  }

  /** Parses a finite numeric bound.
    */
  private def parseFiniteNumericValue(value: String, fieldName: String, column: String): BigDecimal = {
    try {
      val parsed = BigDecimal(value).bigDecimal.stripTrailingZeros
      val normalized = if (parsed.scale < 0) parsed.setScale(0) else parsed
      val requiredPrecision = math.max(normalized.precision, normalized.scale)
      if (requiredPrecision > DecimalType.MAX_PRECISION) {
        throw ValidationConfigException(
          s"Range validation error for column '$column'${flowContext}: '$fieldName' value '$value' exceeds Spark's maximum decimal precision of ${DecimalType.MAX_PRECISION} digits"
        )
      }
      BigDecimal(normalized)
    } catch {
      case e: NumberFormatException =>
        val isNonFinite =
          try !java.lang.Double.isFinite(java.lang.Double.parseDouble(value))
          catch { case _: NumberFormatException => false }

        if (isNonFinite)
          throw ValidationConfigException(
            s"Range validation error for column '$column'${flowContext}: '$fieldName' value '$value' must be finite",
            e
          )
        else
          throw ValidationConfigException(
            s"Range validation error for column '$column'${flowContext}: '$fieldName' value '$value' is not numeric",
            e
          )
    }
  }

  private def decimalTypeFor(bounds: Seq[BigDecimal]): DecimalType = {
    val integerDigits = bounds.map(value => math.max(value.precision - value.scale, 0)).max
    DecimalType(DecimalType.MAX_PRECISION, DecimalType.MAX_PRECISION - integerDigits)
  }
}
