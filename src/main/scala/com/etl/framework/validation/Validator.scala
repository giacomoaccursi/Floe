package com.etl.framework.validation

import com.etl.framework.config.ValidationRule
import com.etl.framework.validation.validators.{DomainValidator, RangeValidator, RegexValidator}
import org.apache.spark.sql.DataFrame

/**
 * Trait for custom validators
 */
trait Validator {
  /**
   * Validates a DataFrame according to the rule
   * 
   * @param df DataFrame to validate
   * @param rule Validation rule configuration
   * @return ValidationStepResult with valid and rejected records
   */
  def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult
}

