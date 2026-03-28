package com.etl.framework.validation.validators

import com.etl.framework.config.ValidationRule
import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.validation.{ValidationStepResult, Validator}
import com.etl.framework.validation.ValidationColumns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

/** Base class for validators with common functionality Reduces code duplication across validator implementations
  */
abstract class BaseValidator(flowName: Option[String] = None) extends Validator {

  /** Validates the DataFrame according to the rule Template method that delegates to abstract methods
    */
  final override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    // 1. Extract and validate column
    val column = extractColumn(rule)

    // 2. Build validation condition
    val validationCondition = buildValidationCondition(df, rule, column)

    // 3. Split into valid and invalid
    val validDf = df.filter(validationCondition)
    val invalidDf = df.filter(!validationCondition)

    // 4. Create result
    if (invalidDf.isEmpty) {
      ValidationStepResult(validDf, None)
    } else {
      val rejectedDf = createRejectedDataFrame(invalidDf, rule, column)
      ValidationStepResult(validDf, Some(rejectedDf))
    }
  }

  /** Extracts and validates the column from the rule
    */
  protected def extractColumn(rule: ValidationRule): String = {
    rule.column.getOrElse {
      throw ValidationConfigException(
        s"${validatorName} validation error${flowContext}: 'column' field is required"
      )
    }
  }

  /** Builds the validation condition (to be implemented by subclasses)
    */
  protected def buildValidationCondition(df: DataFrame, rule: ValidationRule, column: String): Column

  /** Creates the rejected DataFrame with metadata columns
    */
  protected def createRejectedDataFrame(invalidDf: DataFrame, rule: ValidationRule, column: String): DataFrame = {
    invalidDf
      .withColumn(REJECTION_CODE, lit(rejectionCode))
      .withColumn(REJECTION_REASON, lit(rejectionReason(rule, column)))
      .withColumn(VALIDATION_STEP, lit(validationStep))
      .withColumn(REJECTED_AT, current_timestamp())
  }

  /** Returns the flow context string for error messages
    */
  protected def flowContext: String = flowName.map(f => s" in flow '$f'").getOrElse("")

  // Abstract methods to be implemented by subclasses

  /** Name of the validator (e.g., "Regex", "Range", "Domain")
    */
  protected def validatorName: String

  /** Rejection code for this validator
    */
  protected def rejectionCode: String

  /** Rejection reason message
    */
  protected def rejectionReason(rule: ValidationRule, column: String): String

  /** Validation step name
    */
  protected def validationStep: String
}
