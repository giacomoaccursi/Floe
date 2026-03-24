package com.etl.framework.validation

import com.etl.framework.config.{
  DomainsConfig,
  FlowConfig,
  ValidationRule,
  ValidationRuleType
}
import com.etl.framework.validation.ValidationColumns._
import com.etl.framework.validation.validators._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Core validation engine that coordinates all validation steps Delegates
  * actual validation logic to specialized validators
  */
class ValidationEngine(domainsConfig: Option[DomainsConfig] = None)(implicit
    spark: SparkSession
) {

  /** Validates a DataFrame according to the flow configuration
    *
    * @param df
    *   Input DataFrame to validate
    * @param flowConfig
    *   Flow configuration containing validation rules
    * @param validatedFlows
    *   Map of already validated flows (for FK validation)
    * @return
    *   ValidationResult containing valid and rejected DataFrames
    */
  def validate(
      df: DataFrame,
      flowConfig: FlowConfig,
      validatedFlows: Map[String, DataFrame] = Map.empty
  ): ValidationResult = {

    val flowName = Some(flowConfig.name)
    val initialDf = df.withColumn(WARNINGS, array().cast("array<string>"))

    val validationSteps: Seq[ValidationStep] = Seq(
      ValidationStep(
        shouldExecute = true,
        new SchemaValidator(flowConfig, flowName),
        ValidationRule(ValidationRuleType.Schema)
      ),
      ValidationStep(
        shouldExecute = true,
        new NotNullValidator(flowConfig, flowName),
        ValidationRule(ValidationRuleType.NotNull)
      ),
      ValidationStep(
        shouldExecute = true,
        new PrimaryKeyValidator(flowConfig, flowName),
        ValidationRule(ValidationRuleType.PKUniqueness)
      ),
      ValidationStep(
        flowConfig.validation.foreignKeys.nonEmpty,
        new ForeignKeyValidator(flowConfig, validatedFlows, flowName),
        ValidationRule(ValidationRuleType.FKIntegrity)
      ),
      ValidationStep(
        flowConfig.validation.rules.nonEmpty,
        new CustomRulesValidator(flowConfig, domainsConfig, flowName),
        ValidationRule(ValidationRuleType.Custom)
      )
    )

    // Execute validation steps
    val finalState = validationSteps
      .filter(_.shouldExecute)
      .foldLeft(ValidationState(initialDf, None)) { (state, step) =>
        executeValidationStep(state, step)
      }

    // Single Spark action: count rejections per validation step using the
    // _validation_step column already set by each validator's metadata
    val rejectionReasons: Map[String, Long] = finalState.rejected match {
      case Some(rejectedDf) =>
        rejectedDf
          .groupBy(VALIDATION_STEP)
          .count()
          .collect()
          .map(row => row.getString(0) -> row.getLong(1))
          .toMap
      case None => Map.empty
    }

    ValidationResult(
      finalState.valid,
      finalState.rejected,
      rejectionReasons
    )
  }

  /** Executes a single validation step and updates the state
    */
  private def executeValidationStep(
      state: ValidationState,
      step: ValidationStep
  ): ValidationState = {
    val result = step.validator.validate(state.valid, step.rule)

    ValidationState(
      valid = result.valid,
      rejected = ValidationUtils.combineRejected(state.rejected, result.rejected)
    )
  }
}

/** Represents a single validation step
  */
private case class ValidationStep(
    shouldExecute: Boolean,
    validator: Validator,
    rule: ValidationRule
)

/** Holds the state during validation execution
  */
private case class ValidationState(
    valid: DataFrame,
    rejected: Option[DataFrame]
)

/** Result of validation containing valid and rejected DataFrames
  */
case class ValidationResult(
    valid: DataFrame,
    rejected: Option[DataFrame],
    rejectionReasons: Map[String, Long] = Map.empty
)

/** Result of a single validation step
  */
case class ValidationStepResult(
    valid: DataFrame,
    rejected: Option[DataFrame]
)
