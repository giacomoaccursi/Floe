package com.etl.framework.validation.validators

import com.etl.framework.config.{DomainsConfig, FlowConfig, OnFailureAction, ValidationRule}
import com.etl.framework.validation.{ValidationStepResult, ValidationUtils, Validator, ValidatorFactory}
import com.etl.framework.validation.ValidationColumns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Validator for custom validation rules with reject/warn support. Orchestrates
  * multiple validators and handles skipNull for all rule types.
  */
class CustomRulesValidator(
    flowConfig: FlowConfig,
    domainsConfig: Option[DomainsConfig] = None,
    flowName: Option[String] = None
)(implicit spark: SparkSession)
    extends Validator {

  override def validate(
      df: DataFrame,
      rule: ValidationRule
  ): ValidationStepResult = {
    val (finalDf, finalRejected, finalWarned) =
      flowConfig.validation.rules.foldLeft((df, Option.empty[DataFrame], Option.empty[DataFrame])) {
        case ((currentDf, rejectedAcc, warnedAcc), customRule) =>
          val validator = ValidatorFactory.create(customRule, domainsConfig, flowName)

          val skipNull = customRule.skipNull.getOrElse(true)
          val targetColumn = customRule.column

          val (dfForValidation, nullRows) =
            if (skipNull && targetColumn.isDefined) {
              val nulls = currentDf.filter(col(targetColumn.get).isNull)
              val nonNulls = currentDf.filter(col(targetColumn.get).isNotNull)
              (nonNulls, Some(nulls))
            } else {
              (currentDf, None)
            }

          val result = validator.validate(dfForValidation, customRule)

          val resultWithNulls = nullRows match {
            case Some(nulls) =>
              ValidationStepResult(result.valid.unionByName(nulls), result.rejected)
            case None => result
          }

          customRule.onFailure match {
            case OnFailureAction.Reject =>
              val updatedRejected = ValidationUtils.combineRejected(rejectedAcc, resultWithNulls.rejected)
              (resultWithNulls.valid, updatedRejected, warnedAcc)

            case OnFailureAction.Warn =>
              val updatedWarned = resultWithNulls.rejected match {
                case Some(failedDf) if !failedDf.isEmpty =>
                  val warningDf = buildWarningDf(failedDf, customRule)
                  ValidationUtils.combineRejected(warnedAcc, Some(warningDf))
                case _ => warnedAcc
              }
              // Record stays valid — do not remove from currentDf
              (currentDf, rejectedAcc, updatedWarned)

            case OnFailureAction.Skip =>
              (currentDf, rejectedAcc, warnedAcc)
          }
      }

    ValidationStepResult(finalDf, finalRejected, finalWarned)
  }

  /** Builds a warning DataFrame with PK columns + warning metadata.
    */
  private def buildWarningDf(failedDf: DataFrame, rule: ValidationRule): DataFrame = {
    val pkColumns = flowConfig.validation.primaryKey
    val pkSelect = if (pkColumns.nonEmpty) pkColumns else failedDf.columns.filterNot(_.startsWith("_")).toSeq

    val warningMessage = rule.description.getOrElse(
      s"${rule.`type`.name} validation failed for column '${rule.column.getOrElse("unknown")}'"
    )

    failedDf
      .select(pkSelect.map(col): _*)
      .withColumn(WARNING_RULE, lit(rule.`type`.name))
      .withColumn(WARNING_MESSAGE, lit(warningMessage))
      .withColumn(WARNING_COLUMN, lit(rule.column.getOrElse("unknown")))
      .withColumn(WARNED_AT, current_timestamp())
  }
}
