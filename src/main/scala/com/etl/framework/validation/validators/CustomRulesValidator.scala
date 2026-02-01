package com.etl.framework.validation.validators

import com.etl.framework.config.{DomainsConfig, FlowConfig, ValidationRule}
import com.etl.framework.validation.{ValidationStepResult, ValidationUtils, Validator, ValidatorFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Validator for custom validation rules with reject/warn support
 * This validator orchestrates multiple validators, so it implements Validator directly
 * rather than extending BaseValidator
 */
class CustomRulesValidator(
  flowConfig: FlowConfig,
  domainsConfig: Option[DomainsConfig] = None,
  flowName: Option[String] = None
)(implicit spark: SparkSession) extends Validator {

  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val (finalDf, finalRejected) = flowConfig.validation.rules.foldLeft((df, Option.empty[DataFrame])) {
      case ((currentDf, rejectedAcc), customRule) =>
        val validator = ValidatorFactory.create(customRule, domainsConfig, flowName)
        val result = validator.validate(currentDf, customRule)
        
        customRule.onFailure match {
          case "reject" => processRejectRule(currentDf, result, rejectedAcc)
          case _ => processWarnRule(currentDf, result, customRule, rejectedAcc)
        }
    }
    
    ValidationStepResult(finalDf, finalRejected)
  }
  
  /**
   * Processes a rule with reject behavior
   */
  private def processRejectRule(
    currentDf: DataFrame,
    result: ValidationStepResult,
    rejectedAcc: Option[DataFrame]
  ): (DataFrame, Option[DataFrame]) = {
    val updatedRejected = ValidationUtils.combineRejected(rejectedAcc, result.rejected)
    (result.valid, updatedRejected)
  }
  
  /**
   * Processes a rule with warn behavior
   */
  private def processWarnRule(
    currentDf: DataFrame,
    result: ValidationStepResult,
    rule: ValidationRule,
    rejectedAcc: Option[DataFrame]
  ): (DataFrame, Option[DataFrame]) = {
    val updatedDf = addWarningsToFailedRecords(currentDf, result, rule)
    (updatedDf, rejectedAcc)
  }
  
  /**
   * Adds warning messages to records that failed validation
   */
  private def addWarningsToFailedRecords(
    df: DataFrame,
    result: ValidationStepResult,
    rule: ValidationRule
  ): DataFrame = {
    result.rejected match {
      case None => df
      case Some(rejected) if rejected.isEmpty => df
      case Some(rejected) =>
        val pkColumns = getPrimaryKeyColumns(df)
        val warningMessage = createWarningMessage(rule)
        val failedRecords = rejected.select(pkColumns.map(col): _*)
        
        joinWarningsToDataFrame(df, failedRecords, pkColumns, warningMessage)
    }
  }
  
  /**
   * Gets primary key columns for identifying records
   */
  private def getPrimaryKeyColumns(df: DataFrame): Seq[String] = {
    if (flowConfig.validation.primaryKey.nonEmpty) {
      flowConfig.validation.primaryKey
    } else {
      // If no PK defined, use all columns as identifier (excluding metadata columns)
      df.columns.filterNot(_.startsWith("_")).toSeq
    }
  }
  
  /**
   * Creates a warning message for a validation rule
   */
  private def createWarningMessage(rule: ValidationRule): String = {
    rule.description.getOrElse(
      s"${rule.`type`} validation failed for column '${rule.column.getOrElse("unknown")}'"
    )
  }
  
  /**
   * Joins warning messages to the DataFrame
   */
  private def joinWarningsToDataFrame(
    df: DataFrame,
    failedRecords: DataFrame,
    pkColumns: Seq[String],
    warningMessage: String
  ): DataFrame = {
    df.join(
      failedRecords.withColumn("_warning_msg", lit(warningMessage)),
      pkColumns,
      "left"
    ).withColumn(
      "_warnings",
      when(col("_warning_msg").isNotNull,
        array_union(col("_warnings"), array(col("_warning_msg"))))
        .otherwise(col("_warnings"))
    ).drop("_warning_msg")
  }
}
