package com.etl.framework.validation

import com.etl.framework.config.{DomainsConfig, FlowConfig, ValidationRule}
import com.etl.framework.validation.validators._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Core validation engine that coordinates all validation steps
 * Delegates actual validation logic to specialized validators
 */
class ValidationEngine(domainsConfig: Option[DomainsConfig] = None)(implicit spark: SparkSession) {
  
  /**
   * Validates a DataFrame according to the flow configuration
   * 
   * @param df Input DataFrame to validate
   * @param flowConfig Flow configuration containing validation rules
   * @param validatedFlows Map of already validated flows (for FK validation)
   * @return ValidationResult containing valid and rejected DataFrames
   */
  def validate(
    df: DataFrame,
    flowConfig: FlowConfig,
    validatedFlows: Map[String, DataFrame] = Map.empty
  ): ValidationResult = {
    
    // Add warnings column to track non-fatal issues
    var currentDf = df.withColumn("_warnings", array().cast("array<string>"))
    var rejectedDf: Option[DataFrame] = None
    val rejectionReasons = scala.collection.mutable.Map[String, Long]()
    
    val flowName = Some(flowConfig.name)
    
    // Step 1: Schema validation
    val schemaValidator = new SchemaValidator(flowConfig, flowName)
    val schemaResult = schemaValidator.validate(currentDf, ValidationRule("schema"))
    currentDf = schemaResult.valid
    rejectedDf = ValidationUtils.combineRejected(rejectedDf, schemaResult.rejected)
    schemaResult.rejected.foreach(r => rejectionReasons("schema_validation") = r.count())
    
    // Step 2: Not-null validation
    val notNullValidator = new NotNullValidator(flowConfig, flowName)
    val notNullResult = notNullValidator.validate(currentDf, ValidationRule("not_null"))
    currentDf = notNullResult.valid
    rejectedDf = ValidationUtils.combineRejected(rejectedDf, notNullResult.rejected)
    notNullResult.rejected.foreach(r => rejectionReasons("not_null_validation") = r.count())
    
    // Step 3: Primary Key validation
    if (flowConfig.validation.primaryKey.nonEmpty) {
      val pkValidator = new PrimaryKeyValidator(flowConfig, flowName)
      val pkResult = pkValidator.validate(currentDf, ValidationRule("pk"))
      currentDf = pkResult.valid
      rejectedDf = ValidationUtils.combineRejected(rejectedDf, pkResult.rejected)
      pkResult.rejected.foreach(r => rejectionReasons("pk_validation") = r.count())
    }
    
    // Step 4: Foreign Key validation
    if (flowConfig.validation.foreignKeys.nonEmpty) {
      val fkValidator = new ForeignKeyValidator(flowConfig, validatedFlows, flowName)
      val fkResult = fkValidator.validate(currentDf, ValidationRule("fk"))
      currentDf = fkResult.valid
      rejectedDf = ValidationUtils.combineRejected(rejectedDf, fkResult.rejected)
      fkResult.rejected.foreach(r => rejectionReasons("fk_validation") = r.count())
    }
    
    // Step 5: Custom validation rules
    if (flowConfig.validation.rules.nonEmpty) {
      val customRulesValidator = new CustomRulesValidator(flowConfig, domainsConfig, flowName)
      val rulesResult = customRulesValidator.validate(currentDf, ValidationRule("custom_rules"))
      currentDf = rulesResult.valid
      rejectedDf = ValidationUtils.combineRejected(rejectedDf, rulesResult.rejected)
      rulesResult.rejected.foreach(r => rejectionReasons("rules_validation") = r.count())
    }
    
    ValidationResult(currentDf, rejectedDf, rejectionReasons.toMap)
  }
}

/**
 * Result of validation containing valid and rejected DataFrames
 */
case class ValidationResult(
  valid: DataFrame,
  rejected: Option[DataFrame],
  rejectionReasons: Map[String, Long] = Map.empty
)

/**
 * Result of a single validation step
 */
case class ValidationStepResult(
  valid: DataFrame,
  rejected: Option[DataFrame],
  validWithWarnings: DataFrame = null
) {
  // If validWithWarnings is not provided, use valid
  def getValidWithWarnings: DataFrame = if (validWithWarnings != null) validWithWarnings else valid
}
