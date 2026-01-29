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
    rejectedDf = combineRejected(rejectedDf, schemaResult.rejected)
    schemaResult.rejected.foreach(r => rejectionReasons("schema_validation") = r.count())
    
    // Step 2: Not-null validation
    val notNullValidator = new NotNullValidator(flowConfig, flowName)
    val notNullResult = notNullValidator.validate(currentDf, ValidationRule("not_null"))
    currentDf = notNullResult.valid
    rejectedDf = combineRejected(rejectedDf, notNullResult.rejected)
    notNullResult.rejected.foreach(r => rejectionReasons("not_null_validation") = r.count())
    
    // Step 3: Primary Key validation
    if (flowConfig.validation.primaryKey.nonEmpty) {
      val pkValidator = new PrimaryKeyValidator(flowConfig, flowName)
      val pkResult = pkValidator.validate(currentDf, ValidationRule("pk"))
      currentDf = pkResult.valid
      rejectedDf = combineRejected(rejectedDf, pkResult.rejected)
      pkResult.rejected.foreach(r => rejectionReasons("pk_validation") = r.count())
    }
    
    // Step 4: Foreign Key validation
    if (flowConfig.validation.foreignKeys.nonEmpty) {
      val fkValidator = new ForeignKeyValidator(flowConfig, validatedFlows, flowName)
      val fkResult = fkValidator.validate(currentDf, ValidationRule("fk"))
      currentDf = fkResult.valid
      rejectedDf = combineRejected(rejectedDf, fkResult.rejected)
      fkResult.rejected.foreach(r => rejectionReasons("fk_validation") = r.count())
    }
    
    // Step 5: Custom validation rules
    if (flowConfig.validation.rules.nonEmpty) {
      val rulesResult = validateRules(currentDf, flowConfig)
      currentDf = rulesResult.valid
      rejectedDf = combineRejected(rejectedDf, rulesResult.rejected)
      rulesResult.rejected.foreach(r => rejectionReasons("rules_validation") = r.count())
    }
    
    ValidationResult(currentDf, rejectedDf, rejectionReasons.toMap)
  }
  
  /**
   * Validates custom rules with reject/warn support
   */
  private def validateRules(df: DataFrame, flowConfig: FlowConfig): ValidationStepResult = {
    var currentDf = df
    var rejectedDf: Option[DataFrame] = None
    
    for (rule <- flowConfig.validation.rules) {
      val validator = ValidatorFactory.create(rule, domainsConfig, Some(flowConfig.name))
      val result = validator.validate(currentDf, rule)
      
      if (rule.onFailure == "reject") {
        // Reject failed records
        rejectedDf = combineRejected(rejectedDf, result.rejected)
        currentDf = result.valid
      } else {
        // Add warnings to records (warn behavior)
        if (result.rejected.isDefined && !result.rejected.get.isEmpty) {
          // Get the primary key columns to identify which records failed
          val pkColumns = if (flowConfig.validation.primaryKey.nonEmpty) {
            flowConfig.validation.primaryKey
          } else {
            // If no PK defined, use all columns as identifier
            currentDf.columns.filter(_ != "_warnings").toSeq
          }
          
          // Create warning message
          val warningMessage = rule.description.getOrElse(
            s"${rule.`type`} validation failed for column '${rule.column.getOrElse("unknown")}'"
          )
          
          // Get records that failed validation
          val failedRecords = result.rejected.get.select(pkColumns.map(col): _*)
          
          // Add warning to those records
          currentDf = currentDf.join(
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
    }
    
    ValidationStepResult(currentDf, rejectedDf)
  }
  
  /**
   * Combines rejected DataFrames
   */
  private def combineRejected(
    existing: Option[DataFrame],
    newRejected: Option[DataFrame]
  ): Option[DataFrame] = {
    (existing, newRejected) match {
      case (None, None) => None
      case (Some(df), None) => Some(df)
      case (None, Some(df)) => Some(df)
      case (Some(df1), Some(df2)) => Some(df1.union(df2))
    }
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
