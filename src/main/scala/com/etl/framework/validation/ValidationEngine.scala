package com.etl.framework.validation

 import com.etl.framework.config.{DomainsConfig, FlowConfig}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Core validation engine that coordinates all validation steps
 * Adds a warnings column to track non-fatal validation issues
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
    
    // Step 1: Schema validation
    val schemaResult = validateSchema(currentDf, flowConfig)
    currentDf = schemaResult.valid
    rejectedDf = combineRejected(rejectedDf, schemaResult.rejected)
    schemaResult.rejected.foreach(r => rejectionReasons("schema_validation") = r.count())
    
    // Step 2: Not-null validation
    val notNullResult = validateNotNull(currentDf, flowConfig)
    currentDf = notNullResult.valid
    rejectedDf = combineRejected(rejectedDf, notNullResult.rejected)
    notNullResult.rejected.foreach(r => rejectionReasons("not_null_validation") = r.count())
    
    // Step 3: Primary Key validation
    if (flowConfig.validation.primaryKey.nonEmpty) {
      val pkResult = validatePrimaryKey(currentDf, flowConfig)
      currentDf = pkResult.valid
      rejectedDf = combineRejected(rejectedDf, pkResult.rejected)
      pkResult.rejected.foreach(r => rejectionReasons("pk_validation") = r.count())
    }
    
    // Step 4: Foreign Key validation
    if (flowConfig.validation.foreignKeys.nonEmpty) {
      val fkResult = validateForeignKeys(currentDf, flowConfig, validatedFlows)
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
   * Validates that all required columns are present and types match
   */
  private def validateSchema(df: DataFrame, flowConfig: FlowConfig): ValidationStepResult = {
    if (!flowConfig.schema.enforceSchema) {
      return ValidationStepResult(df, None)
    }
    
    val requiredColumns = flowConfig.schema.columns.map(_.name).toSet
    val actualColumns = df.columns.toSet
    
    // Check for missing required columns
    val missingColumns = requiredColumns -- actualColumns
    if (missingColumns.nonEmpty) {
      // Reject all records if required columns are missing
      val rejectedDf = addRejectionMetadata(
        df,
        "SCHEMA_VALIDATION_FAILED",
        s"Missing required columns: ${missingColumns.mkString(", ")}",
        "schema_validation"
      )
      return ValidationStepResult(
        df.limit(0), // Empty valid DataFrame
        Some(rejectedDf)
      )
    }
    
    // TODO: Add type validation
    ValidationStepResult(df, None)
  }
  
  /**
   * Validates not-null constraints
   */
  private def validateNotNull(df: DataFrame, flowConfig: FlowConfig): ValidationStepResult = {
    val notNullColumns = flowConfig.schema.columns
      .filter(!_.nullable)
      .map(_.name)
    
    if (notNullColumns.isEmpty) {
      return ValidationStepResult(df, None)
    }
    
    // Build condition for null checks
    val nullCondition = notNullColumns
      .map(col => s"`$col` IS NULL")
      .mkString(" OR ")
    
    val rejectedDf = df.filter(nullCondition)
    val validDf = df.filter(s"NOT ($nullCondition)")
    
    if (rejectedDf.isEmpty) {
      ValidationStepResult(validDf, None)
    } else {
      val rejectedWithMetadata = addRejectionMetadata(
        rejectedDf,
        "NOT_NULL_VIOLATION",
        s"Null value in non-nullable column(s): ${notNullColumns.mkString(", ")}",
        "not_null_validation"
      )
      ValidationStepResult(validDf, Some(rejectedWithMetadata))
    }
  }
  
  /**
   * Validates Primary Key uniqueness
   */
  private def validatePrimaryKey(df: DataFrame, flowConfig: FlowConfig): ValidationStepResult = {
    val pkColumns = flowConfig.validation.primaryKey
    
    // Find duplicates using groupBy and count
    val duplicates = df
      .groupBy(pkColumns.map(col): _*)
      .agg(count("*").as("_count"))
      .filter(col("_count") > 1)
      .drop("_count")
    
    if (duplicates.isEmpty) {
      return ValidationStepResult(df, None)
    }
    
    // Join to get all duplicate records
    val rejectedDf = df.join(duplicates, pkColumns, "inner")
    val validDf = df.join(duplicates, pkColumns, "left_anti")
    
    val rejectedWithMetadata = addRejectionMetadata(
      rejectedDf,
      "PK_DUPLICATE",
      s"Duplicate primary key: ${pkColumns.mkString(", ")}",
      "pk_validation"
    )
    
    ValidationStepResult(validDf, Some(rejectedWithMetadata))
  }
  
  /**
   * Validates Foreign Key integrity
   */
  private def validateForeignKeys(
    df: DataFrame,
    flowConfig: FlowConfig,
    validatedFlows: Map[String, DataFrame]
  ): ValidationStepResult = {
    
    var currentDf = df
    var rejectedDf: Option[DataFrame] = None
    
    for (fk <- flowConfig.validation.foreignKeys) {
      val referencedFlow = validatedFlows.get(fk.references.flow)
      
      if (referencedFlow.isEmpty) {
        throw new ForeignKeyValidationException(
          s"Referenced flow '${fk.references.flow}' not found in validated flows"
        )
      }
      
      // Perform left anti join to find orphan records
      val orphans = currentDf
        .join(
          referencedFlow.get.select(fk.references.column),
          currentDf(fk.column) === referencedFlow.get(fk.references.column),
          "left_anti"
        )
      
      if (!orphans.isEmpty) {
        val orphansWithMetadata = addRejectionMetadata(
          orphans,
          "FK_VIOLATION",
          s"Foreign key violation: ${fk.name} (${fk.column} -> ${fk.references.flow}.${fk.references.column})",
          "fk_validation"
        )
        
        rejectedDf = combineRejected(rejectedDf, Some(orphansWithMetadata))
        currentDf = currentDf.join(orphans.select(fk.column), Seq(fk.column), "left_anti")
      }
    }
    
    ValidationStepResult(currentDf, rejectedDf)
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
   * Adds rejection metadata to rejected records
   */
  private def addRejectionMetadata(
    df: DataFrame,
    rejectionCode: String,
    rejectionReason: String,
    validationStep: String
  ): DataFrame = {
    df
      .withColumn("_rejection_code", lit(rejectionCode))
      .withColumn("_rejection_reason", lit(rejectionReason))
      .withColumn("_validation_step", lit(validationStep))
      .withColumn("_rejected_at", current_timestamp())
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

/**
 * Exception thrown when Foreign Key validation fails
 */
class ForeignKeyValidationException(message: String) extends RuntimeException(message)
