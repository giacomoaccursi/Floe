package com.etl.framework.validation.validators

import com.etl.framework.config.{FlowConfig, ValidationRule}
import com.etl.framework.validation.ValidationStepResult
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Validator for Primary Key uniqueness
 * Validates that primary key columns contain unique values
 */
class PrimaryKeyValidator(flowConfig: FlowConfig, flowName: Option[String] = None) 
  extends FlowConfigValidator(flowConfig, flowName) {
  
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val pkColumns = flowConfig.validation.primaryKey
    
    if (pkColumns.isEmpty) {
      return ValidationStepResult(df, None)
    }
    
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
      s"Duplicate primary key in flow $flowName: ${pkColumns.mkString(", ")}",
      "pk_validation"
    )
    
    ValidationStepResult(validDf, Some(rejectedWithMetadata))
  }
}
