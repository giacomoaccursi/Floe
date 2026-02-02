package com.etl.framework.validation

import com.etl.framework.validation.ValidationColumns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Utility functions for validation operations
 */
object ValidationUtils {
  
  /**
   * Combines two optional DataFrames using union
   * Used to accumulate rejected records from multiple validation steps
   * 
   * @param existing Previously accumulated rejected records
   * @param newRejected Newly rejected records from current validation step
   * @return Combined rejected records, or None if both are empty
   */
  def combineRejected(
    existing: Option[DataFrame],
    newRejected: Option[DataFrame]
  ): Option[DataFrame] = {
    (existing, newRejected) match {
      case (Some(df1), Some(df2)) => Some(df1.union(df2))
      case (ex, nr) => ex.orElse(nr)
    }
  }
  
  /**
   * Adds rejection metadata columns to a DataFrame
   * 
   * @param df DataFrame containing rejected records
   * @param rejectionCode Code identifying the type of rejection
   * @param rejectionReason Human-readable reason for rejection
   * @param validationStep Name of the validation step that rejected the records
   * @return DataFrame with added metadata columns
   */
  def addRejectionMetadata(
    df: DataFrame,
    rejectionCode: String,
    rejectionReason: String,
    validationStep: String
  ): DataFrame = {
    df
      .withColumn(REJECTION_CODE, lit(rejectionCode))
      .withColumn(REJECTION_REASON, lit(rejectionReason))
      .withColumn(VALIDATION_STEP, lit(validationStep))
      .withColumn(REJECTED_AT, current_timestamp())
  }
  
  /**
   * Creates a ValidationStepResult with no rejections
   * Convenience method for early returns when validation passes
   */
  def validResult(df: DataFrame): ValidationStepResult = {
    ValidationStepResult(df, None)
  }
  
  /**
   * Creates a ValidationStepResult with rejections
   * Convenience method for creating results with rejected records
   */
  def resultWithRejections(
    validDf: DataFrame,
    rejectedDf: DataFrame,
    rejectionCode: String,
    rejectionReason: String,
    validationStep: String
  ): ValidationStepResult = {
    val rejectedWithMetadata = addRejectionMetadata(
      rejectedDf,
      rejectionCode,
      rejectionReason,
      validationStep
    )
    ValidationStepResult(validDf, Some(rejectedWithMetadata))
  }
}
