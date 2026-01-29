package com.etl.framework.validation.validators

import com.etl.framework.config.FlowConfig
import com.etl.framework.validation.Validator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Base class for validators that need access to FlowConfig
 * These validators validate structural aspects (schema, PK, FK, not-null)
 * rather than individual field values
 */
abstract class FlowConfigValidator(
  protected val flowConfig: FlowConfig,
  protected val flowName: Option[String] = None
) extends Validator {
  
  /**
   * Returns the flow context string for error messages
   */
  protected def flowContext: String = flowName.map(f => s" in flow '$f'").getOrElse("")
  
  /**
   * Adds rejection metadata to a DataFrame
   */
  protected def addRejectionMetadata(
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
  protected def combineRejected(
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
