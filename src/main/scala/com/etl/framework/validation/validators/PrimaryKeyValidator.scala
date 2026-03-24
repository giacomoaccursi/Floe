package com.etl.framework.validation.validators

import com.etl.framework.config.{FlowConfig, ValidationRule}
import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.validation.{ValidationStepResult, ValidationUtils}
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
      throw ValidationConfigException(
        s"Primary key is not defined for flow ${flowName.getOrElse("unknown")}"
      )
    } else {
      // Find duplicates using groupBy and count
      val duplicates = df
        .groupBy(pkColumns.map(col): _*)
        .agg(count("*").as("_count"))
        .filter(col("_count") > 1)
        .drop("_count")
      
      if (duplicates.isEmpty) {
        ValidationUtils.validResult(df)
      } else {
        // Single left join with a marker column replaces the previous inner + left_anti
        // double-join pattern, scanning df only once instead of twice
        val markedDuplicates = duplicates.withColumn("_is_dup", lit(true))
        val joined = df.join(markedDuplicates, pkColumns, "left")
        val rejectedDf = joined.filter(col("_is_dup").isNotNull).drop("_is_dup")
        val validDf    = joined.filter(col("_is_dup").isNull).drop("_is_dup")

        ValidationUtils.resultWithRejections(
          validDf,
          rejectedDf,
          "PK_DUPLICATE",
          s"Duplicate primary key in flow $flowName: ${pkColumns.mkString(", ")}",
          "pk_validation"
        )
      }
    }
  }
}
