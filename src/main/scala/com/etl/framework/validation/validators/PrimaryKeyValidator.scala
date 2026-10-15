package com.etl.framework.validation.validators

import com.etl.framework.config.{FlowConfig, ValidationRule}
import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.util.SparkColumn
import com.etl.framework.validation.{ValidationStepResult, ValidationUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.util.UUID

/** Validator for Primary Key uniqueness Validates that primary key columns contain unique values
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
      val keyWindow = Window.partitionBy(pkColumns.map(SparkColumn(_)): _*)
      val hasNullKey = pkColumns.map(name => SparkColumn(name).isNull).reduce(_ || _)
      val classifierColumn = Iterator
        .continually(s"__floe_pk_invalid_${UUID.randomUUID().toString.replace("-", "")}")
        .find(candidate => !df.columns.exists(_.equalsIgnoreCase(candidate)))
        .get
      val classified = df
        .withColumn(classifierColumn, hasNullKey || count(lit(1)).over(keyWindow) > 1)
        .cache()

      try {
        if (classified.filter(col(classifierColumn)).isEmpty) {
          ValidationUtils.validResult(df)
        } else {
          val rejectedDf = classified.filter(col(classifierColumn)).drop(classifierColumn)
          val validDf = classified.filter(not(col(classifierColumn))).drop(classifierColumn)

          ValidationUtils.resultWithRejections(
            validDf,
            rejectedDf,
            "PK_DUPLICATE",
            s"Null or duplicate primary key in flow $flowName: ${pkColumns.mkString(", ")}",
            "pk_validation"
          )
        }
      } finally {
        classified.unpersist()
      }
    }
  }
}
