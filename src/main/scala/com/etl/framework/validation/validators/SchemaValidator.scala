package com.etl.framework.validation.validators

import com.etl.framework.config.{ColumnDataType, FlowConfig, ValidationRule}
import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.validation.{ValidationStepResult, ValidationUtils}
import org.apache.spark.sql.DataFrame

/** Validator for schema validation Validates that all required columns are present
  */
class SchemaValidator(flowConfig: FlowConfig, flowName: Option[String] = None)
    extends FlowConfigValidator(flowConfig, flowName) {

  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    if (!flowConfig.schema.enforceSchema) {
      ValidationUtils.validResult(df)
    } else {
      val requiredColumns = flowConfig.schema.columns.map(_.name).toSet
      val actualColumns = df.columns.toSet

      // 1. Check for missing required columns
      val missingColumns = requiredColumns -- actualColumns
      if (missingColumns.nonEmpty) {
        ValidationUtils.resultWithRejections(
          df.limit(0),
          df,
          "SCHEMA_VALIDATION_FAILED",
          s"Missing required columns in flow $flowName: ${missingColumns.mkString(", ")}",
          "schema_validation"
        )
      } else {
        val mismatchedTypes = flowConfig.schema.columns.flatMap { configured =>
          val expected = ColumnDataType
            .parse(configured.`type`)
            .fold(error => throw ValidationConfigException(error), identity)
          val actual = df.schema(configured.name).dataType
          if (actual != expected) Some(s"${configured.name}: expected ${expected.sql} but found ${actual.sql}")
          else None
        }
        if (mismatchedTypes.nonEmpty)
          ValidationUtils.resultWithRejections(
            df.limit(0),
            df,
            "SCHEMA_TYPE_MISMATCH",
            s"Column type mismatch in flow $flowName: ${mismatchedTypes.mkString(", ")}",
            "schema_validation"
          )
        else validateExtraColumns(df, requiredColumns, actualColumns)
      }
    }
  }

  private def validateExtraColumns(
      df: DataFrame,
      requiredColumns: Set[String],
      actualColumns: Set[String]
  ): ValidationStepResult = {
    if (!flowConfig.schema.allowExtraColumns) {
      // Schema validation runs before Floe adds diagnostic columns, so every undeclared input column is external.
      val extraColumns = actualColumns -- requiredColumns
      if (extraColumns.nonEmpty) {
        ValidationUtils.resultWithRejections(
          df.limit(0),
          df,
          "SCHEMA_EXTRA_COLUMNS",
          s"Unexpected extra columns in flow $flowName: ${extraColumns.mkString(", ")}",
          "schema_validation"
        )
      } else {
        ValidationUtils.validResult(df)
      }
    } else ValidationUtils.validResult(df)
  }
}
