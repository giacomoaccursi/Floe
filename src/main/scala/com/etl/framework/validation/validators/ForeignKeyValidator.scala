package com.etl.framework.validation.validators

import com.etl.framework.config.{FlowConfig, ForeignKeyConfig, ValidationRule}
import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.validation.{ValidationStepResult, ValidationUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

/** Validator for Foreign Key integrity. Supports composite (multi-column) foreign keys.
  */
class ForeignKeyValidator(
    flowConfig: FlowConfig,
    validatedFlows: Map[String, DataFrame],
    flowName: Option[String] = None
) extends FlowConfigValidator(flowConfig, flowName) {

  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    if (flowConfig.validation.foreignKeys.isEmpty) {
      ValidationUtils.validResult(df)
    } else {
      // Pre-compute refs deduplicated by (flow, columns).
      // No explicit broadcast — AQE decides the optimal join strategy at runtime.
      val refs: Map[(String, Seq[String]), DataFrame] =
        flowConfig.validation.foreignKeys
          .map(fk => (fk.references.flow, fk.references.columns))
          .distinct
          .flatMap { case (refFlow, refCols) =>
            validatedFlows.get(refFlow).map { refDf =>
              (refFlow, refCols) -> refDf.select(refCols.map(col): _*)
            }
          }
          .toMap

      flowConfig.validation.foreignKeys.foldLeft(ValidationStepResult(df, None)) {
        case (ValidationStepResult(currentDf, rejectedAcc, _), fk) =>
          refs.get((fk.references.flow, fk.references.columns)) match {
            case None =>
              throw ValidationConfigException(
                s"Referenced flow '${fk.references.flow}' not found in flow $flowName"
              )
            case Some(refKeys) =>
              validateSingleFk(currentDf, rejectedAcc, fk, refKeys)
          }
      }
    }
  }

  private def validateSingleFk(
      currentDf: DataFrame,
      rejectedAcc: Option[DataFrame],
      fk: ForeignKeyConfig,
      refKeys: DataFrame
  ): ValidationStepResult = {
    // Exclude rows where ANY FK column is NULL (standard SQL semantics)
    val nonNullCondition = fk.columns.map(c => col(c).isNotNull).reduce(_ && _)
    val nullCondition = fk.columns.map(c => col(c).isNull).reduce(_ || _)
    val nonNullCurrentDf = currentDf.filter(nonNullCondition)

    // Rename ref columns to avoid ambiguity, add marker
    val renamedPairs = fk.columns.zip(fk.references.columns)
    val markedRef = renamedPairs
      .foldLeft(refKeys) { case (df, (_, refCol)) =>
        df.withColumnRenamed(refCol, s"_fk_ref_$refCol")
      }
      .withColumn("_fk_ref_exists", lit(true))

    val joinCondition = renamedPairs
      .map { case (localCol, refCol) => nonNullCurrentDf(localCol) === col(s"_fk_ref_$refCol") }
      .reduce(_ && _)

    val refColsToDrop = renamedPairs.map { case (_, refCol) => s"_fk_ref_$refCol" }

    val joinedWithRef = nonNullCurrentDf
      .join(markedRef, joinCondition, "left")
      .drop(refColsToDrop: _*)
      .cache()

    try {
      val orphans = joinedWithRef.filter(col("_fk_ref_exists").isNull).drop("_fk_ref_exists")
      val validNonNull = joinedWithRef.filter(col("_fk_ref_exists").isNotNull).drop("_fk_ref_exists")

      if (orphans.isEmpty) {
        ValidationStepResult(currentDf, rejectedAcc)
      } else {
        val orphansWithMetadata = ValidationUtils.addRejectionMetadata(
          orphans,
          "FK_VIOLATION",
          s"Foreign key violation in flow $flowName: ${fk.displayName}",
          "fk_validation"
        )
        val newRejected = ValidationUtils.combineRejected(rejectedAcc, Some(orphansWithMetadata))
        val nullFkRecords = currentDf.filter(nullCondition)
        val newCurrent = validNonNull.unionByName(nullFkRecords)
        ValidationStepResult(newCurrent, newRejected)
      }
    } finally {
      joinedWithRef.unpersist()
    }
  }
}
