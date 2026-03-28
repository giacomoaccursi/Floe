package com.etl.framework.validation.validators

import com.etl.framework.config.{FlowConfig, ValidationRule}
import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.validation.{ValidationStepResult, ValidationUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col, lit}

/** Validator for Foreign Key integrity Validates that foreign key values exist in referenced tables
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
      // Pre-compute broadcast refs deduplicated by (flow, column): if multiple FK
      // constraints reference the same flow/column pair the broadcast is created once
      val broadcastedRefs: Map[(String, String), DataFrame] =
        flowConfig.validation.foreignKeys
          .map(fk => (fk.references.flow, fk.references.column))
          .distinct
          .flatMap { case (refFlow, refCol) =>
            validatedFlows.get(refFlow).map { refDf =>
              (refFlow, refCol) -> broadcast(refDf.select(refCol))
            }
          }
          .toMap

      flowConfig.validation.foreignKeys.foldLeft(ValidationStepResult(df, None)) {
        case (ValidationStepResult(currentDf, rejectedAcc, _), fk) =>
          broadcastedRefs.get((fk.references.flow, fk.references.column)) match {
            case None =>
              throw ValidationConfigException(
                s"Referenced flow '${fk.references.flow}' not found in flow $flowName"
              )

            case Some(refKeys) =>
              // Single left join with a marker column replaces the previous left_anti +
              // left_anti double-join pattern (one on nonNullCurrentDf, one on currentDf).
              // NULL FK values are excluded upfront: they are not referential violations
              // (NULL means "no reference", consistent with standard SQL semantics).
              val nonNullCurrentDf = currentDf.filter(col(fk.column).isNotNull)
              val markedRef = refKeys
                .withColumnRenamed(fk.references.column, "_fk_ref_key")
                .withColumn("_fk_ref_exists", lit(true))
              val joinCondition = nonNullCurrentDf(fk.column) === col("_fk_ref_key")
              val joinedWithRef = nonNullCurrentDf
                .join(markedRef, joinCondition, "left")
                .drop("_fk_ref_key")
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
                    s"Foreign key violation in flow $flowName: ${fk.displayName} (${fk.column} -> ${fk.references.flow}.${fk.references.column})",
                    "fk_validation"
                  )
                  val newRejected = ValidationUtils.combineRejected(rejectedAcc, Some(orphansWithMetadata))
                  // Reconstruct valid records: non-null FK records that passed + null FK records
                  val nullFkRecords = currentDf.filter(col(fk.column).isNull)
                  val newCurrent = validNonNull.unionByName(nullFkRecords)
                  ValidationStepResult(newCurrent, newRejected)
                }
              } finally {
                joinedWithRef.unpersist()
              }
          }
      }
    }
  }
}
