package com.etl.framework.validation.validators

import com.etl.framework.config.{FlowConfig, ForeignKeyConfig, ValidationRule}
import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.validation.{ValidationStepResult, ValidationUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

import java.util.UUID

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
      // Pre-compute distinct reference keys: historical or otherwise duplicated
      // parent rows must not multiply valid child records in the left join.
      // No explicit broadcast — AQE decides the optimal join strategy at runtime.
      val refs: Map[(String, Seq[String]), DataFrame] =
        flowConfig.validation.foreignKeys
          .map(fk => (fk.references.flow, fk.references.columns))
          .distinct
          .flatMap { case (refFlow, refCols) =>
            validatedFlows.get(refFlow).map { refDf =>
              (refFlow, refCols) -> refDf.select(refCols.map(col): _*).distinct()
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

    // Project reference keys under per-validation aliases so source columns can use any non-reserved name.
    val renamedPairs = fk.columns.zip(fk.references.columns)
    def temporaryNames(suffix: String): Seq[String] =
      renamedPairs.indices.map(index => s"__floe_fk_ref_${index}_$suffix") :+ s"__floe_fk_exists_$suffix"
    val suffix = Iterator
      .continually(UUID.randomUUID().toString.replace("-", ""))
      .find(candidate => temporaryNames(candidate).forall(name => !currentDf.columns.exists(_.equalsIgnoreCase(name))))
      .get
    val referenceAliases = renamedPairs.indices.map(index => s"__floe_fk_ref_${index}_$suffix")
    val markerColumn = s"__floe_fk_exists_$suffix"
    val markedRef = refKeys
      .select(renamedPairs.zip(referenceAliases).map { case ((_, refCol), alias) => col(refCol).as(alias) }: _*)
      .withColumn(markerColumn, lit(true))

    val joinCondition = renamedPairs
      .zip(referenceAliases)
      .map { case ((localCol, _), alias) => nonNullCurrentDf(localCol) === markedRef(alias) }
      .reduce(_ && _)

    val refColsToDrop = referenceAliases

    val joinedWithRef = nonNullCurrentDf
      .join(markedRef, joinCondition, "left")
      .drop(refColsToDrop: _*)
      .cache()

    try {
      val orphans = joinedWithRef.filter(col(markerColumn).isNull).drop(markerColumn)
      val validNonNull = joinedWithRef.filter(col(markerColumn).isNotNull).drop(markerColumn)

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
