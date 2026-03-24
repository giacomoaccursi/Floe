package com.etl.framework.validation.validators

import com.etl.framework.config.{FlowConfig, ValidationRule}
import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.validation.{ValidationStepResult, ValidationUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col}

/**
 * Validator for Foreign Key integrity
 * Validates that foreign key values exist in referenced tables
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
        case (ValidationStepResult(currentDf, rejectedAcc), fk) =>
          broadcastedRefs.get((fk.references.flow, fk.references.column)) match {
            case None =>
              throw ValidationConfigException(
                s"Referenced flow '${fk.references.flow}' not found in flow $flowName"
              )

            case Some(refKeys) =>
              // Perform left anti join to find orphan records.
              // NULL FK values are excluded: they are not referential violations
              // (NULL means "no reference", consistent with standard SQL semantics).
              val nonNullCurrentDf = currentDf.filter(col(fk.column).isNotNull)
              val orphans = nonNullCurrentDf
                .join(
                  refKeys,
                  nonNullCurrentDf(fk.column) === refKeys(fk.references.column),
                  "left_anti"
                )
              
              if (orphans.isEmpty) {
                ValidationStepResult(currentDf, rejectedAcc)
              } else {
                val orphansWithMetadata = ValidationUtils.addRejectionMetadata(
                  orphans,
                  "FK_VIOLATION",
                  s"Foreign key violation in flow $flowName: ${fk.name} (${fk.column} -> ${fk.references.flow}.${fk.references.column})",
                  "fk_validation"
                )
                
                val newRejected = ValidationUtils.combineRejected(rejectedAcc, Some(orphansWithMetadata))
                val newCurrent = currentDf.join(orphans.select(fk.column), Seq(fk.column), "left_anti")
                
                ValidationStepResult(newCurrent, newRejected)
              }
          }
      }
    }
  }
}
