package com.etl.framework.validation.validators

import com.etl.framework.config.{FlowConfig, ValidationRule}
import com.etl.framework.validation.ValidationStepResult
import org.apache.spark.sql.DataFrame

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
      return ValidationStepResult(df, None)
    }
    
    var currentDf = df
    var rejectedDf: Option[DataFrame] = None
    
    for (fk <- flowConfig.validation.foreignKeys) {
      val referencedFlow = validatedFlows.get(fk.references.flow)
      
      if (referencedFlow.isEmpty) {
        throw new ForeignKeyValidationException(
          s"Referenced flow '${fk.references.flow}' not found$flowContext"
        )
      }
      
      // Perform left anti join to find orphan records
      val orphans = currentDf
        .join(
          referencedFlow.get.select(fk.references.column),
          currentDf(fk.column) === referencedFlow.get(fk.references.column),
          "left_anti"
        )
      
      if (!orphans.isEmpty) {
        val orphansWithMetadata = addRejectionMetadata(
          orphans,
          "FK_VIOLATION",
          s"Foreign key violation$flowContext: ${fk.name} (${fk.column} -> ${fk.references.flow}.${fk.references.column})",
          "fk_validation"
        )
        
        rejectedDf = combineRejected(rejectedDf, Some(orphansWithMetadata))
        currentDf = currentDf.join(orphans.select(fk.column), Seq(fk.column), "left_anti")
      }
    }
    
    ValidationStepResult(currentDf, rejectedDf)
  }
}

/**
 * Exception thrown when Foreign Key validation fails
 */
class ForeignKeyValidationException(message: String) extends RuntimeException(message)
