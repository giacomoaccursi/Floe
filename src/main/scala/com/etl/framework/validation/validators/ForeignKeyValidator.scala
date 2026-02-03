package com.etl.framework.validation.validators

import com.etl.framework.config.{FlowConfig, ValidationRule}
import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.validation.{ValidationStepResult, ValidationUtils}
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
      return ValidationUtils.validResult(df)
    }
    
    var currentDf = df
    var rejectedDf: Option[DataFrame] = None
    
    for (fk <- flowConfig.validation.foreignKeys) {
      validatedFlows.get(fk.references.flow) match {
        case None =>
          throw ValidationConfigException(
            s"Referenced flow '${fk.references.flow}' not found in flow $flowName"
          )
        
        case Some(referencedFlow) =>
          // Perform left anti join to find orphan records
          val orphans = currentDf
            .join(
              referencedFlow.select(fk.references.column),
              currentDf(fk.column) === referencedFlow(fk.references.column),
              "left_anti"
            )
          
          if (!orphans.isEmpty) {
            val orphansWithMetadata = ValidationUtils.addRejectionMetadata(
              orphans,
              "FK_VIOLATION",
              s"Foreign key violation in flow $flowName: ${fk.name} (${fk.column} -> ${fk.references.flow}.${fk.references.column})",
              "fk_validation"
            )
            
            rejectedDf = ValidationUtils.combineRejected(rejectedDf, Some(orphansWithMetadata))
            currentDf = currentDf.join(orphans.select(fk.column), Seq(fk.column), "left_anti")
          }
      }
    }
    
    ValidationStepResult(currentDf, rejectedDf)
  }
}
