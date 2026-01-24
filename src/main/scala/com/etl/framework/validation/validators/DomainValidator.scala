package com.etl.framework.validation.validators

import com.etl.framework.config.ValidationRule
import com.etl.framework.validation.{ValidationStepResult, Validator}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Validator for domain value validation
 * Validates that values are in a predefined set of allowed values
 */
class DomainValidator extends Validator {
  
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val column = rule.column.getOrElse(
      throw new ValidationException("Column not specified for domain validation")
    )
    
    val domainName = rule.domainName.getOrElse(
      throw new ValidationException("Domain name not specified for domain validation")
    )
    
    val skipNull = rule.skipNull.getOrElse(true)
    
    // TODO: Load domain values from DomainsConfig
    // For now, we'll use a placeholder implementation
    // In a real implementation, this would load from the domains configuration
    
    // Example: Assume domain values are passed in config
    val domainValues = rule.config
      .flatMap(_.get("values"))
      .map(_.split(",").map(_.trim).toSeq)
      .getOrElse(Seq.empty)
    
    if (domainValues.isEmpty) {
      throw new ValidationException(s"No domain values found for domain: $domainName")
    }
    
    // Build condition for domain validation
    val domainCondition = col(column).isin(domainValues: _*)
    
    val finalCondition = if (skipNull) {
      col(column).isNull || domainCondition
    } else {
      domainCondition
    }
    
    val validDf = df.filter(finalCondition)
    val invalidDf = df.filter(!finalCondition)
    
    if (invalidDf.isEmpty) {
      ValidationStepResult(validDf, None)
    } else {
      val rejectedDf = invalidDf
        .withColumn("_rejection_code", lit("DOMAIN_VALIDATION_FAILED"))
        .withColumn("_rejection_reason", 
          lit(s"Value in column '$column' is not in domain '$domainName'"))
        .withColumn("_validation_step", lit("domain_validation"))
        .withColumn("_rejected_at", current_timestamp())
      
      ValidationStepResult(validDf, Some(rejectedDf))
    }
  }
}
