package com.etl.framework.validation.validators

import com.etl.framework.config.{DomainsConfig, ValidationRule}
import com.etl.framework.validation.{ValidationStepResult, Validator}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Validator for domain value validation
 * Validates that values are in a predefined set of allowed values
 * Requires DomainsConfig to be provided - no inline values supported
 */
class DomainValidator(domainsConfig: Option[DomainsConfig] = None) extends Validator {
  
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val column = rule.column.getOrElse(
      throw new IllegalArgumentException("Column not specified for domain validation")
    )
    
    val domainName = rule.domainName.getOrElse(
      throw new IllegalArgumentException("Domain name not specified for domain validation")
    )
    
    val skipNull = rule.skipNull.getOrElse(true)
    
    if (domainsConfig.isEmpty) {
      throw new IllegalArgumentException(
        s"DomainsConfig is required for domain validation. " +
        s"Ensure domains.yaml is loaded and passed to ValidationEngine."
      )
    }
    
    // Load domain values from DomainsConfig
    val domainConfig = domainsConfig.get.domains.get(domainName).getOrElse {
      throw new IllegalArgumentException(
        s"Domain '$domainName' not found in DomainsConfig. " +
        s"Available domains: ${domainsConfig.get.domains.keys.mkString(", ")}"
      )
    }


    
    val domainValues = domainConfig.values
    
    if (domainValues.isEmpty) {
      throw new IllegalArgumentException(s"Domain '$domainName' has no values defined")
    }
    
    val caseSensitive = domainConfig.caseSensitive
    
    // Build condition for domain validation
    val domainCondition = if (caseSensitive) {
      col(column).isin(domainValues: _*)
    } else {
      // Case-insensitive comparison
      val lowerValues = domainValues.map(_.toLowerCase)
      lower(col(column)).isin(lowerValues: _*)
    }
    
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
          lit(s"Value in column '$column' is not in domain '$domainName' (allowed: ${domainValues.take(5).mkString(", ")}${if (domainValues.size > 5) "..." else ""})"))
        .withColumn("_validation_step", lit("domain_validation"))
        .withColumn("_rejected_at", current_timestamp())
      
      ValidationStepResult(validDf, Some(rejectedDf))
    }
  }
}
