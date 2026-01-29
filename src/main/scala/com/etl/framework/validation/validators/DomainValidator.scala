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
class DomainValidator(domainsConfig: Option[DomainsConfig] = None, flowName: Option[String] = None) extends Validator {
  
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val flowContext = flowName.map(f => s" in flow '$f'").getOrElse("")
    
    val column = rule.column.getOrElse {
      throw new IllegalArgumentException(
        s"Domain validation configuration error$flowContext: 'column' field is required.\n")
    }
    
    val domainName = rule.domainName.getOrElse {
      throw new IllegalArgumentException(
        s"Domain validation configuration error for column '$column'$flowContext: 'domainName' field is required.\n"
      )
    }
    
    val skipNull = rule.skipNull.getOrElse(true)
    
    if (domainsConfig.isEmpty) {
      throw new IllegalArgumentException(
        s"DomainsConfig is required for domain validation on column '$column' (domain: '$domainName').\n"
      )
    }
    
    // Load domain values from DomainsConfig
    val domainConfig = domainsConfig.get.domains.get(domainName) match {
      case Some(config) => config
      case None =>
        throw new IllegalArgumentException(
          s"Domain '$domainName' not found in DomainsConfig for column '$column'$flowContext.\n"
        )
    }
    
    val domainValues = domainConfig.values
    
    if (domainValues.isEmpty) {
      throw new ValidationException(
        s"Domain '$domainName' has no values defined for column '$column'$flowContext.\n"
      )
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
          lit(s"Value in column '$column' is not in domain '$domainName' (allowed: ${domainValues.take(3).mkString(", ")}${if (domainValues.size > 3) "..." else ""})"))
        .withColumn("_validation_step", lit("domain_validation"))
        .withColumn("_rejected_at", current_timestamp())
      
      ValidationStepResult(validDf, Some(rejectedDf))
    }
  }
}
