package com.etl.framework.validation.validators

import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.config.{DomainsConfig, ValidationRule}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

/** Validator for domain value validation Validates that values are in a predefined set of allowed values Requires
  * DomainsConfig to be provided - no inline values supported
  */
class DomainValidator(
    domainsConfig: Option[DomainsConfig] = None,
    flowName: Option[String] = None
) extends BaseValidator(flowName) {

  override protected def validatorName: String = "Domain"

  override protected def rejectionCode: String = "DOMAIN_VALIDATION_FAILED"

  override protected def validationStep: String = "domain_validation"

  override protected def rejectionReason(rule: ValidationRule, column: String): String = {
    val domainName = rule.domainName.getOrElse("unknown")
    s"Value in column '$column' is not in domain '$domainName'"
  }

  override protected def buildValidationCondition(
      df: DataFrame,
      rule: ValidationRule,
      column: String
  ): Column = {
    val domainName = rule.domainName.getOrElse {
      throw ValidationConfigException(
        s"Domain validation error for column '$column'${flowContext}: 'domainName' field is required"
      )
    }

    // Validate DomainsConfig is provided
    if (domainsConfig.isEmpty) {
      throw ValidationConfigException(
        s"DomainsConfig required for domain validation on column '$column' (domain: '$domainName')${flowContext}"
      )
    }

    // Load domain configuration
    val domainConfig = domainsConfig.get.domains.get(domainName).getOrElse {
      throw ValidationConfigException(
        s"Domain '$domainName' not found for column '$column'${flowContext}. Check your domains.yaml."
      )
    }

    // Validate domain has values
    val domainValues = domainConfig.values
    if (domainValues.isEmpty) {
      throw ValidationConfigException(
        s"Domain '$domainName' has no values defined for column '$column'${flowContext}"
      )
    }

    // Build condition based on case sensitivity
    val caseSensitive = domainConfig.caseSensitive
    if (caseSensitive) {
      col(column).isin(domainValues: _*)
    } else {
      val lowerValues = domainValues.map(_.toLowerCase)
      lower(col(column)).isin(lowerValues: _*)
    }
  }
}
