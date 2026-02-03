package com.etl.framework.validation

import com.etl.framework.config.{DomainsConfig, ValidationRule}
import com.etl.framework.exceptions.{UnsupportedOperationException, ValidationConfigException}
import com.etl.framework.validation.validators.{DomainValidator, RangeValidator, RegexValidator}
import org.apache.spark.sql.DataFrame

/**
 * Trait for custom validators
 */
trait Validator {
  /**
   * Validates a DataFrame according to the rule
   * 
   * @param df DataFrame to validate
   * @param rule Validation rule configuration
   * @return ValidationStepResult with valid and rejected records
   */
  def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult
}

/**
 * Factory for creating Validator instances
 */
object ValidatorFactory {

  /**
   * Creates a Validator instance based on the rule type
   *
   * @param rule Validation rule configuration
   * @param domainsConfig Optional domains configuration for domain validation
   * @param flowName Optional flow name for better error messages
   * @return Validator instance
   */
  def create(
    rule: ValidationRule, 
    domainsConfig: Option[DomainsConfig] = None,
    flowName: Option[String] = None
  ): Validator = {
    rule.`type` match {
      case "regex" => new RegexValidator(flowName)
      case "range" => new RangeValidator(flowName)
      case "domain" => new DomainValidator(domainsConfig, flowName)
      case "custom" => createCustomValidator(rule, flowName)
      case unsupported =>
        val flowContext = flowName.map(f => s" in flow '$f'").getOrElse("")
        throw UnsupportedOperationException(
          operation = s"validator type '$unsupported'",
          details = s"Supported validators: regex, range, domain, custom$flowContext"
        )
    }
  }

  /**
   * Creates a custom validator via reflection
   */
  private def createCustomValidator(rule: ValidationRule, flowName: Option[String]): Validator = {
    val flowContext = flowName.map(f => s" in flow '$f'").getOrElse("")
    
    val className = rule.`class`.getOrElse {
      throw ValidationConfigException(
        s"Custom validator error$flowContext: 'class' field is required"
      )
    }

    try {
      val clazz = Class.forName(className)
      val instance = clazz.getDeclaredConstructor().newInstance().asInstanceOf[Validator]

      // Configure the validator if it implements Configurable
      instance match {
        case configurable: ConfigurableValidator =>
          configurable.configure(rule.config.getOrElse(Map.empty))
        case _ => // No configuration needed
      }

      instance
    } catch {
      case e: ClassNotFoundException =>
        throw ValidationConfigException(s"Custom validator class not found: $className", e)
      case e: Exception =>
        throw ValidationConfigException(s"Failed to instantiate custom validator: $className", e)
    }
  }
}

/**
 * Trait for validators that can be configured
 */
trait ConfigurableValidator {
  def configure(config: Map[String, String]): Unit
}
