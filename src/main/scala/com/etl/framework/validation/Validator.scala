package com.etl.framework.validation

import com.etl.framework.config.{DomainsConfig, ValidationRule}
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
   * @return Validator instance
   */
  def create(rule: ValidationRule, domainsConfig: Option[DomainsConfig] = None): Validator = {
    rule.`type` match {
      case "regex" => new RegexValidator()
      case "range" => new RangeValidator()
      case "domain" => new DomainValidator(domainsConfig)
      case "custom" => createCustomValidator(rule)
      case unsupported =>
        throw new UnsupportedValidatorException(
          s"Unsupported validator type: $unsupported"
        )
    }
  }

  /**
   * Creates a custom validator via reflection
   */
  private def createCustomValidator(rule: ValidationRule): Validator = {
    val className = rule.`class`.getOrElse(
      throw new CustomValidatorException("Custom validator class not specified")
    )

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
        throw new CustomValidatorException(s"Custom validator class not found: $className", e)
      case e: Exception =>
        throw new CustomValidatorException(s"Failed to instantiate custom validator: $className", e)
    }
  }
}

/**
 * Trait for validators that can be configured
 */
trait ConfigurableValidator {
  def configure(config: Map[String, String]): Unit
}

/**
 * Exception thrown when an unsupported validator type is encountered
 */
class UnsupportedValidatorException(message: String) extends RuntimeException(message)

/**
 * Exception thrown when custom validator fails
 */
class CustomValidatorException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)
