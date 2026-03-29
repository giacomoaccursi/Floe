package com.etl.framework.validation

import com.etl.framework.config.{DomainsConfig, ValidationRule, ValidationRuleType}
import com.etl.framework.exceptions.{UnsupportedOperationException, ValidationConfigException}
import com.etl.framework.validation.validators.{DomainValidator, RangeValidator, RegexValidator}
import org.apache.spark.sql.DataFrame

/** Trait for custom validators
  */
trait Validator {

  /** Validates a DataFrame according to the rule
    *
    * @param df
    *   DataFrame to validate
    * @param rule
    *   Validation rule configuration
    * @return
    *   ValidationStepResult with valid and rejected records
    */
  def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult
}

/** Factory for creating Validator instances
  */
object ValidatorFactory {

  /** Creates a Validator instance based on the rule type
    *
    * @param rule
    *   Validation rule configuration
    * @param domainsConfig
    *   Optional domains configuration for domain validation
    * @param flowName
    *   Optional flow name for better error messages
    * @return
    *   Validator instance
    */
  def create(
      rule: ValidationRule,
      domainsConfig: Option[DomainsConfig] = None,
      flowName: Option[String] = None,
      customValidators: Map[String, () => Validator] = Map.empty
  ): Validator = {
    rule.`type` match {
      case ValidationRuleType.Regex => new RegexValidator(flowName)
      case ValidationRuleType.Range => new RangeValidator(flowName)
      case ValidationRuleType.Domain =>
        new DomainValidator(domainsConfig, flowName)
      case ValidationRuleType.Custom => resolveCustomValidator(rule, flowName, customValidators)
      case unsupported =>
        val flowContext = flowName.map(f => s" in flow '$f'").getOrElse("")
        throw UnsupportedOperationException(
          operation = s"validator type '${unsupported.name}'",
          details = s"Supported validators: regex, range, domain, custom$flowContext"
        )
    }
  }

  /** Resolves a custom validator: tries the registry first, falls back to reflection.
    */
  private def resolveCustomValidator(
      rule: ValidationRule,
      flowName: Option[String],
      customValidators: Map[String, () => Validator]
  ): Validator = {
    val flowContext = flowName.map(f => s" in flow '$f'").getOrElse("")
    val name = rule.`class`.getOrElse {
      throw ValidationConfigException(
        s"Custom validator error$flowContext: 'class' field is required"
      )
    }

    // Try registry by name first, fall back to reflection
    customValidators.get(name) match {
      case Some(factory) =>
        factory()
      case None =>
        createCustomValidator(rule, flowName)
    }
  }

  /** Creates a custom validator via reflection
    */
  private def createCustomValidator(
      rule: ValidationRule,
      flowName: Option[String]
  ): Validator = {
    val flowContext = flowName.map(f => s" in flow '$f'").getOrElse("")

    val className = rule.`class`.getOrElse {
      throw ValidationConfigException(
        s"Custom validator error$flowContext: 'class' field is required"
      )
    }

    try {
      val clazz = Class.forName(className)
      val instance = clazz.getDeclaredConstructor().newInstance()

      // Type-safe check
      instance match {
        case validator: Validator =>
          validator
        case other =>
          throw ValidationConfigException(
            s"Class $className does not implement Validator trait. Got: ${other.getClass.getName}"
          )
      }
    } catch {
      case e: ClassNotFoundException =>
        throw ValidationConfigException(
          s"Custom validator class not found: $className",
          e
        )
      case e: ValidationConfigException => throw e
      case e: Exception =>
        throw ValidationConfigException(
          s"Failed to instantiate custom validator: $className",
          e
        )
    }
  }
}
