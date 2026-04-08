package com.etl.framework.validation

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import com.etl.framework.validation.validators.CustomRulesValidator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AlwaysRejectValidator extends Validator {
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val rejected = df
      .withColumn(ValidationColumns.REJECTION_CODE, lit("CUSTOM_ALWAYS_REJECT"))
      .withColumn(ValidationColumns.REJECTION_REASON, lit("always rejected"))
      .withColumn(ValidationColumns.VALIDATION_STEP, lit("custom"))
      .withColumn(ValidationColumns.REJECTED_AT, current_timestamp())
    ValidationStepResult(df.limit(0), Some(rejected))
  }
}

class ValidatorRegistryTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("ValidatorRegistryTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  import spark.implicits._

  private def makeFlowConfig(className: String): FlowConfig = TestFixtures.flowConfig(
    name = "test",
    sourcePath = "data/test.csv",
    rules = Seq(
      ValidationRule(
        `type` = ValidationRuleType.Custom,
        column = Some("value"),
        `class` = Some(className),
        onFailure = OnFailureAction.Reject
      )
    )
  )

  "ValidatorFactory" should "resolve a validator from the registry by name" in {
    val registry = Map("always_reject" -> (() => new AlwaysRejectValidator(): Validator))

    val df = Seq(("1", "a"), ("2", "b")).toDF("id", "value")
    val flowConfig = makeFlowConfig("always_reject")

    val validator = new CustomRulesValidator(flowConfig, None, Some("test"), registry)
    val result = validator.validate(df, ValidationRule(ValidationRuleType.Custom))

    result.valid.count() shouldBe 0
    result.rejected.get.count() shouldBe 2
  }

  it should "fall back to reflection when name is not in registry" in {
    val registry = Map.empty[String, () => Validator]

    val df = Seq(("1", "a"), ("2", "b")).toDF("id", "value")
    val flowConfig = makeFlowConfig("com.etl.framework.validation.AlwaysRejectValidator")

    val validator = new CustomRulesValidator(flowConfig, None, Some("test"), registry)
    val result = validator.validate(df, ValidationRule(ValidationRuleType.Custom))

    result.valid.count() shouldBe 0
    result.rejected.get.count() shouldBe 2
  }

  it should "prefer registry over reflection when name matches" in {
    // Register a validator that passes everything under the name of the reject class
    val passAll = new Validator {
      override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult =
        ValidationStepResult(df, None)
    }
    val registry = Map(
      "com.etl.framework.validation.AlwaysRejectValidator" -> (() => passAll)
    )

    val df = Seq(("1", "a"), ("2", "b")).toDF("id", "value")
    val flowConfig = makeFlowConfig("com.etl.framework.validation.AlwaysRejectValidator")

    val validator = new CustomRulesValidator(flowConfig, None, Some("test"), registry)
    val result = validator.validate(df, ValidationRule(ValidationRuleType.Custom))

    // Registry wins — passAll is used, not the reflection-loaded AlwaysRejectValidator
    result.valid.count() shouldBe 2
    result.rejected shouldBe None
  }
}
