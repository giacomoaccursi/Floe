package com.etl.framework.validation

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import com.etl.framework.validation.validators.CustomRulesValidator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A custom validator that rejects all rows — used to verify skipNull behavior. If skipNull works, NULL rows should NOT
  * be passed to this validator and should survive.
  */
class RejectAllValidator extends Validator {
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val rejected = df
      .withColumn(ValidationColumns.REJECTION_CODE, lit("CUSTOM_REJECT"))
      .withColumn(ValidationColumns.REJECTION_REASON, lit("rejected by custom"))
      .withColumn(ValidationColumns.VALIDATION_STEP, lit("custom"))
      .withColumn(ValidationColumns.REJECTED_AT, current_timestamp())
    ValidationStepResult(df.limit(0), Some(rejected))
  }
}

class CustomValidatorSkipNullTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("CustomValidatorSkipNullTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  import spark.implicits._

  private def makeFlowConfig(skipNull: Boolean): FlowConfig = TestFixtures.flowConfig(
    name = "test",
    sourcePath = "data/test.csv",
    rules = Seq(
      ValidationRule(
        `type` = ValidationRuleType.Custom,
        column = Some("value"),
        `class` = Some("com.etl.framework.validation.RejectAllValidator"),
        skipNull = Some(skipNull),
        onFailure = OnFailureAction.Reject
      )
    )
  )

  "CustomRulesValidator" should "pass NULL rows through when skipNull is true for custom validators" in {
    val df = Seq(
      ("1", Some("a")),
      ("2", Option.empty[String]),
      ("3", Some("b"))
    ).toDF("id", "value")

    val flowConfig = makeFlowConfig(skipNull = true)
    val validator = new CustomRulesValidator(flowConfig, None, Some("test"))
    val result = validator.validate(df, ValidationRule(ValidationRuleType.Custom))

    // Row "2" has NULL value — with skipNull=true it should NOT be rejected
    result.valid.count() shouldBe 1 // the NULL row survives
    result.rejected.get.count() shouldBe 2 // only non-NULL rows rejected
  }

  it should "reject NULL rows when skipNull is false for custom validators" in {
    val df = Seq(
      ("1", Some("a")),
      ("2", Option.empty[String]),
      ("3", Some("b"))
    ).toDF("id", "value")

    val flowConfig = makeFlowConfig(skipNull = false)
    val validator = new CustomRulesValidator(flowConfig, None, Some("test"))
    val result = validator.validate(df, ValidationRule(ValidationRuleType.Custom))

    // With skipNull=false, all rows go to the custom validator which rejects everything
    result.valid.count() shouldBe 0
    result.rejected.get.count() shouldBe 3
  }
}
