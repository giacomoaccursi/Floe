package com.etl.framework.validation

import com.etl.framework.config._
import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.validation.ValidationColumns._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class CustomValidatorRecord(id: Int, value: String, amount: Double)

class CustomValidatorTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("CustomValidatorTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  import spark.implicits._

  private val sampleRecords = Seq(
    CustomValidatorRecord(1, "hi", 100.0),
    CustomValidatorRecord(2, "verylongvalue", 200.0),
    CustomValidatorRecord(3, "mid", 300.0),
    CustomValidatorRecord(4, "ab", 50.0),
    CustomValidatorRecord(5, "extralongvaluethatexceeds", 500.0)
  )

  private def customRule(
      className: String = "com.etl.framework.validation.TestCustomValidator",
      config: Map[String, String] = Map("threshold" -> "5"),
      column: String = "value"
  ): ValidationRule = ValidationRule(
    `type` = ValidationRuleType.Custom,
    column = Some(column),
    `class` = Some(className),
    config = Some(config),
    description = Some("Test custom validator"),
    skipNull = Some(false),
    onFailure = OnFailureAction.Reject
  )

  "CustomValidator" should "load successfully via reflection" in {
    val rule = customRule()
    val validator = ValidatorFactory.create(rule)
    validator shouldBe a[TestCustomValidator]
  }

  it should "execute validation and return valid/rejected DataFrames" in {
    val df = sampleRecords.toDF()
    val rule = customRule()
    val validator = ValidatorFactory.create(rule)
    val result = validator.validate(df, rule)

    result.valid should not be null
    val validCount = result.valid.count()
    val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)
    (validCount + rejectedCount) shouldBe sampleRecords.size
  }

  it should "read configuration from rule.config in validate" in {
    val df = sampleRecords.toDF()

    // threshold=3: records with value.length > 3 should be rejected
    val rule = customRule(config = Map("threshold" -> "3"))
    val validator = ValidatorFactory.create(rule)
    val result = validator.validate(df, rule)

    val expectedRejected = sampleRecords.count(_.value.length > 3)
    result.rejected.map(_.count()).getOrElse(0L) shouldBe expectedRejected

    // threshold=20: fewer records should be rejected
    val rule2 = customRule(config = Map("threshold" -> "20"))
    val validator2 = ValidatorFactory.create(rule2)
    val result2 = validator2.validate(df, rule2)

    val expectedRejected2 = sampleRecords.count(_.value.length > 20)
    result2.rejected.map(_.count()).getOrElse(0L) shouldBe expectedRejected2
  }

  it should "throw ValidationConfigException for invalid class name" in {
    val rule = customRule(className = "com.invalid.NonExistentValidator")
    intercept[ValidationConfigException] {
      ValidatorFactory.create(rule)
    }
  }

  it should "throw ValidationConfigException for missing class name" in {
    val rule = ValidationRule(
      `type` = ValidationRuleType.Custom,
      column = Some("value"),
      `class` = None,
      skipNull = Some(false),
      onFailure = OnFailureAction.Reject
    )
    intercept[ValidationConfigException] {
      ValidatorFactory.create(rule)
    }
  }

  it should "include rejection metadata on rejected records" in {
    val df = sampleRecords.toDF()
    val rule = customRule(config = Map("threshold" -> "3"))
    val validator = ValidatorFactory.create(rule)
    val result = validator.validate(df, rule)

    result.rejected.isDefined shouldBe true
    val rejected = result.rejected.get
    rejected.count() should be > 0L
    rejected.columns should contain allOf (REJECTION_CODE, REJECTION_REASON, VALIDATION_STEP, REJECTED_AT)
  }

  it should "preserve record count (input = valid + rejected)" in {
    val df = sampleRecords.toDF()
    val rule = customRule()
    val validator = ValidatorFactory.create(rule)
    val result = validator.validate(df, rule)

    val validCount = result.valid.count()
    val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)
    (validCount + rejectedCount) shouldBe sampleRecords.size
  }
}

/** Test validator that rejects records where column length exceeds a threshold from rule.config.
  */
class TestCustomValidator extends Validator {
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val threshold = rule.config.flatMap(_.get("threshold")).map(_.toInt).getOrElse(10)
    val column = rule.column.getOrElse("value")

    val valid = df.filter(length(col(column)) <= threshold)
    val rejected = df
      .filter(length(col(column)) > threshold)
      .withColumn(REJECTION_REASON, lit(s"Value length exceeds threshold $threshold"))
      .withColumn(REJECTION_CODE, lit("CUSTOM_VALIDATION_FAILED"))
      .withColumn(VALIDATION_STEP, lit(s"custom_$column"))
      .withColumn(REJECTED_AT, current_timestamp())

    ValidationStepResult(valid, Some(rejected))
  }
}

/** Test validator that checks prefix/suffix from rule.config.
  */
class TestConfigurableValidator extends Validator {
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val prefix = rule.config.flatMap(_.get("prefix")).getOrElse("")
    val suffix = rule.config.flatMap(_.get("suffix")).getOrElse("")
    val column = rule.column.getOrElse("value")

    var condition = lit(true)
    if (prefix.nonEmpty) condition = condition && col(column).startsWith(prefix)
    if (suffix.nonEmpty) condition = condition && col(column).endsWith(suffix)

    val valid = df.filter(condition)
    val rejected = df
      .filter(!condition)
      .withColumn(REJECTION_REASON, lit("Value doesn't match prefix/suffix pattern"))
      .withColumn(REJECTION_CODE, lit("CUSTOM_VALIDATION_FAILED"))
      .withColumn(VALIDATION_STEP, lit(s"custom_configurable_$column"))
      .withColumn(REJECTED_AT, current_timestamp())

    ValidationStepResult(valid, Some(rejected))
  }
}
