package com.etl.framework.validation.validators

import com.etl.framework.config.{ValidationRule, ValidationRuleType}
import com.etl.framework.exceptions.ValidationConfigException

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

class RangeValidatorTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("RangeValidatorTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  import spark.implicits._

  // Helper to create rule
  def createRule(min: Option[String], max: Option[String]): ValidationRule = {
    ValidationRule(
      `type` = ValidationRuleType.Range,
      column = Some("value"),
      min = min,
      max = max
    )
  }

  "RangeValidator" should "validate values within range" in {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("value", IntegerType, true)
      )
    )

    val data = Seq(
      Row(1, 5),
      Row(2, 10),
      Row(3, 15)
    )

    val df = spark.createDataFrame(data.asJava, schema)
    val rule = createRule(Some("5"), Some("15"))
    val validator = new RangeValidator()

    val result = validator.validate(df, rule)

    // valid records should remain in result.valid
    // invalid records go to result.rejected
    // Since all are valid (5, 10, 15 are in [5, 15]), rejected should be empty (None or empty DF)

    result.rejected match {
      case Some(rejectedDf) => rejectedDf.count() shouldBe 0
      case None             => succeed // No rejection DataFrame created implies success
    }
    result.valid.count() shouldBe 3
  }

  it should "detect values outside range" in {
    val schema = StructType(Seq(StructField("value", IntegerType, true)))
    val data = Seq(Row(4), Row(16), Row(10))
    val df = spark.createDataFrame(data.asJava, schema)

    val rule = createRule(Some("5"), Some("15"))
    val result = new RangeValidator().validate(df, rule)

    // 4 and 16 are rejected, 10 is kept
    result.valid.count() shouldBe 1
    result.valid.select("value").as[Int].collect() should contain(10)

    val rejected = result.rejected.get
    rejected.count() shouldBe 2

    // Usually rejected DF has original columns + error info? Or just original?
    // BaseValidator usually splits.
    // Let's check values in rejected if simple schema
    rejected.select("value").as[Int].collect() should contain allOf (4, 16)
  }

  it should "classify null conditions as invalid instead of dropping rows" in {
    val schema = StructType(Seq(StructField("value", IntegerType, true)))
    val data = Seq(Row(null), Row(10))
    val df = spark.createDataFrame(data.asJava, schema)

    val rule = createRule(Some("5"), Some("15"))
    val result = new RangeValidator().validate(df, rule)

    result.valid.count() shouldBe 1
    result.rejected.get.count() shouldBe 1
    result.rejected.get.filter("value IS NULL").count() shouldBe 1
  }

  it should "report a typed configuration error for a non-numeric bound" in {
    val df = Seq(10).toDF("value")
    val rule = createRule(Some("not-a-number"), Some("15"))

    val error = intercept[ValidationConfigException] {
      new RangeValidator().validate(df, rule)
    }

    error.getMessage should include("'min' value 'not-a-number' is not numeric")
  }

  it should "reject non-finite numeric bounds" in {
    val df = Seq(10).toDF("value")

    Seq("NaN", "Infinity", "-Infinity").foreach { bound =>
      val error = intercept[ValidationConfigException] {
        new RangeValidator().validate(df, createRule(Some(bound), None))
      }

      error.getMessage should include(s"'min' value '$bound' must be finite")
    }
  }

  it should "compare integral values without losing precision" in {
    val firstValueOutsideDoubleIntegerPrecision = 9007199254740993L
    val df = Seq(firstValueOutsideDoubleIntegerPrecision).toDF("value")
    val rule = createRule(None, Some("9007199254740992"))

    val result = new RangeValidator().validate(df, rule)

    result.valid.count() shouldBe 0
    result.rejected.get.select("value").as[Long].collect() should contain only firstValueOutsideDoubleIntegerPrecision
  }

  it should "compare numeric strings without losing decimal precision" in {
    val valueAboveMaximum = "0.12345678901234567892"
    val df = Seq(valueAboveMaximum).toDF("value")
    val rule = createRule(None, Some("0.12345678901234567891"))

    val result = new RangeValidator().validate(df, rule)

    result.valid.count() shouldBe 0
    result.rejected.get.select("value").as[String].collect() should contain only valueAboveMaximum
  }

  it should "reject bounds that exceed Spark decimal precision" in {
    val df = Seq(10).toDF("value")
    val boundWith39Digits = "123456789012345678901234567890123456789"

    val error = intercept[ValidationConfigException] {
      new RangeValidator().validate(df, createRule(Some(boundWith39Digits), None))
    }

    error.getMessage should include("exceeds Spark's maximum decimal precision of 38 digits")
  }
}
