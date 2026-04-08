package com.etl.framework.validation.validators

import com.etl.framework.config.{ValidationRule, ValidationRuleType}

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

  it should "lose null rows when called directly (skipNull is handled by CustomRulesValidator)" in {
    val schema = StructType(Seq(StructField("value", IntegerType, true)))
    val data = Seq(Row(null), Row(10))
    val df = spark.createDataFrame(data.asJava, schema)

    val rule = createRule(Some("5"), Some("15"))
    val result = new RangeValidator().validate(df, rule)

    // When called directly, NULL values produce null for both condition and !condition.
    // Spark treats null as false in filter, so NULL rows end up in neither valid nor rejected.
    // This is expected — skipNull is handled by CustomRulesValidator, not by individual validators.
    result.valid.count() shouldBe 1 // only the non-null row (10) passes
  }
}
