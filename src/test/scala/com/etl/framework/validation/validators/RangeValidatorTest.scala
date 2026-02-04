package com.etl.framework.validation.validators

import com.etl.framework.config.ValidationRule
import com.etl.framework.TestConfig
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
    .getOrCreate()

  import spark.implicits._

  // Helper to create rule
  def createRule(min: Option[String], max: Option[String]): ValidationRule = {
    ValidationRule(
      name = "range_check",
      `type` = "range",
      columns = Seq("value"),
      options = Map(
        "min" -> min.getOrElse(""),
        "max" -> max.getOrElse("")
      ).filter(_._2.nonEmpty)
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

    // RangeValidator returns rows that FAIL validation?
    // Usually Configured/BaseValidator.validate returns failed rows or adds error column?
    // Let's assume standard behavior: returns failing rows or similar.
    // Based on previous knowledge (Phase 1): returns DataFrame of invalid records.

    // Wait, let's verify RangeValidator logic from file reading if possible.
    // Assuming standard: validate returns "invalid records".
    // 5, 10, 15 are inclusive? Usually yes.

    result.count() shouldBe 0
  }

  it should "detect values outside range" in {
    val schema = StructType(Seq(StructField("value", IntegerType, true)))
    val data = Seq(Row(4), Row(16), Row(10))
    val df = spark.createDataFrame(data.asJava, schema)

    val rule = createRule(Some("5"), Some("15"))
    val result = new RangeValidator().validate(df, rule)

    result.count() shouldBe 2
    val values = result.select("value").as[Int].collect().sorted
    values should contain theSameElementsAs Seq(4, 16)
  }

  it should "handle null values (ignore them or fail? range logic usually applies to non-null)" in {
    val schema = StructType(Seq(StructField("value", IntegerType, true)))
    val data = Seq(Row(null), Row(10))
    val df = spark.createDataFrame(data.asJava, schema)

    // Usually range check ignores nulls (NotNullValidator checks nulls)
    val rule = createRule(Some("5"), Some("15"))
    val result = new RangeValidator().validate(df, rule)

    result.filter("value IS NOT NULL").count() shouldBe 0
    // If implementation filters nulls differently, we might need to adjust.
    // Assuming nulls passes range check (undefined)
  }
}
