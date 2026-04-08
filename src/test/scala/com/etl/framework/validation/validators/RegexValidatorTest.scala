package com.etl.framework.validation.validators

import com.etl.framework.config.{ValidationRule, ValidationRuleType}
import com.etl.framework.exceptions.ValidationConfigException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

class RegexValidatorTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("RegexValidatorTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  import spark.implicits._

  def createRule(pattern: String): ValidationRule = {
    ValidationRule(
      `type` = ValidationRuleType.Regex,
      column = Some("email"),
      pattern = Some(pattern)
    )
  }

  "RegexValidator" should "validate strings matching pattern" in {
    val schema = StructType(Seq(StructField("email", StringType, true)))
    val data = Seq(
      Row("user@example.com"),
      Row("valid-email@test.co.uk")
    )
    val df = spark.createDataFrame(data.asJava, schema)

    // Simple email regex
    val rule = createRule("^[^@]+@[^@]+\\.[^@]+$")
    val validator = new RegexValidator()

    val result = validator.validate(df, rule)

    result.rejected.flatMap(df => Some(df.count())).getOrElse(0L) shouldBe 0
    result.valid.count() shouldBe 2
  }

  it should "detect strings not matching pattern" in {
    val schema = StructType(Seq(StructField("email", StringType, true)))
    val data = Seq(
      Row("user@example.com"),
      Row("invalid-email"),
      Row("missing-at.com")
    )
    val df = spark.createDataFrame(data.asJava, schema)

    val rule = createRule("^[^@]+@[^@]+\\.[^@]+$")
    val result = new RegexValidator().validate(df, rule)

    result.valid.count() shouldBe 1
    result.valid.select("email").as[String].collect() should contain(
      "user@example.com"
    )

    val rejected = result.rejected.get
    rejected.count() shouldBe 2
    rejected.select("email").as[String].collect() should contain allOf (
      "invalid-email",
      "missing-at.com"
    )
  }

  it should "throw exception if pattern is missing" in {
    val rule =
      ValidationRule(
        `type` = ValidationRuleType.Regex,
        column = Some("email"),
        pattern = None
      )
    val df = spark.createDataFrame(
      Seq(Row("test")).asJava,
      StructType(Seq(StructField("email", StringType, true)))
    )

    assertThrows[ValidationConfigException] {
      new RegexValidator().validate(df, rule)
    }
  }
}
