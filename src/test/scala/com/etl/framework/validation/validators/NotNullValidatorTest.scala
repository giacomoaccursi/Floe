package com.etl.framework.validation.validators

import com.etl.framework.config.ValidationRule
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

class NotNullValidatorTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("NotNullValidatorTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  def createRule(columnName: String): ValidationRule = {
    // Assuming NotNull rule structure. Often applies to multiple columns or single 'column' in generic rule.
    // If NotNullValidator processes generic rule with 'column'.
    ValidationRule(
      `type` = "not_null",
      column = Some(columnName)
    )
  }

  "NotNullValidator" should "validate non-null values" in {
    val schema = StructType(Seq(StructField("id", StringType, true)))
    val data = Seq(Row("1"), Row("2"))
    val df = spark.createDataFrame(data.asJava, schema)

    val rule = createRule("id")
    val validator = new NotNullValidator()

    val result = validator.validate(df, rule)

    result.rejected.flatMap(df => Some(df.count())).getOrElse(0L) shouldBe 0
    result.valid.count() shouldBe 2
  }

  it should "detect null values" in {
    val schema = StructType(Seq(StructField("id", StringType, true)))
    val data = Seq(Row("1"), Row(null), Row("3"))
    val df = spark.createDataFrame(data.asJava, schema)

    val rule = createRule("id")
    val result = new NotNullValidator().validate(df, rule)

    result.valid.count() shouldBe 2
    result.valid.select("id").as[String].collect() should contain allOf (
      "1",
      "3"
    )

    val rejected = result.rejected.get
    rejected.count() shouldBe 1
    // row with null id
  }
}
