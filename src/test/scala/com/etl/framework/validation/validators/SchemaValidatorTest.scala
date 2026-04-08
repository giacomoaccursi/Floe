package com.etl.framework.validation.validators

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import com.etl.framework.config.ValidationRuleType
import com.etl.framework.config.ValidationRuleType.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

class SchemaValidatorTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("SchemaValidatorTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  // Helper to create basic FlowConfig with Schema
  def createConfig(
      columns: Seq[ColumnConfig],
      enforceSchema: Boolean = true
  ): FlowConfig =
    TestFixtures.flowConfig(
      name = "test_flow",
      enforceSchema = enforceSchema,
      allowExtraColumns = false,
      columns = columns
    )

  "SchemaValidator" should "validate correct schema" in {
    val schema = StructType(
      Seq(
        StructField("id", StringType, false),
        StructField("age", IntegerType, true)
      )
    )
    val data = Seq(Row("1", 25), Row("2", 30))
    val df = spark.createDataFrame(data.asJava, schema)

    val columns = Seq(
      ColumnConfig("id", "string", false, "pk"),
      ColumnConfig("age", "integer", true, "age")
    )

    val config = createConfig(columns)
    val validator = new SchemaValidator(config, Some("test_flow"))

    val result =
      validator.validate(df, ValidationRule(ValidationRuleType.Schema))

    result.rejected match {
      case Some(rejectedDf: org.apache.spark.sql.DataFrame) =>
        rejectedDf.count() shouldBe 0
      case _ => succeed
    }
    result.valid.count() shouldBe 2
  }

  it should "detect missing required columns" in {
    val schema = StructType(
      Seq(
        StructField("id", StringType, false)
        // Missing 'age'
      )
    )
    val data = Seq(Row("1"))
    val df = spark.createDataFrame(data.asJava, schema)

    val columns = Seq(
      ColumnConfig("id", "string", false, "pk"),
      ColumnConfig("age", "integer", true, "age")
    )

    // enforceSchema = true
    val config = createConfig(columns, enforceSchema = true)
    val validator = new SchemaValidator(config, Some("test_flow"))

    val result = validator.validate(df, ValidationRule(Schema))

    // SchemaValidator fails ALL rows if schema is invalid
    result.valid.count() shouldBe 0
    result.rejected.get.count() shouldBe 1
    // Usually rejection explanation is in metadata or log?
    // resultWithRejections puts all rows in rejected? Yes.
  }

  it should "detect extra columns if not allowed" in {
    val schema = StructType(
      Seq(
        StructField("id", StringType, false),
        StructField("extra", StringType, true)
      )
    )
    val data = Seq(Row("1", "val"))
    val df = spark.createDataFrame(data.asJava, schema)

    val columns = Seq(
      ColumnConfig("id", "string", false, "pk")
    )

    // enforceSchema = true, allowExtraColumns false (default in ValidatingUtils?)
    // In createConfig I set allowExtraColumns = false.
    val config = createConfig(columns, enforceSchema = true)
    val validator = new SchemaValidator(config, Some("test_flow"))

    val result = validator.validate(df, ValidationRule(Schema))

    result.valid.count() shouldBe 0
    result.rejected.get.count() shouldBe 1
  }
}
