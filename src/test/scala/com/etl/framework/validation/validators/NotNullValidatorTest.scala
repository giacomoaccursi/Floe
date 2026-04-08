package com.etl.framework.validation.validators

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import com.etl.framework.config.ValidationRuleType
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
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  import spark.implicits._

  def createRule(columnName: String): ValidationRule = {
    ValidationRule(
      `type` = ValidationRuleType.NotNull,
      column = Some(columnName)
    )
  }

  def createDummyConfig(columns: Seq[ColumnConfig] = Seq.empty): FlowConfig =
    TestFixtures.flowConfig(
      name = "test_flow",
      primaryKey = Seq.empty,
      enforceSchema = true,
      allowExtraColumns = false,
      columns = columns
    )

  "NotNullValidator" should "validate non-null values" in {
    val schema = StructType(Seq(StructField("id", StringType, true)))
    val data = Seq(Row("1"), Row("2"))
    val df = spark.createDataFrame(data.asJava, schema)

    val columns = Seq(
      ColumnConfig("id", "string", nullable = false, description = "ID column")
    )
    val rule = createRule("id")
    val validator = new NotNullValidator(createDummyConfig(columns))

    val result = validator.validate(df, rule)

    result.rejected.flatMap(df => Some(df.count())).getOrElse(0L) shouldBe 0
    result.valid.count() shouldBe 2
  }

  it should "detect null values" in {
    val schema = StructType(Seq(StructField("id", StringType, true)))
    val data = Seq(Row("1"), Row(null), Row("3"))
    val df = spark.createDataFrame(data.asJava, schema)

    val columns = Seq(
      ColumnConfig("id", "string", nullable = false, description = "ID column")
    )
    val rule = createRule("id")
    val result =
      new NotNullValidator(createDummyConfig(columns)).validate(df, rule)

    result.valid.count() shouldBe 2
    result.valid.select("id").as[String].collect() should contain allOf (
      "1",
      "3"
    )

    val rejected = result.rejected.get
    rejected.count() shouldBe 1
  }

  it should "allow null values in nullable columns" in {
    val schema = StructType(Seq(StructField("id", StringType, true)))
    val data = Seq(Row("1"), Row(null), Row("3"))
    val df = spark.createDataFrame(data.asJava, schema)

    // Mark column as nullable in schema config
    val columns = Seq(
      ColumnConfig("id", "string", nullable = true, description = "ID column")
    )
    val rule = createRule("id")
    val validator = new NotNullValidator(createDummyConfig(columns))

    val result = validator.validate(df, rule)

    // Should pass all records since column is marked nullable
    result.rejected.flatMap(df => Some(df.count())).getOrElse(0L) shouldBe 0
    result.valid.count() shouldBe 3
  }

  it should "validate multiple non-nullable columns" in {
    val schema = StructType(
      Seq(
        StructField("id", StringType, true),
        StructField("name", StringType, true),
        StructField("email", StringType, true)
      )
    )
    val data = Seq(
      Row("1", "Alice", "alice@example.com"),
      Row("2", null, "bob@example.com"), // null name
      Row("3", "Charlie", null), // null email
      Row("4", "Dave", "dave@example.com")
    )
    val df = spark.createDataFrame(data.asJava, schema)

    val columns = Seq(
      ColumnConfig("id", "string", nullable = false, description = "ID"),
      ColumnConfig("name", "string", nullable = false, description = "Name"),
      ColumnConfig("email", "string", nullable = false, description = "Email")
    )
    val rule = createRule("id") // rule parameter is ignored by validator
    val validator = new NotNullValidator(createDummyConfig(columns))

    val result = validator.validate(df, rule)

    // Should reject rows 2 and 3 (with nulls)
    result.valid.count() shouldBe 2
    result.rejected.get.count() shouldBe 2
  }

  it should "pass all records when no non-nullable columns are defined" in {
    val schema = StructType(Seq(StructField("id", StringType, true)))
    val data = Seq(Row("1"), Row(null), Row("3"))
    val df = spark.createDataFrame(data.asJava, schema)

    // Empty columns or all nullable columns
    val rule = createRule("id")
    val validator = new NotNullValidator(createDummyConfig(Seq.empty))

    val result = validator.validate(df, rule)

    // Should pass all records since no non-nullable columns defined
    result.rejected.flatMap(df => Some(df.count())).getOrElse(0L) shouldBe 0
    result.valid.count() shouldBe 3
  }
}
