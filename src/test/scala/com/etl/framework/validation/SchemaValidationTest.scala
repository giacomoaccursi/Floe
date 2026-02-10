package com.etl.framework.validation

import com.etl.framework.config._
import com.etl.framework.validation.ValidationColumns._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => f}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.collection.JavaConverters._

class SchemaValidationTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("SchemaValidationTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  def createFlowConfig(
      schemaFields: Seq[ColumnConfig] = Seq.empty,
      enforceSchema: Boolean = true,
      allowExtraColumns: Boolean = false
  ): FlowConfig = {
    FlowConfig(
      name = "test_flow",
      description = "Test flow",
      version = "1.0",
      owner = "test",
      source =
        SourceConfig(SourceType.File, "/path", FileFormat.CSV, Map.empty, None),
      schema = SchemaConfig(enforceSchema, allowExtraColumns, schemaFields),
      loadMode = LoadModeConfig(LoadMode.Full),
      validation = ValidationConfig(Seq("id"), Seq.empty, Seq.empty),
      output = OutputConfig()
    )
  }

  val standardColumns: Seq[ColumnConfig] = Seq(
    ColumnConfig("id", "string", nullable = true, description = ""),
    ColumnConfig("name", "string", nullable = true, description = ""),
    ColumnConfig("age", "string", nullable = true, description = "")
  )

  def createDf(columns: Seq[String], rowCount: Int): org.apache.spark.sql.DataFrame = {
    val rows = (1 to rowCount).map { i =>
      Row.fromSeq(columns.map(c => s"val_${i}_$c"))
    }
    val schema = StructType(columns.map(c => StructField(c, StringType, nullable = true)))
    spark.createDataFrame(rows.asJava, schema)
  }

  "SchemaValidation" should "reject all records when a required column is missing" in {
    val df = createDf(Seq("id", "name"), 5) // missing "age"
    val flowConfig = createFlowConfig(schemaFields = standardColumns)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    result.valid.count() shouldBe 0
    result.rejected.isDefined shouldBe true
    result.rejected.get.count() shouldBe 5
  }

  it should "pass all records when all required columns are present" in {
    val df = createDf(Seq("id", "name", "age"), 3)
    val flowConfig = createFlowConfig(schemaFields = standardColumns)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    result.valid.count() shouldBe 3
    result.rejected shouldBe None
    result.valid.columns should contain(WARNINGS)
  }

  it should "reject all records when multiple required columns are missing" in {
    val df = createDf(Seq("id"), 4) // missing "name" and "age"
    val flowConfig = createFlowConfig(schemaFields = standardColumns)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    result.valid.count() shouldBe 0
    result.rejected.isDefined shouldBe true
    result.rejected.get.count() shouldBe 4

    val reasons = result.rejected.get.select(REJECTION_REASON).distinct().collect()
    reasons.foreach { row =>
      row.getString(0) should include("Missing required columns")
    }
  }

  it should "allow extra columns when configured" in {
    val df = createDf(Seq("id", "name", "age", "extra_col_1", "extra_col_2"), 3)
    val flowConfig = createFlowConfig(
      schemaFields = standardColumns,
      allowExtraColumns = true
    )

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    val schemaRejections = result.rejected.map { rejectedDf =>
      rejectedDf.filter(f.col(REJECTION_CODE) === "SCHEMA_VALIDATION_FAILED").count()
    }.getOrElse(0L)

    schemaRejections shouldBe 0
  }

  it should "detect missing columns even on empty DataFrame" in {
    val df = createDf(Seq("id", "name"), 0) // 0 rows, missing "age"
    val flowConfig = createFlowConfig(schemaFields = standardColumns)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    result.valid.count() shouldBe 0
    result.rejected.isDefined shouldBe true
  }

  it should "include complete rejection metadata on schema failure" in {
    val df = createDf(Seq("id", "name"), 3) // missing "age"
    val flowConfig = createFlowConfig(schemaFields = standardColumns)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    val rejected = result.rejected.get
    rejected.columns should contain allOf (
      REJECTION_CODE,
      REJECTION_REASON,
      VALIDATION_STEP,
      REJECTED_AT
    )

    rejected.select(REJECTION_CODE).distinct().collect().map(_.getString(0)) should contain(
      "SCHEMA_VALIDATION_FAILED"
    )
  }

  it should "skip schema validation when enforceSchema is false" in {
    val df = createDf(Seq("id", "name"), 3) // missing "age"
    val flowConfig = createFlowConfig(
      schemaFields = standardColumns.map(_.copy(nullable = true)),
      enforceSchema = false
    )

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    val schemaRejections = result.rejected.map { rejectedDf =>
      rejectedDf.filter(f.col(REJECTION_CODE) === "SCHEMA_VALIDATION_FAILED").count()
    }.getOrElse(0L)

    schemaRejections shouldBe 0
  }

  it should "preserve record count invariant (input = valid + rejected)" in {
    val df = createDf(Seq("id", "name"), 10) // missing "age"
    val flowConfig = createFlowConfig(schemaFields = standardColumns)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    val validCount = result.valid.count()
    val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)

    (validCount + rejectedCount) shouldBe 10
  }
}
