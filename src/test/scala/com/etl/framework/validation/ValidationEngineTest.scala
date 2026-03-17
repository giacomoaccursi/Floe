package com.etl.framework.validation

import com.etl.framework.config._
import com.etl.framework.validation.ValidationColumns._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

class ValidationEngineTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("ValidationEngineTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  def createFlowConfig(
      name: String = "test_flow",
      schemaFields: Seq[ColumnConfig] = Seq.empty,
      primaryKey: Seq[String] = Seq("id"),
      foreignKeys: Seq[ForeignKeyConfig] = Seq.empty,
      rules: Seq[ValidationRule] = Seq.empty
  ): FlowConfig = {
    FlowConfig(
      name = name,
      description = "Test flow",
      version = "1.0",
      owner = "test",
      source =
        SourceConfig(SourceType.File, "/path", FileFormat.CSV, Map.empty, None),
      schema = SchemaConfig(true, true, schemaFields),
      loadMode = LoadModeConfig(LoadMode.Full),
      validation = ValidationConfig(primaryKey, foreignKeys, rules),
      output = OutputConfig()
    )
  }

  "ValidationEngine" should "initialize DataFrame with warnings column" in {
    val schema = StructType(Seq(StructField("id", StringType, false)))
    val data = Seq(Row("1"), Row("2"))
    val df = spark.createDataFrame(data.asJava, schema)

    val flowConfig = createFlowConfig(
      schemaFields =
        Seq(ColumnConfig("id", "string", nullable = false, description = ""))
    )
    val engine = new ValidationEngine()

    val result = engine.validate(df, flowConfig)

    result.valid.columns should contain(WARNINGS)
  }

  it should "chain multiple validators sequentially" in {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true)
      )
    )
    val data = Seq(
      Row(1, "Alice"),
      Row(null, "Bob"), // fails not_null for id
      Row(3, null) // fails not_null for name
    )
    val df = spark.createDataFrame(data.asJava, schema)

    val flowConfig = createFlowConfig(
      schemaFields = Seq(
        ColumnConfig("id", "integer", nullable = false, description = ""),
        ColumnConfig("name", "string", nullable = false, description = "")
      )
    )
    val engine = new ValidationEngine()

    val result = engine.validate(df, flowConfig)

    // Only valid record should pass both validations
    result.valid.count() shouldBe 1
    result.valid.select("id").as[Int].collect() should contain(1)

    // Two records should be rejected
    result.rejected.isDefined shouldBe true
    result.rejected.get.count() shouldBe 2
  }

  it should "aggregate rejection reasons from multiple validation steps" in {
    val schema = StructType(
      Seq(
        StructField("id", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    val data = Seq(
      Row("1", 25),
      Row("INVALID", 30), // fails schema validation (id should be integer)
      Row(null, 35) // fails not_null validation
    )
    val df = spark.createDataFrame(data.asJava, schema)

    val flowConfig = createFlowConfig(
      schemaFields = Seq(
        ColumnConfig("id", "integer", nullable = false, description = ""),
        ColumnConfig("age", "integer", nullable = false, description = "")
      )
    )
    val engine = new ValidationEngine()

    val result = engine.validate(df, flowConfig)

    // Check that rejectionReasons map contains counts for each step
    result.rejectionReasons should not be empty
    result.rejectionReasons.values.sum shouldBe >(0L)
  }

  it should "combine rejected DataFrames using union" in {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("value", StringType, true)
      )
    )
    val data = Seq(
      Row(1, "valid"),
      Row(null, "invalid1"), // rejected by not_null on id
      Row(2, null) // rejected by not_null on value
    )
    val df = spark.createDataFrame(data.asJava, schema)

    val flowConfig = createFlowConfig(
      schemaFields = Seq(
        ColumnConfig("id", "integer", nullable = false, description = ""),
        ColumnConfig("value", "string", nullable = false, description = "")
      )
    )
    val engine = new ValidationEngine()

    val result = engine.validate(df, flowConfig)

    result.valid.count() shouldBe 1

    // All rejected records should be combined in a single DataFrame
    val rejected = result.rejected.get
    rejected.count() shouldBe 2

    // Rejected DataFrame should have metadata columns
    rejected.columns should contain allOf (
      REJECTION_CODE,
      REJECTION_REASON,
      VALIDATION_STEP,
      REJECTED_AT
    )
  }

  it should "skip validation steps when shouldExecute is false" in {
    val schema = StructType(Seq(StructField("id", StringType, false)))
    val data = Seq(Row("1"), Row("2"))
    val df = spark.createDataFrame(data.asJava, schema)

    // Config without foreign keys or custom rules — these should be skipped
    val flowConfig = createFlowConfig(
      schemaFields =
        Seq(ColumnConfig("id", "string", nullable = false, description = "")),
      foreignKeys = Seq.empty, // FK validation should be skipped
      rules = Seq.empty // Custom rules validation should be skipped
    )
    val engine = new ValidationEngine()

    val result = engine.validate(df, flowConfig)

    // Should execute schema, not_null, and pk validation — all pass
    result.valid.count() shouldBe 2
    result.rejected shouldBe None
  }

  it should "propagate state correctly through validation chain" in {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true)
      )
    )
    val data = Seq(
      Row(1, "Alice"),
      Row(2, "Bob"),
      Row(null, "Charlie") // Should be rejected early by not_null
    )
    val df = spark.createDataFrame(data.asJava, schema)

    val flowConfig = createFlowConfig(
      schemaFields = Seq(
        ColumnConfig("id", "integer", nullable = false, description = ""),
        ColumnConfig("name", "string", nullable = false, description = "")
      ),
      primaryKey = Seq("id") // PK validation should only run on valid records
    )
    val engine = new ValidationEngine()

    val result = engine.validate(df, flowConfig)

    // Valid records should pass through all validations
    result.valid.count() shouldBe 2

    // Invalid record should be in rejected
    result.rejected.isDefined shouldBe true
    result.rejected.get.count() shouldBe 1
  }

  it should "handle empty rejected DataFrames correctly" in {
    val schema = StructType(Seq(StructField("id", StringType, false)))
    val data = Seq(Row("1"), Row("2"), Row("3"))
    val df = spark.createDataFrame(data.asJava, schema)

    val flowConfig = createFlowConfig(
      schemaFields =
        Seq(ColumnConfig("id", "string", nullable = false, description = ""))
    )
    val engine = new ValidationEngine()

    val result = engine.validate(df, flowConfig)

    // All records are valid
    result.valid.count() shouldBe 3

    // No rejected records
    result.rejected shouldBe None
    result.rejectionReasons shouldBe empty
  }

  it should "accumulate rejection reasons across multiple steps" in {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    val data = Seq(
      Row(1, "Alice", 25),
      Row(null, "Bob", 30), // rejected by not_null (id)
      Row(2, null, 35), // rejected by not_null (name)
      Row(3, "Charlie", null) // rejected by not_null (age)
    )
    val df = spark.createDataFrame(data.asJava, schema)

    val flowConfig = createFlowConfig(
      schemaFields = Seq(
        ColumnConfig("id", "integer", nullable = false, description = ""),
        ColumnConfig("name", "string", nullable = false, description = ""),
        ColumnConfig("age", "integer", nullable = false, description = "")
      )
    )
    val engine = new ValidationEngine()

    val result = engine.validate(df, flowConfig)

    result.valid.count() shouldBe 1
    result.rejected.isDefined shouldBe true
    result.rejected.get.count() shouldBe 3

    // RejectionReasons should track counts
    result.rejectionReasons.values.sum shouldBe 3
  }

  // --- Gap #8: multi-validator rejection counting ---

  it should "count each rejected record in exactly one step (no double-counting)" in {
    // Records:
    //   id=1  name="Alice"  → valid in all steps
    //   id=2  name="Bob"    → has duplicate PK → rejected by pk_validation
    //   id=2  name="Carol"  → has duplicate PK → rejected by pk_validation
    //   id=null name="Dave" → null id           → rejected by not_null
    //
    // Dave is removed by not_null BEFORE pk_validation runs, so it is counted
    // only in not_null_validation.
    // Bob and Carol both have id=2 → the PK validator rejects ALL duplicates
    // (both occurrences), so pk_validation contributes 2 to the count.
    //
    // Invariant: rejectionReasons.values.sum == rejected.count()
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true)
      )
    )
    val data = Seq(
      Row(1, "Alice"),
      Row(2, "Bob"),
      Row(2, "Carol"),  // duplicate PK — pk_validation rejects both duplicates
      Row(null, "Dave") // null id       — rejected by not_null before pk runs
    )
    val df = spark.createDataFrame(data.asJava, schema)

    val flowConfig = createFlowConfig(
      schemaFields = Seq(
        ColumnConfig("id", "integer", nullable = false, description = ""),
        ColumnConfig("name", "string", nullable = true, description = "")
      ),
      primaryKey = Seq("id")
    )
    val engine = new ValidationEngine()

    val result = engine.validate(df, flowConfig)

    // 1 (Dave, not_null) + 2 (Bob+Carol, pk) = 3 total rejected
    val totalRejected = result.rejected.map(_.count()).getOrElse(0L)
    totalRejected shouldBe 3

    // Core invariant: rejectionReasons.values.sum == rejected.count().
    // Each rejected record is attributed to exactly one validation step —
    // no double-counting across steps.
    result.rejectionReasons.values.sum shouldBe totalRejected

    // not_null_validation captured the null-id record only
    result.rejectionReasons should contain key "not_null_validation"
    result.rejectionReasons("not_null_validation") shouldBe 1

    // pk_validation captured both duplicate-PK records
    result.rejectionReasons should contain key "pk_validation"
    result.rejectionReasons("pk_validation") shouldBe 2
  }
}
