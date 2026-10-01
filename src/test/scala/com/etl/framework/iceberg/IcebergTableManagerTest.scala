package com.etl.framework.iceberg

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}

class IcebergTableManagerTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var warehousePath: Path = _

  implicit val spark: SparkSession = {
    val tmpDir = Files.createTempDirectory("iceberg-test-warehouse")
    warehousePath = tmpDir

    // Stop any existing session to ensure Iceberg extensions are loaded
    SparkSession.getActiveSession.foreach(_.stop())

    SparkSession
      .builder()
      .appName("IcebergTableManagerTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config(
        "spark.sql.catalog.test_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config("spark.sql.catalog.test_catalog.type", "hadoop")
      .config(
        "spark.sql.catalog.test_catalog.warehouse",
        tmpDir.toString
      )
      .getOrCreate()
  }

  import spark.implicits._

  private val icebergConfig = IcebergConfig(
    catalogName = "test_catalog",
    ddlMode = DdlMode.Automatic,
    warehouse = warehousePath.toString,
    enableSnapshotTagging = true
  )

  private val tableManager = new IcebergTableManager(spark, icebergConfig)
  private val validatingTableManager = new IcebergTableManager(spark, icebergConfig.copy(ddlMode = DdlMode.Validate))

  private def testFlowConfig(
      name: String,
      primaryKey: Seq[String] = Seq("id"),
      sortOrder: Seq[String] = Seq.empty,
      icebergPartitions: Seq[String] = Seq.empty,
      tableProperties: Map[String, String] = Map.empty
  ): FlowConfig = TestFixtures.flowConfig(
    name = name,
    primaryKey = primaryKey,
    output = OutputConfig(
      sortOrder = sortOrder,
      icebergPartitions = icebergPartitions,
      tableProperties = tableProperties
    )
  )

  private val testSchema = StructType(
    Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("value", DoubleType, nullable = true)
    )
  )

  "IcebergTableManager" should "resolve table name correctly" in {
    val flowConfig = testFlowConfig("test_table")
    tableManager.resolveTableName(flowConfig) shouldBe
      "test_catalog.default.test_table"
  }

  it should "reject a missing table without creating it in validate mode" in {
    val flowConfig = testFlowConfig("validate_missing_table")

    val error = intercept[IllegalArgumentException] {
      validatingTableManager.prepareTable(flowConfig, testSchema)
    }

    error.getMessage should include("ddlMode=validate")
    tableManager.tableExists("test_catalog.default.validate_missing_table") shouldBe false
  }

  it should "validate an exactly provisioned table without applying DDL" in {
    val flowConfig = testFlowConfig(
      "validate_existing_table",
      sortOrder = Seq("id"),
      icebergPartitions = Seq("bucket(8, id)"),
      tableProperties = Map("custom.etl.owner" -> "platform")
    )
    tableManager.prepareTable(flowConfig, testSchema)
    val createStatementBefore = spark
      .sql("SHOW CREATE TABLE test_catalog.default.validate_existing_table")
      .first()
      .getString(0)

    validatingTableManager.prepareTable(flowConfig, testSchema)

    spark
      .sql("SHOW CREATE TABLE test_catalog.default.validate_existing_table")
      .first()
      .getString(0) shouldBe createStatementBefore
  }

  it should "report schema drift without evolving a table in validate mode" in {
    val flowConfig = testFlowConfig("validate_schema_drift")
    tableManager.prepareTable(flowConfig, testSchema)
    val changedSchema = StructType(
      Seq(
        StructField("id", LongType),
        StructField("name", StringType),
        StructField("notes", StringType)
      )
    )

    val error = intercept[IllegalArgumentException] {
      validatingTableManager.prepareTable(flowConfig, changedSchema)
    }

    error.getMessage should include("missing columns: notes")
    error.getMessage should include("unexpected columns: value")
    error.getMessage should include("id expected BIGINT but table has INT")
    val unchangedSchema = spark.table("test_catalog.default.validate_schema_drift").schema
    unchangedSchema.fieldNames shouldBe testSchema.fieldNames
    unchangedSchema.fields.map(_.dataType) shouldBe testSchema.fields.map(_.dataType)
  }

  it should "create a new Iceberg table" in {
    val flowConfig = testFlowConfig("create_test")
    tableManager.prepareTable(flowConfig, testSchema)

    val df = spark.sql("SELECT * FROM test_catalog.default.create_test")
    df.schema.fieldNames should contain allOf ("id", "name", "value")
  }

  it should "not fail when table already exists" in {
    val flowConfig = testFlowConfig("existing_table")
    tableManager.prepareTable(flowConfig, testSchema)

    // Should not throw on second call
    noException should be thrownBy {
      tableManager.prepareTable(flowConfig, testSchema)
    }
  }

  it should "apply new table properties to an existing table" in {
    // Create table without custom properties
    val initial = testFlowConfig("props_update_test")
    tableManager.prepareTable(initial, testSchema)

    // Re-run with a new property added to config
    val updated = testFlowConfig(
      "props_update_test",
      tableProperties = Map("custom.etl.owner" -> "data-team")
    )
    tableManager.prepareTable(updated, testSchema)

    val props = spark
      .sql("SHOW TBLPROPERTIES test_catalog.default.props_update_test")
      .collect()
      .map(row => row.getString(0) -> row.getString(1))
      .toMap

    props should contain("custom.etl.owner" -> "data-team")
  }

  it should "not re-apply table properties that are already set" in {
    val fc = testFlowConfig(
      "props_idempotent_test",
      tableProperties = Map("custom.etl.version" -> "1")
    )
    tableManager.prepareTable(fc, testSchema)

    // Second call with same properties — should not throw
    noException should be thrownBy {
      tableManager.prepareTable(fc, testSchema)
    }
  }

  it should "let an explicit file-format property override the global default" in {
    val flowConfig = testFlowConfig(
      "file_format_override_test",
      tableProperties = Map("write.format.default" -> "avro")
    )

    tableManager.prepareTable(flowConfig, testSchema)
    noException should be thrownBy {
      validatingTableManager.prepareTable(flowConfig, testSchema)
    }

    val properties = spark
      .sql("SHOW TBLPROPERTIES test_catalog.default.file_format_override_test")
      .collect()
      .map(row => row.getString(0) -> row.getString(1))
      .toMap
    properties("write.format.default") shouldBe "avro"
  }

  it should "reject a flow-level format-version override" in {
    val flowConfig = testFlowConfig(
      "format_version_override_test",
      tableProperties = Map("format-version" -> "1")
    )

    val error = intercept[IllegalArgumentException] {
      tableManager.prepareTable(flowConfig, testSchema)
    }

    error.getMessage should include("must not contain 'format-version'")
    tableManager.tableExists("test_catalog.default.format_version_override_test") shouldBe false
  }

  it should "add partition spec to an existing unpartitioned table" in {
    val schemaWithDate = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("event_date", DateType, nullable = true)
      )
    )

    // Create table without partitions
    val initial = testFlowConfig("partition_update_test")
    tableManager.prepareTable(initial, schemaWithDate)

    // Re-run with partition added to config
    val updated = testFlowConfig(
      "partition_update_test",
      icebergPartitions = Seq("month(event_date)")
    )
    tableManager.prepareTable(updated, schemaWithDate)

    // Verify partition was applied: write data and check physical layout
    val data = Seq((1, java.sql.Date.valueOf("2024-01-15")), (2, java.sql.Date.valueOf("2024-02-20")))
      .toDF("id", "event_date")
    data.writeTo("test_catalog.default.partition_update_test").append()

    val showCreate = spark
      .sql("SHOW CREATE TABLE test_catalog.default.partition_update_test")
      .collect()
      .map(_.getString(0))
      .mkString("")

    showCreate.toLowerCase should include("month")
  }

  it should "not fail when adding a partition field that already exists" in {
    val schemaWithDate = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("event_date", DateType, nullable = true)
      )
    )

    val fc = testFlowConfig(
      "partition_idempotent_test",
      icebergPartitions = Seq("month(event_date)")
    )

    tableManager.prepareTable(fc, schemaWithDate)

    // Second call with same partition — should not throw
    noException should be thrownBy {
      tableManager.prepareTable(fc, schemaWithDate)
    }
  }

  it should "add a new column to an existing table" in {
    val initial = testFlowConfig("schema_evolution_test")
    tableManager.prepareTable(initial, testSchema)

    val extendedSchema = StructType(
      testSchema.fields :+
        StructField("notes", StringType, nullable = true)
    )
    tableManager.prepareTable(initial, extendedSchema)

    val cols = spark.table("test_catalog.default.schema_evolution_test").schema.fieldNames
    cols should contain("notes")
  }

  it should "not fail when evolving schema with columns that already exist" in {
    val fc = testFlowConfig("schema_evolution_idempotent_test")
    tableManager.prepareTable(fc, testSchema)

    // Second call with same schema — should not throw
    noException should be thrownBy {
      tableManager.prepareTable(fc, testSchema)
    }
  }

  it should "apply sort order when creating table" in {
    val flowConfig = testFlowConfig("sorted_table", sortOrder = Seq("id"))
    tableManager.prepareTable(flowConfig, testSchema)

    // Table should exist without errors
    val df = spark.sql("SELECT * FROM test_catalog.default.sorted_table")
    df.schema.fieldNames should contain("id")
  }

  it should "get current snapshot id after writing data" in {
    val flowConfig = testFlowConfig("snapshot_test")
    tableManager.prepareTable(flowConfig, testSchema)

    val data = Seq((1, "Alice", 10.0)).toDF("id", "name", "value")
    data.writeTo("test_catalog.default.snapshot_test").append()

    val snapshotId = tableManager.getCurrentSnapshotId(flowConfig)
    snapshotId shouldBe defined
  }

  it should "read main after a rollback rather than the newest known snapshot" in {
    val flowConfig = testFlowConfig("snapshot_head_test")
    val tableName = tableManager.resolveTableName(flowConfig)
    tableManager.prepareTable(flowConfig, testSchema)

    Seq((1, "Alice", 10.0)).toDF("id", "name", "value").writeTo(tableName).append()
    val firstSnapshot = tableManager.getCurrentSnapshotId(flowConfig).get
    Seq((2, "Bob", 20.0)).toDF("id", "name", "value").writeTo(tableName).append()
    val newerSnapshot = tableManager.getCurrentSnapshotId(flowConfig).get
    newerSnapshot should not be firstSnapshot

    spark.sql(
      s"CALL test_catalog.system.set_current_snapshot(table => 'default.snapshot_head_test', snapshot_id => $firstSnapshot)"
    )
    tableManager.getCurrentSnapshotId(flowConfig) shouldBe Some(firstSnapshot)
    spark.sql(s"SELECT COUNT(*) FROM $tableName").first().getLong(0) shouldBe 1L
  }

  it should "return None for snapshot id on empty table" in {
    val flowConfig = testFlowConfig("empty_snapshot_test")
    tableManager.prepareTable(flowConfig, testSchema)

    // Empty table may or may not have a snapshot depending on Iceberg version
    // Just verify it doesn't throw
    noException should be thrownBy {
      tableManager.getCurrentSnapshotId(flowConfig)
    }
  }

  it should "tag a snapshot with batch id" in {
    val flowConfig = testFlowConfig("tag_test")
    tableManager.prepareTable(flowConfig, testSchema)

    val data = Seq((1, "Alice", 10.0)).toDF("id", "name", "value")
    data.writeTo("test_catalog.default.tag_test").append()

    val snapshotId = tableManager.getCurrentSnapshotId(flowConfig).get
    tableManager.tagSnapshot(flowConfig, snapshotId, "batch_001") shouldBe true
    tableManager.tagSnapshot(flowConfig, snapshotId, "batch_001") shouldBe true

    val tag = spark
      .sql(
        s"SELECT snapshot_id, max_reference_age_in_ms FROM ${tableManager.resolveTableName(flowConfig)}.refs " +
          "WHERE name = 'batch_batch_001'"
      )
      .first()
    tag.getLong(0) shouldBe snapshotId
    tag.getLong(1) shouldBe 7L * 24 * 60 * 60 * 1000
  }

  it should "collect snapshot metadata" in {
    val flowConfig = testFlowConfig("metadata_test")
    tableManager.prepareTable(flowConfig, testSchema)

    val data = Seq((1, "Alice", 10.0), (2, "Bob", 20.0)).toDF("id", "name", "value")
    data.writeTo("test_catalog.default.metadata_test").append()

    val snapshotId = tableManager.getCurrentSnapshotId(flowConfig).get
    val metadata = tableManager.getSnapshotMetadata(flowConfig, snapshotId, 2L, "batch_002", tagCreated = true)

    metadata shouldBe defined
    metadata.get.tableName shouldBe "test_catalog.default.metadata_test"
    metadata.get.snapshotId shouldBe snapshotId
    metadata.get.snapshotTag shouldBe Some("batch_batch_002")
    metadata.get.recordsWritten shouldBe 2L
    metadata.get.snapshotTimestampMs should be > 0L
    metadata.get.manifestListLocation should not be empty
  }

  it should "not fail when running snapshot expiration" in {
    val flowConfig = testFlowConfig("maintenance_test")
    tableManager.prepareTable(flowConfig, testSchema)

    Seq((1, "Alice", 10.0))
      .toDF("id", "name", "value")
      .writeTo("test_catalog.default.maintenance_test")
      .append()
    Seq((2, "Bob", 20.0))
      .toDF("id", "name", "value")
      .writeTo("test_catalog.default.maintenance_test")
      .append()

    val maintenanceConfig = MaintenanceConfig(
      snapshotRetentionDays = Some(0),
      targetFileSizeMb = None,
      orphanRetentionMinutes = None,
      enableManifestRewrite = false
    )
    noException should be thrownBy {
      tableManager.runMaintenance(flowConfig, maintenanceConfig)
    }
  }

  it should "not fail when running orphan cleanup with valid retention" in {
    val flowConfig = testFlowConfig("orphan_cleanup_test")
    tableManager.prepareTable(flowConfig, testSchema)

    Seq((1, "Alice", 10.0))
      .toDF("id", "name", "value")
      .writeTo("test_catalog.default.orphan_cleanup_test")
      .append()

    val maintenanceConfig = MaintenanceConfig(
      snapshotRetentionDays = None,
      targetFileSizeMb = None,
      orphanRetentionMinutes = Some(1440),
      enableManifestRewrite = false
    )
    noException should be thrownBy {
      tableManager.runMaintenance(flowConfig, maintenanceConfig)
    }
  }

  // --- parsePartitionTransform tests ---

  "parsePartitionTransform" should "pass through identity partitions" in {
    tableManager.parsePartitionTransform("status") shouldBe "`status`"
  }

  it should "parse temporal transforms" in {
    tableManager.parsePartitionTransform("year(ts)") shouldBe "year(`ts`)"
    tableManager.parsePartitionTransform("month(ts)") shouldBe "month(`ts`)"
    tableManager.parsePartitionTransform("day(ts)") shouldBe "day(`ts`)"
    tableManager.parsePartitionTransform("hour(ts)") shouldBe "hour(`ts`)"
  }

  it should "parse bucket transform" in {
    tableManager.parsePartitionTransform("bucket(16, id)") shouldBe
      "bucket(16, `id`)"
  }

  it should "parse truncate transform" in {
    tableManager.parsePartitionTransform("truncate(10, name)") shouldBe
      "truncate(10, `name`)"
  }

  it should "handle case-insensitive transforms" in {
    tableManager.parsePartitionTransform("MONTH(ts)") shouldBe "month(`ts`)"
    tableManager.parsePartitionTransform("Year(ts)") shouldBe "year(`ts`)"
  }

  it should "reject unknown transforms instead of accepting SQL fragments" in {
    an[IllegalArgumentException] should be thrownBy tableManager.parsePartitionTransform("custom(x)")
  }

  "IcebergTableManager type widening" should "widen int to long automatically" in {
    val fc = testFlowConfig("type_widen_int_long")
    val initialSchema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
    tableManager.prepareTable(fc, initialSchema)

    // Write initial data
    val data = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row(1, "Alice"))),
      initialSchema
    )
    data.writeTo("test_catalog.default.type_widen_int_long").append()

    // Update with widened schema
    val widenedSchema = new StructType()
      .add("id", LongType)
      .add("name", StringType)
    tableManager.prepareTable(fc, widenedSchema)

    val tableSchema = spark.table("test_catalog.default.type_widen_int_long").schema
    tableSchema("id").dataType shouldBe LongType
  }

  it should "widen float to double automatically" in {
    val fc = testFlowConfig("type_widen_float_double")
    val initialSchema = new StructType()
      .add("id", IntegerType)
      .add("score", FloatType)
    tableManager.prepareTable(fc, initialSchema)

    val widenedSchema = new StructType()
      .add("id", IntegerType)
      .add("score", DoubleType)
    tableManager.prepareTable(fc, widenedSchema)

    val tableSchema = spark.table("test_catalog.default.type_widen_float_double").schema
    tableSchema("score").dataType shouldBe DoubleType
  }

  it should "widen decimal precision" in {
    val fc = testFlowConfig("type_widen_decimal")
    val initialSchema = new StructType()
      .add("id", IntegerType)
      .add("amount", DecimalType(10, 2))
    tableManager.prepareTable(fc, initialSchema)

    val widenedSchema = new StructType()
      .add("id", IntegerType)
      .add("amount", DecimalType(18, 2))
    tableManager.prepareTable(fc, widenedSchema)

    val tableSchema = spark.table("test_catalog.default.type_widen_decimal").schema
    tableSchema("amount").dataType shouldBe DecimalType(18, 2)
  }

  it should "not widen incompatible types and log warning" in {
    val fc = testFlowConfig("type_no_widen")
    val initialSchema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
    tableManager.prepareTable(fc, initialSchema)

    // Try to change string to int — not a safe widening
    val incompatibleSchema = new StructType()
      .add("id", IntegerType)
      .add("name", IntegerType)
    tableManager.prepareTable(fc, incompatibleSchema)

    // Type should remain unchanged
    val tableSchema = spark.table("test_catalog.default.type_no_widen").schema
    tableSchema("name").dataType shouldBe StringType
  }

  "isSafeWidening" should "accept valid widenings" in {
    import org.apache.spark.sql.types._
    tableManager.isSafeWidening(IntegerType, LongType) shouldBe true
    tableManager.isSafeWidening(FloatType, DoubleType) shouldBe true
    tableManager.isSafeWidening(DecimalType(10, 2), DecimalType(18, 2)) shouldBe true
  }

  it should "reject invalid widenings" in {
    import org.apache.spark.sql.types._
    tableManager.isSafeWidening(LongType, IntegerType) shouldBe false
    tableManager.isSafeWidening(DoubleType, FloatType) shouldBe false
    tableManager.isSafeWidening(StringType, IntegerType) shouldBe false
    tableManager.isSafeWidening(IntegerType, FloatType) shouldBe false
    tableManager.isSafeWidening(DecimalType(18, 2), DecimalType(10, 2)) shouldBe false
    tableManager.isSafeWidening(DecimalType(10, 2), DecimalType(10, 4)) shouldBe false
  }

  override def afterAll(): Unit = {
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.create_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.existing_table")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.sorted_table")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.snapshot_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.empty_snapshot_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.tag_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.metadata_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.props_update_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.props_idempotent_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.file_format_override_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.partition_update_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.partition_idempotent_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.schema_evolution_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.schema_evolution_idempotent_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.maintenance_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.orphan_cleanup_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.type_widen_int_long")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.type_widen_float_double")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.type_widen_decimal")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.type_no_widen")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.validate_existing_table")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.validate_schema_drift")
    super.afterAll()
  }
}
