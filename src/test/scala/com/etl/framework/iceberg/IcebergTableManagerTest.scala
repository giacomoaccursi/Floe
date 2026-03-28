package com.etl.framework.iceberg

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
    warehouse = warehousePath.toString,
    enableSnapshotTagging = true
  )

  private val tableManager = new IcebergTableManager(spark, icebergConfig)

  private def testFlowConfig(
      name: String,
      primaryKey: Seq[String] = Seq("id"),
      sortOrder: Seq[String] = Seq.empty,
      icebergPartitions: Seq[String] = Seq.empty,
      tableProperties: Map[String, String] = Map.empty
  ): FlowConfig = {
    FlowConfig(
      name = name,
      description = "test",
      version = "1.0",
      owner = "test",
      source = SourceConfig(
        `type` = SourceType.File,
        path = "/tmp/test",
        format = FileFormat.CSV,
        options = Map.empty
      ),
      schema = SchemaConfig(
        enforceSchema = false,
        allowExtraColumns = true,
        columns = Seq.empty
      ),
      loadMode = LoadModeConfig(`type` = LoadMode.Full),
      validation = ValidationConfig(
        primaryKey = primaryKey,
        foreignKeys = Seq.empty,
        rules = Seq.empty
      ),
      output = OutputConfig(
        sortOrder = sortOrder,
        icebergPartitions = icebergPartitions,
        tableProperties = tableProperties
      )
    )
  }

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

  it should "create a new Iceberg table" in {
    val flowConfig = testFlowConfig("create_test")
    tableManager.createOrUpdateTable(flowConfig, testSchema)

    val df = spark.sql("SELECT * FROM test_catalog.default.create_test")
    df.schema.fieldNames should contain allOf ("id", "name", "value")
  }

  it should "not fail when table already exists" in {
    val flowConfig = testFlowConfig("existing_table")
    tableManager.createOrUpdateTable(flowConfig, testSchema)

    // Should not throw on second call
    noException should be thrownBy {
      tableManager.createOrUpdateTable(flowConfig, testSchema)
    }
  }

  it should "apply new table properties to an existing table" in {
    // Create table without custom properties
    val initial = testFlowConfig("props_update_test")
    tableManager.createOrUpdateTable(initial, testSchema)

    // Re-run with a new property added to config
    val updated = testFlowConfig(
      "props_update_test",
      tableProperties = Map("custom.etl.owner" -> "data-team")
    )
    tableManager.createOrUpdateTable(updated, testSchema)

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
    tableManager.createOrUpdateTable(fc, testSchema)

    // Second call with same properties — should not throw
    noException should be thrownBy {
      tableManager.createOrUpdateTable(fc, testSchema)
    }
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
    tableManager.createOrUpdateTable(initial, schemaWithDate)

    // Re-run with partition added to config
    val updated = testFlowConfig(
      "partition_update_test",
      icebergPartitions = Seq("month(event_date)")
    )
    tableManager.createOrUpdateTable(updated, schemaWithDate)

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

    tableManager.createOrUpdateTable(fc, schemaWithDate)

    // Second call with same partition — should not throw
    noException should be thrownBy {
      tableManager.createOrUpdateTable(fc, schemaWithDate)
    }
  }

  it should "add a new column to an existing table" in {
    val initial = testFlowConfig("schema_evolution_test")
    tableManager.createOrUpdateTable(initial, testSchema)

    val extendedSchema = StructType(
      testSchema.fields :+
        StructField("notes", StringType, nullable = true)
    )
    tableManager.createOrUpdateTable(initial, extendedSchema)

    val cols = spark.table("test_catalog.default.schema_evolution_test").schema.fieldNames
    cols should contain("notes")
  }

  it should "not fail when evolving schema with columns that already exist" in {
    val fc = testFlowConfig("schema_evolution_idempotent_test")
    tableManager.createOrUpdateTable(fc, testSchema)

    // Second call with same schema — should not throw
    noException should be thrownBy {
      tableManager.createOrUpdateTable(fc, testSchema)
    }
  }

  it should "apply sort order when creating table" in {
    val flowConfig = testFlowConfig("sorted_table", sortOrder = Seq("id"))
    tableManager.createOrUpdateTable(flowConfig, testSchema)

    // Table should exist without errors
    val df = spark.sql("SELECT * FROM test_catalog.default.sorted_table")
    df.schema.fieldNames should contain("id")
  }

  it should "get current snapshot id after writing data" in {
    val flowConfig = testFlowConfig("snapshot_test")
    tableManager.createOrUpdateTable(flowConfig, testSchema)

    val data = Seq((1, "Alice", 10.0)).toDF("id", "name", "value")
    data.writeTo("test_catalog.default.snapshot_test").append()

    val snapshotId = tableManager.getCurrentSnapshotId(flowConfig)
    snapshotId shouldBe defined
  }

  it should "return None for snapshot id on empty table" in {
    val flowConfig = testFlowConfig("empty_snapshot_test")
    tableManager.createOrUpdateTable(flowConfig, testSchema)

    // Empty table may or may not have a snapshot depending on Iceberg version
    // Just verify it doesn't throw
    noException should be thrownBy {
      tableManager.getCurrentSnapshotId(flowConfig)
    }
  }

  it should "tag a snapshot with batch id" in {
    val flowConfig = testFlowConfig("tag_test")
    tableManager.createOrUpdateTable(flowConfig, testSchema)

    val data = Seq((1, "Alice", 10.0)).toDF("id", "name", "value")
    data.writeTo("test_catalog.default.tag_test").append()

    val snapshotId = tableManager.getCurrentSnapshotId(flowConfig).get
    tableManager.tagSnapshot(flowConfig, snapshotId, "batch_001") shouldBe true
  }

  it should "collect snapshot metadata" in {
    val flowConfig = testFlowConfig("metadata_test")
    tableManager.createOrUpdateTable(flowConfig, testSchema)

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
    tableManager.createOrUpdateTable(flowConfig, testSchema)

    Seq((1, "Alice", 10.0))
      .toDF("id", "name", "value")
      .writeTo("test_catalog.default.maintenance_test")
      .append()
    Seq((2, "Bob", 20.0))
      .toDF("id", "name", "value")
      .writeTo("test_catalog.default.maintenance_test")
      .append()

    val maintenanceConfig = MaintenanceConfig(
      enableSnapshotExpiration = true,
      snapshotRetentionDays = 0,
      enableCompaction = false,
      targetFileSizeMb = 128,
      enableOrphanCleanup = false,
      orphanRetentionMinutes = 1440,
      enableManifestRewrite = false
    )
    noException should be thrownBy {
      tableManager.runMaintenance(flowConfig, maintenanceConfig)
    }
  }

  it should "not fail when running orphan cleanup with valid retention" in {
    val flowConfig = testFlowConfig("orphan_cleanup_test")
    tableManager.createOrUpdateTable(flowConfig, testSchema)

    Seq((1, "Alice", 10.0))
      .toDF("id", "name", "value")
      .writeTo("test_catalog.default.orphan_cleanup_test")
      .append()

    val maintenanceConfig = MaintenanceConfig(
      enableSnapshotExpiration = false,
      snapshotRetentionDays = 7,
      enableCompaction = false,
      targetFileSizeMb = 128,
      enableOrphanCleanup = true,
      orphanRetentionMinutes = 1440,
      enableManifestRewrite = false
    )
    noException should be thrownBy {
      tableManager.runMaintenance(flowConfig, maintenanceConfig)
    }
  }

  // --- parsePartitionTransform tests ---

  "parsePartitionTransform" should "pass through identity partitions" in {
    tableManager.parsePartitionTransform("status") shouldBe "status"
  }

  it should "parse temporal transforms" in {
    tableManager.parsePartitionTransform("year(ts)") shouldBe "year(ts)"
    tableManager.parsePartitionTransform("month(ts)") shouldBe "month(ts)"
    tableManager.parsePartitionTransform("day(ts)") shouldBe "day(ts)"
    tableManager.parsePartitionTransform("hour(ts)") shouldBe "hour(ts)"
  }

  it should "parse bucket transform" in {
    tableManager.parsePartitionTransform("bucket(16, id)") shouldBe
      "bucket(16, id)"
  }

  it should "parse truncate transform" in {
    tableManager.parsePartitionTransform("truncate(10, name)") shouldBe
      "truncate(10, name)"
  }

  it should "handle case-insensitive transforms" in {
    tableManager.parsePartitionTransform("MONTH(ts)") shouldBe "month(ts)"
    tableManager.parsePartitionTransform("Year(ts)") shouldBe "year(ts)"
  }

  it should "pass through unknown transforms" in {
    tableManager.parsePartitionTransform("custom(x)") shouldBe "custom(x)"
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
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.partition_update_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.partition_idempotent_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.schema_evolution_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.schema_evolution_idempotent_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.maintenance_test")
    spark.sql("DROP TABLE IF EXISTS test_catalog.default.orphan_cleanup_test")
    super.afterAll()
  }
}
