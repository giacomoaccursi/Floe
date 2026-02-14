package com.etl.framework.iceberg

import com.etl.framework.config._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}

class IcebergTableWriterTest
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private var warehousePath: Path = _

  implicit val spark: SparkSession = {
    val tmpDir = Files.createTempDirectory("iceberg-writer-test")
    warehousePath = tmpDir

    // Stop any existing session to ensure Iceberg extensions are loaded
    SparkSession.getActiveSession.foreach(_.stop())

    SparkSession
      .builder()
      .appName("IcebergTableWriterTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config(
        "spark.sql.catalog.writer_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config("spark.sql.catalog.writer_catalog.type", "hadoop")
      .config(
        "spark.sql.catalog.writer_catalog.warehouse",
        tmpDir.toString
      )
      .getOrCreate()
  }

  import spark.implicits._

  private val icebergConfig = IcebergConfig(
    catalogName = "writer_catalog",
    warehouse = warehousePath.toString,
    enableSnapshotTagging = true
  )

  private val tableManager = new IcebergTableManager(spark, icebergConfig)
  private val writer = new IcebergTableWriter(spark, icebergConfig, tableManager)

  private def flowConfig(
      name: String,
      loadMode: LoadMode = LoadMode.Full,
      primaryKey: Seq[String] = Seq("id"),
      updateTimestampColumn: Option[String] = None,
      compareColumns: Seq[String] = Seq.empty,
      validFromColumn: Option[String] = None,
      validToColumn: Option[String] = None,
      isCurrentColumn: Option[String] = None
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
      loadMode = LoadModeConfig(
        `type` = loadMode,
        updateTimestampColumn = updateTimestampColumn,
        compareColumns = compareColumns,
        validFromColumn = validFromColumn,
        validToColumn = validToColumn,
        isCurrentColumn = isCurrentColumn
      ),
      validation = ValidationConfig(
        primaryKey = primaryKey,
        foreignKeys = Seq.empty,
        rules = Seq.empty
      ),
      output = OutputConfig()
    )
  }

  // --- Full Load Tests ---

  "IcebergTableWriter.writeFullLoad" should "write data and return WriteResult" in {
    val fc = flowConfig("full_test")
    val data = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")

    val result = writer.writeFullLoad(data, fc)
    result.recordsWritten shouldBe 2
    result.snapshotId shouldBe defined

    val readBack = spark.sql("SELECT * FROM writer_catalog.default.full_test")
    readBack.count() shouldBe 2
  }

  it should "overwrite data on subsequent full loads" in {
    val fc = flowConfig("full_overwrite")
    val data1 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    writer.writeFullLoad(data1, fc)

    val data2 = Seq((3, "Charlie"), (4, "Dave"), (5, "Eve")).toDF("id", "name")
    val result = writer.writeFullLoad(data2, fc)

    result.recordsWritten shouldBe 3
    val readBack =
      spark.sql("SELECT * FROM writer_catalog.default.full_overwrite")
    readBack.count() shouldBe 3
  }

  // --- Delta Upsert Tests ---

  "IcebergTableWriter.writeDeltaLoad" should "insert all records on first load" in {
    val fc = flowConfig("delta_first", loadMode = LoadMode.Delta)
    val data = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")

    val result = writer.writeDeltaLoad(data, fc)
    result.recordsWritten shouldBe 2

    val readBack =
      spark.sql("SELECT * FROM writer_catalog.default.delta_first ORDER BY id")
    readBack.count() shouldBe 2
  }

  it should "upsert records on subsequent loads" in {
    val fc = flowConfig("delta_upsert", loadMode = LoadMode.Delta)
    val initial = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    writer.writeDeltaLoad(initial, fc)

    val update = Seq((2, "Bob_updated"), (3, "Charlie")).toDF("id", "name")
    writer.writeDeltaLoad(update, fc)

    val readBack = spark
      .sql(
        "SELECT * FROM writer_catalog.default.delta_upsert ORDER BY id"
      )
      .collect()
    readBack.length shouldBe 3
    readBack(0).getAs[String]("name") shouldBe "Alice"
    readBack(1).getAs[String]("name") shouldBe "Bob_updated"
    readBack(2).getAs[String]("name") shouldBe "Charlie"
  }

  it should "respect timestamp column for conditional updates" in {
    val fc = flowConfig(
      "delta_ts",
      loadMode = LoadMode.Delta,
      updateTimestampColumn = Some("ts")
    )
    val initial =
      Seq((1, "Alice", 2L), (2, "Bob", 3L)).toDF("id", "name", "ts")
    writer.writeDeltaLoad(initial, fc)

    // Update with older timestamp should NOT overwrite
    val olderUpdate =
      Seq((1, "Alice_OLD", 1L)).toDF("id", "name", "ts")
    writer.writeDeltaLoad(olderUpdate, fc)

    val readBack = spark
      .sql("SELECT * FROM writer_catalog.default.delta_ts WHERE id = 1")
      .collect()
    readBack.head.getAs[String]("name") shouldBe "Alice"

    // Update with newer timestamp SHOULD overwrite
    val newerUpdate =
      Seq((1, "Alice_NEW", 5L)).toDF("id", "name", "ts")
    writer.writeDeltaLoad(newerUpdate, fc)

    val readBack2 = spark
      .sql("SELECT * FROM writer_catalog.default.delta_ts WHERE id = 1")
      .collect()
    readBack2.head.getAs[String]("name") shouldBe "Alice_NEW"
  }

  // --- SCD2 Tests ---

  "IcebergTableWriter.writeSCD2Load" should "create current records on first load" in {
    val fc = flowConfig(
      "scd2_first",
      loadMode = LoadMode.SCD2,
      compareColumns = Seq("name"),
      validFromColumn = Some("valid_from"),
      validToColumn = Some("valid_to"),
      isCurrentColumn = Some("is_current")
    )
    val data = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")

    val result = writer.writeSCD2Load(data, fc)
    result.recordsWritten shouldBe 2

    val readBack =
      spark.sql("SELECT * FROM writer_catalog.default.scd2_first")
    readBack.count() shouldBe 2
    readBack.filter("is_current = true").count() shouldBe 2
    readBack.filter("valid_to IS NULL").count() shouldBe 2
  }

  it should "create new versions for changed records" in {
    val fc = flowConfig(
      "scd2_change",
      loadMode = LoadMode.SCD2,
      compareColumns = Seq("name"),
      validFromColumn = Some("valid_from"),
      validToColumn = Some("valid_to"),
      isCurrentColumn = Some("is_current")
    )

    val initial = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    writer.writeSCD2Load(initial, fc)

    val changed = Seq((1, "Alice_v2"), (2, "Bob")).toDF("id", "name")
    writer.writeSCD2Load(changed, fc)

    val readBack =
      spark.sql("SELECT * FROM writer_catalog.default.scd2_change")

    // Alice: 1 closed + 1 current = 2. Bob: 1 current = 1. Total = 3
    readBack.count() shouldBe 3
    readBack.filter("is_current = true").count() shouldBe 2

    val aliceRecords = readBack.filter("id = 1").orderBy("valid_from")
    aliceRecords.count() shouldBe 2
  }

  // --- Snapshot Tagging ---

  "IcebergTableWriter.tagBatchSnapshot" should "tag snapshot and return metadata" in {
    val fc = flowConfig("tag_result_test")
    val data = Seq((1, "Alice")).toDF("id", "name")
    val writeResult = writer.writeFullLoad(data, fc)

    val taggedResult = writer.tagBatchSnapshot(fc, writeResult, "test_batch")
    taggedResult.icebergMetadata shouldBe defined
    taggedResult.icebergMetadata.get.snapshotTag shouldBe Some(
      "batch_test_batch"
    )
  }

  it should "return unmodified result when no snapshot id" in {
    val noSnapshot = WriteResult(0, None)
    val fc = flowConfig("no_snapshot_tag")

    val result = writer.tagBatchSnapshot(fc, noSnapshot, "batch_001")
    result.icebergMetadata shouldBe None
  }

  private val allTables = Seq(
    "full_test",
    "full_overwrite",
    "delta_first",
    "delta_upsert",
    "delta_ts",
    "scd2_first",
    "scd2_change",
    "tag_result_test"
  )

  override def afterAll(): Unit = {
    allTables.foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS writer_catalog.default.$t")
    }
    super.afterAll()
  }
}
