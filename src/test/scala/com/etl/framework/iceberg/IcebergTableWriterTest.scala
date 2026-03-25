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
      compareColumns: Seq[String] = Seq.empty,
      validFromColumn: Option[String] = None,
      validToColumn: Option[String] = None,
      isCurrentColumn: Option[String] = None,
      detectDeletes: Boolean = false,
      isActiveColumn: Option[String] = None
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
        compareColumns = compareColumns,
        validFromColumn = validFromColumn,
        validToColumn = validToColumn,
        isCurrentColumn = isCurrentColumn,
        detectDeletes = detectDeletes,
        isActiveColumn = isActiveColumn
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

  it should "be idempotent: re-writing unchanged data leaves table content unchanged" in {
    val fc = flowConfig("delta_idempotent", loadMode = LoadMode.Delta)
    val data = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    writer.writeDeltaLoad(data, fc)

    // Re-write exact same data — change detection should skip all updates
    writer.writeDeltaLoad(data, fc)

    val readBack = spark
      .sql("SELECT * FROM writer_catalog.default.delta_idempotent ORDER BY id")
      .collect()
    readBack.length shouldBe 2
    readBack(0).getAs[String]("name") shouldBe "Alice"
    readBack(1).getAs[String]("name") shouldBe "Bob"
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

  // --- SCD2 Detect Deletes Tests ---

  "IcebergTableWriter.writeSCD2Load with detectDeletes" should "soft-delete records missing from source" in {
    val fc = flowConfig(
      "scd2_detect_del",
      loadMode = LoadMode.SCD2,
      compareColumns = Seq("name"),
      validFromColumn = Some("valid_from"),
      validToColumn = Some("valid_to"),
      isCurrentColumn = Some("is_current"),
      detectDeletes = true,
      isActiveColumn = Some("is_active")
    )

    // Day 1: initial load
    val day1 = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    writer.writeSCD2Load(day1, fc)

    val afterDay1 = spark.sql("SELECT * FROM writer_catalog.default.scd2_detect_del")
    afterDay1.count() shouldBe 3
    afterDay1.filter("is_current = true AND is_active = true").count() shouldBe 3

    // Day 2: Alice changes, Charlie removed from source
    val day2 = Seq((1, "Alice_v2"), (2, "Bob")).toDF("id", "name")
    writer.writeSCD2Load(day2, fc)

    val afterDay2 = spark.sql("SELECT * FROM writer_catalog.default.scd2_detect_del")

    // Alice: old closed + new current = 2
    // Bob: unchanged = 1
    // Charlie: soft-deleted (is_current=false, is_active=false) = 1
    // Total = 4
    afterDay2.count() shouldBe 4
    afterDay2.filter("is_current = true").count() shouldBe 2

    // Verify Charlie is soft-deleted
    val charlie = afterDay2.filter("id = 3").collect()
    charlie.length shouldBe 1
    charlie.head.getAs[Boolean]("is_current") shouldBe false
    charlie.head.getAs[Boolean]("is_active") shouldBe false

    // Verify Alice has 2 versions
    afterDay2.filter("id = 1").count() shouldBe 2
    val aliceCurrent = afterDay2.filter("id = 1 AND is_current = true").collect().head
    aliceCurrent.getAs[String]("name") shouldBe "Alice_v2"
    aliceCurrent.getAs[Boolean]("is_active") shouldBe true
  }

  it should "not affect missing records when detectDeletes is false" in {
    val fc = flowConfig(
      "scd2_no_detect_del",
      loadMode = LoadMode.SCD2,
      compareColumns = Seq("name"),
      validFromColumn = Some("valid_from"),
      validToColumn = Some("valid_to"),
      isCurrentColumn = Some("is_current"),
      detectDeletes = false
    )

    // Day 1: initial load
    val day1 = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    writer.writeSCD2Load(day1, fc)

    // Day 2: Charlie removed from source (but detectDeletes=false)
    val day2 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    writer.writeSCD2Load(day2, fc)

    val afterDay2 = spark.sql("SELECT * FROM writer_catalog.default.scd2_no_detect_del")

    // Charlie should still be is_current=true (not soft-deleted)
    afterDay2.filter("is_current = true").count() shouldBe 3
    val charlie = afterDay2.filter("id = 3 AND is_current = true").collect()
    charlie.length shouldBe 1
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
    "delta_idempotent",
    "scd2_first",
    "scd2_change",
    "scd2_detect_del",
    "scd2_no_detect_del",
    "tag_result_test"
  )

  override def afterAll(): Unit = {
    allTables.foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS writer_catalog.default.$t")
    }
    super.afterAll()
  }
}
