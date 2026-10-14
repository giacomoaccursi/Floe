package com.etl.framework.iceberg

import com.etl.framework.config._
import com.etl.framework.TestFixtures
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}
import java.time.Instant

class IcebergTableWriterTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

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
      .config("spark.driver.bindAddress", "127.0.0.1")
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
    ddlMode = DdlMode.Automatic,
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
  ): FlowConfig =
    TestFixtures.flowConfig(
      name = name,
      loadMode = loadMode,
      primaryKey = primaryKey,
      compareColumns =
        if (compareColumns.nonEmpty) compareColumns
        else if (loadMode == LoadMode.SCD2) Seq("name")
        else Seq.empty,
      detectDeletes = detectDeletes
    )

  // --- Full Load Tests ---

  "IcebergTableWriter.writeFullLoad" should "write data and return WriteResult" in {
    val fc = flowConfig("full_test")
    val data = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")

    val result = writer.writeFullLoad(data, fc)
    result.recordsProcessed shouldBe 2
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

    result.recordsProcessed shouldBe 3
    val readBack =
      spark.sql("SELECT * FROM writer_catalog.default.full_overwrite")
    readBack.count() shouldBe 3
  }

  // --- Delta Upsert Tests ---

  "IcebergTableWriter.writeDeltaLoad" should "insert all records on first load" in {
    val fc = flowConfig("delta_first", loadMode = LoadMode.Delta)
    val data = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")

    val result = writer.writeDeltaLoad(data, fc)
    result.recordsProcessed shouldBe 2

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

  it should "quote reserved and spaced identifiers in delta MERGE" in {
    val fc = flowConfig(
      "order items",
      loadMode = LoadMode.Delta,
      primaryKey = Seq("select")
    )
    val initial = Seq((1, "Alice"), (2, "Bob")).toDF("select", "customer name")
    writer.writeDeltaLoad(initial, fc)

    val update = Seq((2, "Robert"), (3, "Charlie")).toDF("select", "customer name")
    writer.writeDeltaLoad(update, fc)

    val rows = spark
      .sql("SELECT `select`, `customer name` FROM writer_catalog.default.`order items` ORDER BY `select`")
      .collect()
    rows.map(_.getInt(0)) shouldBe Array(1, 2, 3)
    rows(1).getString(1) shouldBe "Robert"
  }

  it should "reject duplicate source keys before a delta MERGE" in {
    val fc = flowConfig("delta_duplicate_source", loadMode = LoadMode.Delta)
    val duplicates = Seq((1, "Alice"), (1, "Alice_v2")).toDF("id", "name")

    val error = intercept[IllegalArgumentException] {
      writer.writeDeltaLoad(duplicates, fc)
    }
    error.getMessage should include("duplicate primary keys")
    spark.catalog.tableExists("writer_catalog.default.delta_duplicate_source") shouldBe false
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
    result.recordsProcessed shouldBe 2

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

  it should "reject a changed version whose effective time does not follow the current version" in {
    val fc = flowConfig("scd2_out_of_order", loadMode = LoadMode.SCD2, compareColumns = Seq("name"))
    val currentTime = Instant.parse("2026-10-06T10:00:00Z")
    val staleTime = Instant.parse("2026-10-06T09:00:00Z")
    writer.writeSCD2Load(
      Seq((1, "Alice")).toDF("id", "name"),
      fc,
      CommitContext.forFlow("initial", fc.name, "scd2", currentTime)
    )

    val error = intercept[IllegalArgumentException] {
      writer.writeSCD2Load(
        Seq((1, "Alice stale")).toDF("id", "name"),
        fc,
        CommitContext.forFlow("stale", fc.name, "scd2", staleTime)
      )
    }

    error.getMessage should include("must be later than the current SCD2 version")
    val table = spark.sql("SELECT * FROM writer_catalog.default.scd2_out_of_order")
    table.count() shouldBe 1L
    table.filter("is_current = true AND name = 'Alice'").count() shouldBe 1L
  }

  it should "reject a target with multiple current versions before the SCD2 MERGE" in {
    val fc = flowConfig("scd2_duplicate_current", loadMode = LoadMode.SCD2, compareColumns = Seq("name"))
    writer.writeSCD2Load(
      Seq((1, "Alice")).toDF("id", "name"),
      fc,
      CommitContext.forFlow("initial", fc.name, "scd2", Instant.parse("2026-10-06T10:00:00Z"))
    )
    spark.sql(
      """INSERT INTO writer_catalog.default.scd2_duplicate_current
        |VALUES (1, 'corrupt duplicate', TIMESTAMP '2026-10-06 11:00:00', CAST(NULL AS TIMESTAMP), true)""".stripMargin
    )

    val error = intercept[IllegalArgumentException] {
      writer.writeSCD2Load(
        Seq((1, "Alice changed")).toDF("id", "name"),
        fc,
        CommitContext.forFlow("update", fc.name, "scd2", Instant.parse("2026-10-06T12:00:00Z"))
      )
    }

    error.getMessage should include("more than one current version for the same primary key")
    spark.sql("SELECT * FROM writer_catalog.default.scd2_duplicate_current").count() shouldBe 2L
  }

  it should "reject a malformed current SCD2 interval before the MERGE" in {
    val fc = flowConfig("scd2_invalid_current", loadMode = LoadMode.SCD2, compareColumns = Seq("name"))
    writer.writeSCD2Load(
      Seq((1, "Alice")).toDF("id", "name"),
      fc,
      CommitContext.forFlow("initial", fc.name, "scd2", Instant.parse("2026-10-06T10:00:00Z"))
    )
    spark.sql(
      """UPDATE writer_catalog.default.scd2_invalid_current
        |SET valid_to = TIMESTAMP '2026-10-06 11:00:00'
        |WHERE id = 1""".stripMargin
    )

    val error = intercept[IllegalArgumentException] {
      writer.writeSCD2Load(
        Seq((1, "Alice changed")).toDF("id", "name"),
        fc,
        CommitContext.forFlow("update", fc.name, "scd2", Instant.parse("2026-10-06T12:00:00Z"))
      )
    }

    error.getMessage should include("invalid current version")
    spark
      .sql("SELECT * FROM writer_catalog.default.scd2_invalid_current")
      .filter("is_current = true AND valid_to IS NOT NULL")
      .count() shouldBe 1L
  }

  it should "allow an unchanged retry at the same effective time" in {
    val fc = flowConfig("scd2_same_time_noop", loadMode = LoadMode.SCD2, compareColumns = Seq("name"))
    val effectiveAt = Instant.parse("2026-10-06T10:00:00Z")
    val data = Seq((1, "Alice")).toDF("id", "name")

    writer.writeSCD2Load(data, fc, CommitContext.forFlow("initial", fc.name, "scd2", effectiveAt))
    writer.writeSCD2Load(data, fc, CommitContext.forFlow("retry", fc.name, "scd2", effectiveAt))

    val table = spark.sql("SELECT * FROM writer_catalog.default.scd2_same_time_noop")
    table.count() shouldBe 1L
    table.filter("is_current = true").count() shouldBe 1L
  }

  it should "reject duplicate source keys before an SCD2 load" in {
    val fc = flowConfig("scd2_duplicate_source", loadMode = LoadMode.SCD2, compareColumns = Seq("name"))
    val duplicates = Seq((1, "Alice"), (1, "Alice_v2")).toDF("id", "name")

    val error = intercept[IllegalArgumentException] {
      writer.writeSCD2Load(duplicates, fc)
    }
    error.getMessage should include("duplicate primary keys")
    spark.catalog.tableExists("writer_catalog.default.scd2_duplicate_source") shouldBe false
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

  it should "reject an out-of-order soft delete" in {
    val fc = flowConfig(
      "scd2_out_of_order_delete",
      loadMode = LoadMode.SCD2,
      compareColumns = Seq("name"),
      detectDeletes = true,
      isActiveColumn = Some("is_active")
    )
    val currentTime = Instant.parse("2026-10-06T10:00:00Z")
    writer.writeSCD2Load(
      Seq((1, "Alice"), (2, "Bob")).toDF("id", "name"),
      fc,
      CommitContext.forFlow("initial-delete", fc.name, "scd2", currentTime)
    )

    intercept[IllegalArgumentException] {
      writer.writeSCD2Load(
        Seq((1, "Alice")).toDF("id", "name"),
        fc,
        CommitContext.forFlow("stale-delete", fc.name, "scd2", currentTime.minusSeconds(1))
      )
    }.getMessage should include("must be later than the current SCD2 version")

    val table = spark.sql("SELECT * FROM writer_catalog.default.scd2_out_of_order_delete")
    table.filter("is_current = true AND is_active = true").count() shouldBe 2L
  }

  it should "reject an out-of-order reactivation after a soft delete" in {
    val fc = flowConfig(
      "scd2_out_of_order_reactivation",
      loadMode = LoadMode.SCD2,
      compareColumns = Seq("name"),
      detectDeletes = true,
      isActiveColumn = Some("is_active")
    )
    val initialTime = Instant.parse("2026-10-06T10:00:00Z")
    val deletionTime = Instant.parse("2026-10-06T12:00:00Z")
    val staleReactivationTime = Instant.parse("2026-10-06T11:00:00Z")
    val original = Seq((1, "Alice")).toDF("id", "name")

    writer.writeSCD2Load(
      original,
      fc,
      CommitContext.forFlow("initial-reactivation", fc.name, "scd2", initialTime)
    )
    writer.writeSCD2Load(
      original.limit(0),
      fc,
      CommitContext.forFlow("delete-before-reactivation", fc.name, "scd2", deletionTime)
    )

    intercept[IllegalArgumentException] {
      writer.writeSCD2Load(
        Seq((1, "Alice restored")).toDF("id", "name"),
        fc,
        CommitContext.forFlow("stale-reactivation", fc.name, "scd2", staleReactivationTime)
      )
    }.getMessage should include("must be later than the latest SCD2 boundary")

    val table = spark.sql("SELECT * FROM writer_catalog.default.scd2_out_of_order_reactivation")
    table.count() shouldBe 1L
    table.filter("is_current = true").count() shouldBe 0L
    table.filter("is_active = false").count() shouldBe 1L
  }

  it should "reactivate a soft-deleted key after its latest temporal boundary" in {
    val fc = flowConfig(
      "scd2_ordered_reactivation",
      loadMode = LoadMode.SCD2,
      compareColumns = Seq("name"),
      detectDeletes = true,
      isActiveColumn = Some("is_active")
    )
    val original = Seq((1, "Alice")).toDF("id", "name")

    writer.writeSCD2Load(
      original,
      fc,
      CommitContext.forFlow("ordered-initial", fc.name, "scd2", Instant.parse("2026-10-06T10:00:00Z"))
    )
    writer.writeSCD2Load(
      original.limit(0),
      fc,
      CommitContext.forFlow("ordered-delete", fc.name, "scd2", Instant.parse("2026-10-06T11:00:00Z"))
    )
    writer.writeSCD2Load(
      Seq((1, "Alice restored")).toDF("id", "name"),
      fc,
      CommitContext.forFlow("ordered-reactivate", fc.name, "scd2", Instant.parse("2026-10-06T12:00:00Z"))
    )

    val table = spark.sql("SELECT * FROM writer_catalog.default.scd2_ordered_reactivation")
    table.count() shouldBe 2L
    table.filter("is_current = true AND is_active = true AND name = 'Alice restored'").count() shouldBe 1L
    table.filter("is_current = false AND is_active = false").count() shouldBe 1L
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

  "IcebergTableWriter tracked commits" should "persist a stable operation identity in the committed snapshot" in {
    val fc = flowConfig("tracked_commit")
    val data = Seq((1, "Alice")).toDF("id", "name")
    val context = CommitContext.forFlow("batch_20260923", fc.name, "full")

    val result = writer.writeFullLoad(data, fc, context)

    result.operationId shouldBe context.operationId
    result.snapshotId shouldBe defined
    tableManager.findSnapshotsByOperationId(fc, context.operationId).map(_.snapshotId) shouldBe Seq(
      result.snapshotId.get
    )
    result.icebergMetadata.flatMap(_.summary.get("floe.operation-id")) shouldBe Some(context.operationId)
  }

  it should "retain the committed snapshot when the optional schema lookup fails" in {
    val fc = flowConfig("tracked_commit_schema_lookup_failure")
    val context = CommitContext.forFlow("schema-lookup-failure", fc.name, "full")
    val schemaFailure = new IllegalStateException("simulated post-commit schema lookup failure")
    val schemaFailingWriter = new IcebergTableWriter(spark, icebergConfig, tableManager) {
      override private[iceberg] def loadResultingSchemaJson(tableName: String): String = throw schemaFailure
    }

    val result = schemaFailingWriter.writeFullLoad(Seq((1, "Alice")).toDF("id", "name"), fc, context)

    result.snapshotId shouldBe defined
    result.resultingSnapshotId shouldBe result.snapshotId
    result.resultingSchemaJson shouldBe None
    tableManager.findSnapshotsByOperationId(fc, context.operationId).map(_.snapshotId) shouldBe Seq(
      result.snapshotId.get
    )
  }

  it should "propagate fatal errors without attempting commit reconciliation" in {
    class ProbeTableManager extends IcebergTableManager(spark, icebergConfig) {
      var reconciliationLookups: Int = 0
      override def findSnapshotsByOperationId(tableName: String, operationId: String): Seq[CommittedSnapshot] = {
        reconciliationLookups += 1
        Seq.empty
      }
    }
    val probeManager = new ProbeTableManager
    val probeWriter = new IcebergTableWriter(spark, icebergConfig, probeManager)
    val fc = flowConfig("fatal_commit")
    val fatal = new VirtualMachineError("simulated fatal error") {}

    val thrown = intercept[VirtualMachineError] {
      probeWriter.writeFullLoad(
        Seq((1, "Alice")).toDF("id", "name"),
        fc,
        CommitContext.forFlow("fatal-attempt", fc.name, "full"),
        () => throw fatal
      )
    }

    thrown should be theSameInstanceAs fatal
    probeManager.reconciliationLookups shouldBe 0
  }

  it should "return unmodified result when no snapshot id" in {
    val noSnapshot = WriteResult(0, None)
    val fc = flowConfig("no_snapshot_tag")

    val result = writer.tagBatchSnapshot(fc, noSnapshot, "batch_001")
    result.icebergMetadata shouldBe None
  }

  // --- Delta Load without Primary Key ---

  "IcebergTableWriter.writeDeltaLoad" should "reject a missing primary key before creating the target" in {
    val fc = flowConfig("delta_no_pk", loadMode = LoadMode.Delta, primaryKey = Seq.empty)
    val data = Seq((1, "Alice")).toDF("id", "name")

    val error = the[IllegalArgumentException] thrownBy writer.writeDeltaLoad(data, fc)
    error.getMessage should include("requires a primary key")
    spark.catalog.tableExists("writer_catalog.default.delta_no_pk") shouldBe false
  }

  // --- SCD2 NULL handling ---

  "IcebergTableWriter.writeSCD2Load" should "treat NULL-to-value change as a new version" in {
    val fc = flowConfig(
      "scd2_null_change",
      loadMode = LoadMode.SCD2,
      compareColumns = Seq("score"),
      validFromColumn = Some("valid_from"),
      validToColumn = Some("valid_to"),
      isCurrentColumn = Some("is_current")
    )

    // Initial load: score is NULL for both records
    val initial = Seq(
      (1, null.asInstanceOf[java.lang.Integer]),
      (2, null.asInstanceOf[java.lang.Integer])
    ).toDF("id", "score")
    writer.writeSCD2Load(initial, fc)

    spark
      .sql("SELECT * FROM writer_catalog.default.scd2_null_change")
      .filter("is_current = true")
      .count() shouldBe 2L

    // Second load: record 1 now has score=100 (NULL→value = change), record 2 stays NULL→NULL (no change)
    val update = Seq(
      (1, 100.asInstanceOf[java.lang.Integer]),
      (2, null.asInstanceOf[java.lang.Integer])
    ).toDF("id", "score")
    writer.writeSCD2Load(update, fc)

    val table = spark.sql("SELECT * FROM writer_catalog.default.scd2_null_change")
    // record 1: 1 closed + 1 current = 2; record 2: unchanged = 1; total = 3
    table.count() shouldBe 3L
    table.filter("is_current = true").count() shouldBe 2L
    // record 1 current version has score=100
    val rec1Current = table.filter("id = 1 AND is_current = true").collect().head
    rec1Current.getAs[java.lang.Integer]("score") shouldBe 100
    // record 2 has only one version (no change)
    table.filter("id = 2").count() shouldBe 1L
  }

  it should "treat value-to-NULL change as a new version" in {
    val fc = flowConfig(
      "scd2_to_null",
      loadMode = LoadMode.SCD2,
      compareColumns = Seq("score"),
      validFromColumn = Some("valid_from"),
      validToColumn = Some("valid_to"),
      isCurrentColumn = Some("is_current")
    )

    val initial = Seq((1, 42.asInstanceOf[java.lang.Integer])).toDF("id", "score")
    writer.writeSCD2Load(initial, fc)

    // Now score becomes NULL → that is a change
    val update = Seq((1, null.asInstanceOf[java.lang.Integer])).toDF("id", "score")
    writer.writeSCD2Load(update, fc)

    val table = spark.sql("SELECT * FROM writer_catalog.default.scd2_to_null")
    // old version (score=42) closed + new version (score=NULL) current
    table.count() shouldBe 2L
    table.filter("is_current = true AND score IS NULL").count() shouldBe 1L
    table.filter("is_current = false AND score = 42").count() shouldBe 1L
  }

  it should "handle empty DataFrame on first load without error" in {
    val fc = flowConfig(
      "scd2_empty_first",
      loadMode = LoadMode.SCD2,
      compareColumns = Seq("name"),
      validFromColumn = Some("valid_from"),
      validToColumn = Some("valid_to"),
      isCurrentColumn = Some("is_current")
    )

    val emptyDf = spark.emptyDataFrame
      .selectExpr("CAST(NULL AS INT) AS id", "CAST(NULL AS STRING) AS name")
      .limit(0)
    // Should not throw
    writer.writeSCD2Load(emptyDf, fc)

    spark.sql("SELECT * FROM writer_catalog.default.scd2_empty_first").count() shouldBe 0L

    // Subsequent load with actual data should work normally
    val data = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    writer.writeSCD2Load(data, fc)

    spark
      .sql("SELECT * FROM writer_catalog.default.scd2_empty_first")
      .filter("is_current = true")
      .count() shouldBe 2L
  }

  // --- Full Load with empty DataFrame ---

  "IcebergTableWriter.writeFullLoad" should "overwrite with empty DataFrame clearing all records" in {
    val fc = flowConfig("full_empty")
    val initial = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    writer.writeFullLoad(initial, fc)
    spark.sql("SELECT * FROM writer_catalog.default.full_empty").count() shouldBe 2L

    val emptyDf = spark.createDataFrame(
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      initial.schema
    )
    val result = writer.writeFullLoad(emptyDf, fc)
    result.recordsProcessed shouldBe 0L
    spark.sql("SELECT * FROM writer_catalog.default.full_empty").count() shouldBe 0L
  }

  "IcebergTableWriter.writeDeltaLoad" should "handle PK-only DataFrame without crashing" in {
    val fc = flowConfig("delta_pk_only", loadMode = LoadMode.Delta, primaryKey = Seq("id"))
    val batch1 = Seq(1, 2, 3).toDF("id")
    writer.writeDeltaLoad(batch1, fc)

    val batch2 = Seq(2, 3, 4).toDF("id")
    writer.writeDeltaLoad(batch2, fc)

    // id=4 is new, id=2 and id=3 already exist (no columns to update), id=1 stays
    val rows = spark
      .sql("SELECT id FROM writer_catalog.default.delta_pk_only ORDER BY id")
      .collect()
      .map(_.getInt(0))
    rows shouldBe Array(1, 2, 3, 4)
  }

  // --- Schema evolution + SCD2 ---

  "IcebergTableWriter.writeSCD2Load" should "handle schema evolution with new column on existing SCD2 table" in {
    val fc = flowConfig(
      "scd2_schema_evo",
      loadMode = LoadMode.SCD2,
      compareColumns = Seq("name"),
      validFromColumn = Some("valid_from"),
      validToColumn = Some("valid_to"),
      isCurrentColumn = Some("is_current")
    )

    // Batch 1: initial load with (id, name)
    val batch1 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    writer.writeSCD2Load(batch1, fc)

    spark
      .sql("SELECT * FROM writer_catalog.default.scd2_schema_evo")
      .filter("is_current = true")
      .count() shouldBe 2L

    // Batch 2: source now has a new column (score). Schema evolution should add it.
    val fcWithScore = flowConfig(
      "scd2_schema_evo",
      loadMode = LoadMode.SCD2,
      compareColumns = Seq("name", "score"),
      validFromColumn = Some("valid_from"),
      validToColumn = Some("valid_to"),
      isCurrentColumn = Some("is_current")
    )
    val batch2 = Seq((1, "Alice", 100), (2, "Bob_v2", 200)).toDF("id", "name", "score")
    writer.writeSCD2Load(batch2, fcWithScore)

    val table = spark.sql("SELECT * FROM writer_catalog.default.scd2_schema_evo")

    // Alice: name unchanged but score is new (NULL→100) → new version. Bob: name changed → new version.
    // Total: Alice(old) + Alice(new) + Bob(old) + Bob(new) = 4
    table.count() shouldBe 4L
    table.filter("is_current = true").count() shouldBe 2L

    // New column should exist
    table.columns should contain("score")

    // Current versions should have the new score values
    val aliceCurrent = table.filter("id = 1 AND is_current = true").collect().head
    aliceCurrent.getAs[Int]("score") shouldBe 100

    // Old versions should have score = NULL (written before schema evolution)
    val aliceOld = table.filter("id = 1 AND is_current = false").collect().head
    aliceOld.isNullAt(aliceOld.fieldIndex("score")) shouldBe true
  }

  private val allTables = Seq(
    "full_test",
    "full_overwrite",
    "delta_first",
    "delta_upsert",
    "delta_idempotent",
    "scd2_first",
    "scd2_change",
    "scd2_out_of_order",
    "scd2_duplicate_current",
    "scd2_invalid_current",
    "scd2_same_time_noop",
    "scd2_detect_del",
    "scd2_out_of_order_delete",
    "scd2_no_detect_del",
    "tag_result_test",
    "tracked_commit",
    "fatal_commit",
    "recovery_commit",
    "delta_no_pk",
    "scd2_null_change",
    "scd2_to_null",
    "scd2_empty_first",
    "full_empty",
    "delta_pk_only",
    "scd2_schema_evo"
  )

  override def afterAll(): Unit = {
    allTables.foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS writer_catalog.default.$t")
    }
    super.afterAll()
  }
}
