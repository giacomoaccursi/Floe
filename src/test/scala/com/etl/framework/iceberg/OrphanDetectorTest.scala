package com.etl.framework.iceberg

import com.etl.framework.config._
import com.etl.framework.orchestration.{ExecutionGroup, ExecutionPlan}
import com.etl.framework.orchestration.flow.FlowResult
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}

class OrphanDetectorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var warehousePath: Path = _

  implicit val spark: SparkSession = {
    val tmpDir = Files.createTempDirectory("orphan-detector-test")
    warehousePath = tmpDir

    SparkSession.getActiveSession.foreach(_.stop())

    SparkSession
      .builder()
      .appName("OrphanDetectorTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "1")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config(
        "spark.sql.catalog.orphan_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config("spark.sql.catalog.orphan_catalog.type", "hadoop")
      .config(
        "spark.sql.catalog.orphan_catalog.warehouse",
        tmpDir.toString
      )
      .getOrCreate()
  }

  import spark.implicits._

  private val icebergConfig = IcebergConfig(
    catalogName = "orphan_catalog",
    warehouse = warehousePath.toString,
    enableSnapshotTagging = false
  )

  private val tableManager = new IcebergTableManager(spark, icebergConfig)
  private val writer = new IcebergTableWriter(spark, icebergConfig, tableManager)

  private def makeFlowConfig(
      name: String,
      loadMode: LoadMode = LoadMode.Full,
      primaryKey: Seq[String] = Seq("id"),
      foreignKeys: Seq[ForeignKeyConfig] = Seq.empty,
      detectDeletes: Boolean = false
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
        compareColumns = if (loadMode == LoadMode.SCD2) Seq("name") else Seq.empty,
        validFromColumn = if (loadMode == LoadMode.SCD2) Some("valid_from") else None,
        validToColumn = if (loadMode == LoadMode.SCD2) Some("valid_to") else None,
        isCurrentColumn = if (loadMode == LoadMode.SCD2) Some("is_current") else None,
        detectDeletes = detectDeletes,
        isActiveColumn = if (detectDeletes) Some("is_active") else None
      ),
      validation = ValidationConfig(
        primaryKey = primaryKey,
        foreignKeys = foreignKeys,
        rules = Seq.empty
      ),
      output = OutputConfig()
    )
  }

  private def makeFlowResult(
      flowName: String,
      snapshotId: Option[Long],
      parentSnapshotId: Option[Long],
      tableName: Option[String] = None
  ): FlowResult = {
    val meta = snapshotId.map { sid =>
      IcebergFlowMetadata(
        tableName = tableName.getOrElse(s"orphan_catalog.default.$flowName"),
        snapshotId = sid,
        snapshotTag = None,
        parentSnapshotId = parentSnapshotId,
        snapshotTimestampMs = System.currentTimeMillis(),
        recordsWritten = 0,
        manifestListLocation = "",
        summary = Map.empty
      )
    }
    FlowResult(flowName = flowName, batchId = "test", success = true, icebergMetadata = meta)
  }

  // ── Existing tests ──────────────────────────────────────────────────

  "OrphanDetector" should "detect orphans when parent Full load removes records" in {
    val customersConfig = makeFlowConfig("od_customers")
    val ordersConfig = makeFlowConfig(
      "od_orders",
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("customer_id"),
          references = ReferenceConfig(flow = "od_customers", columns = Seq("id")),
          onOrphan = OrphanAction.Warn
        )
      )
    )

    val customers1 = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    val pr1 = writer.writeFullLoad(customers1, customersConfig)

    val orders = Seq((100, 1), (101, 2), (102, 3)).toDF("id", "customer_id")
    writer.writeFullLoad(orders, ordersConfig)

    val customers2 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val pr2 = writer.writeFullLoad(customers2, customersConfig)

    val flowResults = Seq(
      makeFlowResult("od_customers", pr2.snapshotId, pr1.snapshotId,
        Some(tableManager.resolveTableName(customersConfig))),
      makeFlowResult("od_orders", None, None)
    )
    val flowConfigs = Seq(customersConfig, ordersConfig)
    val plan = ExecutionPlan(Seq(
      ExecutionGroup(Seq(customersConfig), parallel = false),
      ExecutionGroup(Seq(ordersConfig), parallel = false)
    ))

    val detector = new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    reports.size shouldBe 1
    reports.head.flowName shouldBe "od_orders"
    reports.head.fkName shouldBe "(customer_id) -> od_customers.(id)"
    reports.head.parentFlowName shouldBe "od_customers"
    reports.head.orphanCount shouldBe 1
    reports.head.actionTaken shouldBe "warn"
    reports.head.cascadeSource shouldBe None

    spark.sql("SELECT * FROM orphan_catalog.default.od_orders").count() shouldBe 3
  }

  it should "delete orphaned records when onOrphan=Delete" in {
    val customersConfig = makeFlowConfig("od_cust_del")
    val ordersConfig = makeFlowConfig(
      "od_orders_del",
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("customer_id"),
          references = ReferenceConfig(flow = "od_cust_del", columns = Seq("id")),
          onOrphan = OrphanAction.Delete
        )
      )
    )

    val customers1 = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    val pr1 = writer.writeFullLoad(customers1, customersConfig)

    val orders = Seq((100, 1), (101, 2), (102, 3)).toDF("id", "customer_id")
    writer.writeFullLoad(orders, ordersConfig)

    val customers2 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val pr2 = writer.writeFullLoad(customers2, customersConfig)

    val flowResults = Seq(
      makeFlowResult("od_cust_del", pr2.snapshotId, pr1.snapshotId,
        Some(tableManager.resolveTableName(customersConfig))),
      makeFlowResult("od_orders_del", None, None)
    )
    val flowConfigs = Seq(customersConfig, ordersConfig)
    val plan = ExecutionPlan(Seq(
      ExecutionGroup(Seq(customersConfig), parallel = false),
      ExecutionGroup(Seq(ordersConfig), parallel = false)
    ))

    val detector = new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    reports.size shouldBe 1
    reports.head.actionTaken shouldBe "delete"
    reports.head.deletedChildKeyCount shouldBe 1
    reports.head.cascadeSource shouldBe None

    val ordersAfter = spark.sql("SELECT * FROM orphan_catalog.default.od_orders_del ORDER BY id")
    ordersAfter.count() shouldBe 2
    ordersAfter.collect().map(_.getAs[Int]("customer_id")) should contain theSameElementsAs Seq(1, 2)
  }

  it should "skip orphan check when parent has no previous snapshot" in {
    val customersConfig = makeFlowConfig("od_cust_first")
    val ordersConfig = makeFlowConfig(
      "od_orders_first",
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("customer_id"),
          references = ReferenceConfig(flow = "od_cust_first", columns = Seq("id")),
          onOrphan = OrphanAction.Warn
        )
      )
    )

    val customers = Seq((1, "Alice")).toDF("id", "name")
    val pr = writer.writeFullLoad(customers, customersConfig)

    val orders = Seq((100, 1)).toDF("id", "customer_id")
    writer.writeFullLoad(orders, ordersConfig)

    val flowResults = Seq(
      makeFlowResult("od_cust_first", pr.snapshotId, None,
        Some(tableManager.resolveTableName(customersConfig))),
      makeFlowResult("od_orders_first", None, None)
    )
    val flowConfigs = Seq(customersConfig, ordersConfig)
    val plan = ExecutionPlan(Seq(
      ExecutionGroup(Seq(customersConfig), parallel = false),
      ExecutionGroup(Seq(ordersConfig), parallel = false)
    ))

    val detector = new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    reports shouldBe empty
  }

  it should "return empty reports when onOrphan=Ignore" in {
    val customersConfig = makeFlowConfig("od_cust_ign")
    val ordersConfig = makeFlowConfig(
      "od_orders_ign",
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("customer_id"),
          references = ReferenceConfig(flow = "od_cust_ign", columns = Seq("id")),
          onOrphan = OrphanAction.Ignore
        )
      )
    )

    val customers1 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val pr1 = writer.writeFullLoad(customers1, customersConfig)

    val orders = Seq((100, 1), (101, 2)).toDF("id", "customer_id")
    writer.writeFullLoad(orders, ordersConfig)

    val customers2 = Seq((1, "Alice")).toDF("id", "name")
    val pr2 = writer.writeFullLoad(customers2, customersConfig)

    val flowResults = Seq(
      makeFlowResult("od_cust_ign", pr2.snapshotId, pr1.snapshotId,
        Some(tableManager.resolveTableName(customersConfig))),
      makeFlowResult("od_orders_ign", None, None)
    )
    val flowConfigs = Seq(customersConfig, ordersConfig)
    val plan = ExecutionPlan(Seq(
      ExecutionGroup(Seq(customersConfig), parallel = false),
      ExecutionGroup(Seq(ordersConfig), parallel = false)
    ))

    val detector = new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    reports shouldBe empty
  }

  it should "skip orphan check for Delta parent" in {
    val parentConfig = makeFlowConfig("od_parent_delta", loadMode = LoadMode.Delta)
    val childConfig = makeFlowConfig(
      "od_child_delta",
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("parent_id"),
          references = ReferenceConfig(flow = "od_parent_delta", columns = Seq("id")),
          onOrphan = OrphanAction.Warn
        )
      )
    )

    val parentData = Seq((1, "Alice")).toDF("id", "name")
    val pr = writer.writeDeltaLoad(parentData, parentConfig)

    val childData = Seq((100, 1)).toDF("id", "parent_id")
    writer.writeFullLoad(childData, childConfig)

    val flowResults = Seq(
      makeFlowResult("od_parent_delta", pr.snapshotId, Some(999L),
        Some(tableManager.resolveTableName(parentConfig))),
      makeFlowResult("od_child_delta", None, None)
    )
    val flowConfigs = Seq(parentConfig, childConfig)
    val plan = ExecutionPlan(Seq(
      ExecutionGroup(Seq(parentConfig), parallel = false),
      ExecutionGroup(Seq(childConfig), parallel = false)
    ))

    val detector = new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    reports shouldBe empty
  }

  it should "detect orphans with composite foreign keys" in {
    val parentConfig = makeFlowConfig("od_parent_comp", primaryKey = Seq("region", "dept_id"))
    val childConfig = makeFlowConfig(
      "od_child_comp",
      primaryKey = Seq("emp_id"),
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("region", "dept_id"),
          references = ReferenceConfig(flow = "od_parent_comp", columns = Seq("region", "dept_id")),
          onOrphan = OrphanAction.Warn
        )
      )
    )

    val parentData1 = Seq(("US", 1), ("US", 2), ("EU", 1)).toDF("region", "dept_id")
    val pr1 = writer.writeFullLoad(parentData1, parentConfig)

    val childData = Seq((100, "US", 1), (101, "US", 2), (102, "EU", 1)).toDF("emp_id", "region", "dept_id")
    writer.writeFullLoad(childData, childConfig)

    val parentData2 = Seq(("US", 1), ("EU", 1)).toDF("region", "dept_id")
    val pr2 = writer.writeFullLoad(parentData2, parentConfig)

    val flowResults = Seq(
      makeFlowResult("od_parent_comp", pr2.snapshotId, pr1.snapshotId,
        Some(tableManager.resolveTableName(parentConfig))),
      makeFlowResult("od_child_comp", None, None)
    )
    val plan = ExecutionPlan(Seq(
      ExecutionGroup(Seq(parentConfig), parallel = false),
      ExecutionGroup(Seq(childConfig), parallel = false)
    ))

    val detector = new OrphanDetector(spark, icebergConfig, Seq(parentConfig, childConfig), flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    reports should have size 1
    reports.head.orphanCount shouldBe 1
    reports.head.actionTaken shouldBe "warn"
  }

  // ── New tests: cascade ──────────────────────────────────────────────

  it should "cascade delete from parent to child to grandchild" in {
    // grandparent: customers → child: orders → grandchild: order_lines
    val customersConfig = makeFlowConfig("od_casc_cust")
    val ordersConfig = makeFlowConfig(
      "od_casc_orders",
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("customer_id"),
          references = ReferenceConfig(flow = "od_casc_cust", columns = Seq("id")),
          onOrphan = OrphanAction.Delete
        )
      )
    )
    val linesConfig = makeFlowConfig(
      "od_casc_lines",
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("order_id"),
          references = ReferenceConfig(flow = "od_casc_orders", columns = Seq("id")),
          onOrphan = OrphanAction.Delete
        )
      )
    )

    // Batch 1: load all three levels
    val customers1 = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    val pr1 = writer.writeFullLoad(customers1, customersConfig)

    val orders = Seq((10, 1), (20, 2), (30, 3)).toDF("id", "customer_id")
    writer.writeFullLoad(orders, ordersConfig)

    val lines = Seq((100, 10), (200, 20), (300, 30), (301, 30)).toDF("id", "order_id")
    writer.writeFullLoad(lines, linesConfig)

    // Batch 2: remove customer 3
    val customers2 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val pr2 = writer.writeFullLoad(customers2, customersConfig)

    val flowConfigs = Seq(customersConfig, ordersConfig, linesConfig)
    val flowResults = Seq(
      makeFlowResult("od_casc_cust", pr2.snapshotId, pr1.snapshotId,
        Some(tableManager.resolveTableName(customersConfig))),
      makeFlowResult("od_casc_orders", None, None),
      makeFlowResult("od_casc_lines", None, None)
    )
    val plan = ExecutionPlan(Seq(
      ExecutionGroup(Seq(customersConfig), parallel = false),
      ExecutionGroup(Seq(ordersConfig), parallel = false),
      ExecutionGroup(Seq(linesConfig), parallel = false)
    ))

    val detector = new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    // Should have 2 reports: orders and lines
    reports should have size 2

    val ordersReport = reports.find(_.flowName == "od_casc_orders").get
    ordersReport.actionTaken shouldBe "delete"
    ordersReport.orphanCount shouldBe 1
    ordersReport.cascadeSource shouldBe None

    val linesReport = reports.find(_.flowName == "od_casc_lines").get
    linesReport.actionTaken shouldBe "delete"
    linesReport.orphanCount shouldBe 2 // lines 300 and 301 reference order 30
    linesReport.cascadeSource shouldBe Some("od_casc_orders")

    // Verify data
    spark.sql("SELECT * FROM orphan_catalog.default.od_casc_orders").count() shouldBe 2
    spark.sql("SELECT * FROM orphan_catalog.default.od_casc_lines").count() shouldBe 2
  }

  it should "not cascade when child uses Warn (cascade stops)" in {
    val customersConfig = makeFlowConfig("od_casc_warn_cust")
    val ordersConfig = makeFlowConfig(
      "od_casc_warn_orders",
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("customer_id"),
          references = ReferenceConfig(flow = "od_casc_warn_cust", columns = Seq("id")),
          onOrphan = OrphanAction.Warn // Warn, not Delete
        )
      )
    )
    val linesConfig = makeFlowConfig(
      "od_casc_warn_lines",
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("order_id"),
          references = ReferenceConfig(flow = "od_casc_warn_orders", columns = Seq("id")),
          onOrphan = OrphanAction.Delete
        )
      )
    )

    val customers1 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val pr1 = writer.writeFullLoad(customers1, customersConfig)

    val orders = Seq((10, 1), (20, 2)).toDF("id", "customer_id")
    writer.writeFullLoad(orders, ordersConfig)

    val lines = Seq((100, 10), (200, 20)).toDF("id", "order_id")
    writer.writeFullLoad(lines, linesConfig)

    val customers2 = Seq((1, "Alice")).toDF("id", "name")
    val pr2 = writer.writeFullLoad(customers2, customersConfig)

    val flowConfigs = Seq(customersConfig, ordersConfig, linesConfig)
    val flowResults = Seq(
      makeFlowResult("od_casc_warn_cust", pr2.snapshotId, pr1.snapshotId,
        Some(tableManager.resolveTableName(customersConfig))),
      makeFlowResult("od_casc_warn_orders", None, None),
      makeFlowResult("od_casc_warn_lines", None, None)
    )
    val plan = ExecutionPlan(Seq(
      ExecutionGroup(Seq(customersConfig), parallel = false),
      ExecutionGroup(Seq(ordersConfig), parallel = false),
      ExecutionGroup(Seq(linesConfig), parallel = false)
    ))

    val detector = new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    // Only 1 report: orders (warn). Lines should NOT be affected because orders didn't delete.
    reports should have size 1
    reports.head.flowName shouldBe "od_casc_warn_orders"
    reports.head.actionTaken shouldBe "warn"

    // All data intact
    spark.sql("SELECT * FROM orphan_catalog.default.od_casc_warn_orders").count() shouldBe 2
    spark.sql("SELECT * FROM orphan_catalog.default.od_casc_warn_lines").count() shouldBe 2
  }

  // ── New tests: multiple FKs on same child ───────────────────────────

  it should "handle multiple FKs on same child with Delete, accumulating cascade keys" in {
    // child has FK to two different parents, both Delete
    val parent1Config = makeFlowConfig("od_multi_p1")
    val parent2Config = makeFlowConfig("od_multi_p2")
    val childConfig = makeFlowConfig(
      "od_multi_child",
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("p1_id"),
          references = ReferenceConfig(flow = "od_multi_p1", columns = Seq("id")),
          onOrphan = OrphanAction.Delete
        ),
        ForeignKeyConfig(
          columns = Seq("p2_id"),
          references = ReferenceConfig(flow = "od_multi_p2", columns = Seq("id")),
          onOrphan = OrphanAction.Delete
        )
      )
    )

    // Batch 1
    val p1Data1 = Seq((1, "A"), (2, "B")).toDF("id", "name")
    val p1r1 = writer.writeFullLoad(p1Data1, parent1Config)

    val p2Data1 = Seq((10, "X"), (20, "Y")).toDF("id", "name")
    val p2r1 = writer.writeFullLoad(p2Data1, parent2Config)

    // child: 4 records, each referencing one parent
    val childData = Seq(
      (100, 1, 10),
      (101, 2, 10),
      (102, 1, 20),
      (103, 2, 20)
    ).toDF("id", "p1_id", "p2_id")
    writer.writeFullLoad(childData, childConfig)

    // Batch 2: remove parent1.id=2 and parent2.id=20
    val p1Data2 = Seq((1, "A")).toDF("id", "name")
    val p1r2 = writer.writeFullLoad(p1Data2, parent1Config)

    val p2Data2 = Seq((10, "X")).toDF("id", "name")
    val p2r2 = writer.writeFullLoad(p2Data2, parent2Config)

    val flowConfigs = Seq(parent1Config, parent2Config, childConfig)
    val flowResults = Seq(
      makeFlowResult("od_multi_p1", p1r2.snapshotId, p1r1.snapshotId,
        Some(tableManager.resolveTableName(parent1Config))),
      makeFlowResult("od_multi_p2", p2r2.snapshotId, p2r1.snapshotId,
        Some(tableManager.resolveTableName(parent2Config))),
      makeFlowResult("od_multi_child", None, None)
    )
    val plan = ExecutionPlan(Seq(
      ExecutionGroup(Seq(parent1Config), parallel = false),
      ExecutionGroup(Seq(parent2Config), parallel = false),
      ExecutionGroup(Seq(childConfig), parallel = false)
    ))

    val detector = new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    // FK1 (p1_id→p1): orphans are records 101, 103 (p1_id=2)
    // After FK1 delete: remaining records are 100 (p1=1,p2=10) and 102 (p1=1,p2=20)
    // FK2 (p2_id→p2): orphan is record 102 (p2_id=20)
    reports should have size 2

    val fk1Report = reports.find(_.fkName.contains("p1_id")).get
    fk1Report.actionTaken shouldBe "delete"
    fk1Report.orphanCount shouldBe 2

    val fk2Report = reports.find(_.fkName.contains("p2_id")).get
    fk2Report.actionTaken shouldBe "delete"
    fk2Report.orphanCount shouldBe 1

    // Only record 100 (p1=1, p2=10) should survive
    val remaining = spark.sql("SELECT * FROM orphan_catalog.default.od_multi_child")
    remaining.count() shouldBe 1
    remaining.collect().head.getAs[Int]("id") shouldBe 100
  }

  // ── New test: no orphans found (parent removes keys but child doesn't reference them) ──

  it should "return empty reports when removed parent keys have no child references" in {
    val customersConfig = makeFlowConfig("od_no_orphan_cust")
    val ordersConfig = makeFlowConfig(
      "od_no_orphan_orders",
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("customer_id"),
          references = ReferenceConfig(flow = "od_no_orphan_cust", columns = Seq("id")),
          onOrphan = OrphanAction.Delete
        )
      )
    )

    // Batch 1: customers 1,2,3 but orders only reference 1,2
    val customers1 = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    val pr1 = writer.writeFullLoad(customers1, customersConfig)

    val orders = Seq((100, 1), (101, 2)).toDF("id", "customer_id")
    writer.writeFullLoad(orders, ordersConfig)

    // Batch 2: remove customer 3 (no orders reference it)
    val customers2 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val pr2 = writer.writeFullLoad(customers2, customersConfig)

    val flowConfigs = Seq(customersConfig, ordersConfig)
    val flowResults = Seq(
      makeFlowResult("od_no_orphan_cust", pr2.snapshotId, pr1.snapshotId,
        Some(tableManager.resolveTableName(customersConfig))),
      makeFlowResult("od_no_orphan_orders", None, None)
    )
    val plan = ExecutionPlan(Seq(
      ExecutionGroup(Seq(customersConfig), parallel = false),
      ExecutionGroup(Seq(ordersConfig), parallel = false)
    ))

    val detector = new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    reports shouldBe empty
    spark.sql("SELECT * FROM orphan_catalog.default.od_no_orphan_orders").count() shouldBe 2
  }

  // ── New test: parent not in batch results ──

  it should "skip orphan check when parent flow is not in batch results" in {
    val ordersConfig = makeFlowConfig(
      "od_missing_parent_orders",
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("customer_id"),
          references = ReferenceConfig(flow = "nonexistent_flow", columns = Seq("id")),
          onOrphan = OrphanAction.Delete
        )
      )
    )

    val orders = Seq((100, 1)).toDF("id", "customer_id")
    writer.writeFullLoad(orders, ordersConfig)

    val flowConfigs = Seq(ordersConfig)
    val flowResults = Seq(makeFlowResult("od_missing_parent_orders", None, None))
    val plan = ExecutionPlan(Seq(
      ExecutionGroup(Seq(ordersConfig), parallel = false)
    ))

    val detector = new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    reports shouldBe empty
  }

  // ── New test: multiple parents remove keys affecting same child ──

  it should "delete orphans from multiple removed parent keys in same batch" in {
    val customersConfig = makeFlowConfig("od_multi_rem_cust")
    val ordersConfig = makeFlowConfig(
      "od_multi_rem_orders",
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("customer_id"),
          references = ReferenceConfig(flow = "od_multi_rem_cust", columns = Seq("id")),
          onOrphan = OrphanAction.Delete
        )
      )
    )

    // Batch 1: 4 customers, orders for all
    val customers1 = Seq((1, "A"), (2, "B"), (3, "C"), (4, "D")).toDF("id", "name")
    val pr1 = writer.writeFullLoad(customers1, customersConfig)

    val orders = Seq((10, 1), (20, 2), (30, 3), (40, 4)).toDF("id", "customer_id")
    writer.writeFullLoad(orders, ordersConfig)

    // Batch 2: remove customers 3 and 4
    val customers2 = Seq((1, "A"), (2, "B")).toDF("id", "name")
    val pr2 = writer.writeFullLoad(customers2, customersConfig)

    val flowConfigs = Seq(customersConfig, ordersConfig)
    val flowResults = Seq(
      makeFlowResult("od_multi_rem_cust", pr2.snapshotId, pr1.snapshotId,
        Some(tableManager.resolveTableName(customersConfig))),
      makeFlowResult("od_multi_rem_orders", None, None)
    )
    val plan = ExecutionPlan(Seq(
      ExecutionGroup(Seq(customersConfig), parallel = false),
      ExecutionGroup(Seq(ordersConfig), parallel = false)
    ))

    val detector = new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    reports should have size 1
    reports.head.orphanCount shouldBe 2
    reports.head.deletedChildKeyCount shouldBe 2

    spark.sql("SELECT * FROM orphan_catalog.default.od_multi_rem_orders").count() shouldBe 2
  }

  // ── New test: composite FK delete with cascade ──

  it should "cascade delete with composite foreign keys" in {
    val parentConfig = makeFlowConfig("od_casc_comp_parent", primaryKey = Seq("region", "dept_id"))
    val childConfig = makeFlowConfig(
      "od_casc_comp_child",
      primaryKey = Seq("emp_id"),
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("region", "dept_id"),
          references = ReferenceConfig(flow = "od_casc_comp_parent", columns = Seq("region", "dept_id")),
          onOrphan = OrphanAction.Delete
        )
      )
    )
    val grandchildConfig = makeFlowConfig(
      "od_casc_comp_gc",
      primaryKey = Seq("task_id"),
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("emp_id"),
          references = ReferenceConfig(flow = "od_casc_comp_child", columns = Seq("emp_id")),
          onOrphan = OrphanAction.Delete
        )
      )
    )

    // Batch 1
    val parentData1 = Seq(("US", 1), ("US", 2), ("EU", 1)).toDF("region", "dept_id")
    val pr1 = writer.writeFullLoad(parentData1, parentConfig)

    val childData = Seq((100, "US", 1), (101, "US", 2), (102, "EU", 1)).toDF("emp_id", "region", "dept_id")
    writer.writeFullLoad(childData, childConfig)

    val gcData = Seq((1000, 100), (1001, 101), (1002, 101), (1003, 102)).toDF("task_id", "emp_id")
    writer.writeFullLoad(gcData, grandchildConfig)

    // Batch 2: remove (US, 2)
    val parentData2 = Seq(("US", 1), ("EU", 1)).toDF("region", "dept_id")
    val pr2 = writer.writeFullLoad(parentData2, parentConfig)

    val flowConfigs = Seq(parentConfig, childConfig, grandchildConfig)
    val flowResults = Seq(
      makeFlowResult("od_casc_comp_parent", pr2.snapshotId, pr1.snapshotId,
        Some(tableManager.resolveTableName(parentConfig))),
      makeFlowResult("od_casc_comp_child", None, None),
      makeFlowResult("od_casc_comp_gc", None, None)
    )
    val plan = ExecutionPlan(Seq(
      ExecutionGroup(Seq(parentConfig), parallel = false),
      ExecutionGroup(Seq(childConfig), parallel = false),
      ExecutionGroup(Seq(grandchildConfig), parallel = false)
    ))

    val detector = new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    reports should have size 2

    val childReport = reports.find(_.flowName == "od_casc_comp_child").get
    childReport.orphanCount shouldBe 1 // emp 101
    childReport.cascadeSource shouldBe None

    val gcReport = reports.find(_.flowName == "od_casc_comp_gc").get
    gcReport.orphanCount shouldBe 2 // tasks 1001, 1002
    gcReport.cascadeSource shouldBe Some("od_casc_comp_child")

    spark.sql("SELECT * FROM orphan_catalog.default.od_casc_comp_child").count() shouldBe 2
    spark.sql("SELECT * FROM orphan_catalog.default.od_casc_comp_gc").count() shouldBe 2
  }

  // ── New test: child with empty PK (no cascade possible) ──

  it should "delete orphans but not cascade when child has no primary key" in {
    val parentConfig = makeFlowConfig("od_nopk_parent")
    val childConfig = makeFlowConfig(
      "od_nopk_child",
      primaryKey = Seq.empty,
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("parent_id"),
          references = ReferenceConfig(flow = "od_nopk_parent", columns = Seq("id")),
          onOrphan = OrphanAction.Delete
        )
      )
    )

    val parentData1 = Seq((1, "A"), (2, "B")).toDF("id", "name")
    val pr1 = writer.writeFullLoad(parentData1, parentConfig)

    val childData = Seq((100, 1), (200, 2)).toDF("value", "parent_id")
    writer.writeFullLoad(childData, childConfig)

    val parentData2 = Seq((1, "A")).toDF("id", "name")
    val pr2 = writer.writeFullLoad(parentData2, parentConfig)

    val flowConfigs = Seq(parentConfig, childConfig)
    val flowResults = Seq(
      makeFlowResult("od_nopk_parent", pr2.snapshotId, pr1.snapshotId,
        Some(tableManager.resolveTableName(parentConfig))),
      makeFlowResult("od_nopk_child", None, None)
    )
    val plan = ExecutionPlan(Seq(
      ExecutionGroup(Seq(parentConfig), parallel = false),
      ExecutionGroup(Seq(childConfig), parallel = false)
    ))

    val detector = new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    reports should have size 1
    reports.head.actionTaken shouldBe "delete"
    reports.head.orphanCount shouldBe 1

    spark.sql("SELECT * FROM orphan_catalog.default.od_nopk_child").count() shouldBe 1
  }

  private val allTables = Seq(
    "od_customers", "od_orders",
    "od_cust_del", "od_orders_del",
    "od_cust_first", "od_orders_first",
    "od_cust_ign", "od_orders_ign",
    "od_parent_delta", "od_child_delta",
    "od_parent_comp", "od_child_comp",
    "od_casc_cust", "od_casc_orders", "od_casc_lines",
    "od_casc_warn_cust", "od_casc_warn_orders", "od_casc_warn_lines",
    "od_multi_p1", "od_multi_p2", "od_multi_child",
    "od_no_orphan_cust", "od_no_orphan_orders",
    "od_missing_parent_orders",
    "od_multi_rem_cust", "od_multi_rem_orders",
    "od_casc_comp_parent", "od_casc_comp_child", "od_casc_comp_gc",
    "od_nopk_parent", "od_nopk_child"
  )

  override def afterAll(): Unit = {
    allTables.foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS orphan_catalog.default.$t")
    }
    super.afterAll()
  }
}
