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

  "OrphanDetector" should "detect orphans when parent Full load removes records" in {
    // Setup parent (customers) and child (orders)
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

    // Day 1: load customers and orders
    val customers1 =
      Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    val parentResult1 = writer.writeFullLoad(customers1, customersConfig)
    val parentSnapshot1 = parentResult1.snapshotId

    val orders =
      Seq((100, 1), (101, 2), (102, 3)).toDF("id", "customer_id")
    writer.writeFullLoad(orders, ordersConfig)

    // Day 2: Full load removes customer 3 (Charlie)
    val customers2 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val parentResult2 = writer.writeFullLoad(customers2, customersConfig)

    // Build flow results with Iceberg metadata
    val flowResults = Seq(
      FlowResult(
        flowName = "od_customers",
        batchId = "test",
        success = true,
        icebergMetadata = Some(
          IcebergFlowMetadata(
            tableName = tableManager.resolveTableName(customersConfig),
            snapshotId = parentResult2.snapshotId.get,
            snapshotTag = None,
            parentSnapshotId = parentSnapshot1,
            snapshotTimestampMs = System.currentTimeMillis(),
            recordsWritten = 2,
            manifestListLocation = "",
            summary = Map.empty
          )
        )
      ),
      FlowResult(flowName = "od_orders", batchId = "test", success = true)
    )

    val flowConfigs = Seq(customersConfig, ordersConfig)
    val plan = ExecutionPlan(
      Seq(
        ExecutionGroup(Seq(customersConfig), parallel = false),
        ExecutionGroup(Seq(ordersConfig), parallel = false)
      )
    )

    val detector =
      new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    reports.size shouldBe 1
    reports.head.flowName shouldBe "od_orders"
    reports.head.fkName shouldBe "(customer_id) -> od_customers.(id)"
    reports.head.parentFlowName shouldBe "od_customers"
    reports.head.orphanCount shouldBe 1
    reports.head.actionTaken shouldBe "warn"

    // Orders should NOT be deleted (warn only)
    val ordersAfter =
      spark.sql("SELECT * FROM orphan_catalog.default.od_orders")
    ordersAfter.count() shouldBe 3
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

    // Day 1
    val customers1 =
      Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    val pr1 = writer.writeFullLoad(customers1, customersConfig)
    val snap1 = pr1.snapshotId

    val orders =
      Seq((100, 1), (101, 2), (102, 3)).toDF("id", "customer_id")
    writer.writeFullLoad(orders, ordersConfig)

    // Day 2: remove customer 3
    val customers2 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val pr2 = writer.writeFullLoad(customers2, customersConfig)

    val flowResults = Seq(
      FlowResult(
        flowName = "od_cust_del",
        batchId = "test",
        success = true,
        icebergMetadata = Some(
          IcebergFlowMetadata(
            tableName = tableManager.resolveTableName(customersConfig),
            snapshotId = pr2.snapshotId.get,
            snapshotTag = None,
            parentSnapshotId = snap1,
            snapshotTimestampMs = System.currentTimeMillis(),
            recordsWritten = 2,
            manifestListLocation = "",
            summary = Map.empty
          )
        )
      ),
      FlowResult(
        flowName = "od_orders_del",
        batchId = "test",
        success = true
      )
    )

    val flowConfigs = Seq(customersConfig, ordersConfig)
    val plan = ExecutionPlan(
      Seq(
        ExecutionGroup(Seq(customersConfig), parallel = false),
        ExecutionGroup(Seq(ordersConfig), parallel = false)
      )
    )

    val detector =
      new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    reports.size shouldBe 1
    reports.head.actionTaken shouldBe "delete"
    reports.head.deletedChildKeyCount shouldBe 1

    // Order 102 (customer_id=3) should be deleted
    val ordersAfter =
      spark.sql("SELECT * FROM orphan_catalog.default.od_orders_del ORDER BY id")
    ordersAfter.count() shouldBe 2
    ordersAfter
      .collect()
      .map(_.getAs[Int]("customer_id")) should contain theSameElementsAs Seq(
      1,
      2
    )
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

    // First execution only
    val customers = Seq((1, "Alice")).toDF("id", "name")
    val pr = writer.writeFullLoad(customers, customersConfig)

    val orders = Seq((100, 1)).toDF("id", "customer_id")
    writer.writeFullLoad(orders, ordersConfig)

    val flowResults = Seq(
      FlowResult(
        flowName = "od_cust_first",
        batchId = "test",
        success = true,
        icebergMetadata = Some(
          IcebergFlowMetadata(
            tableName = tableManager.resolveTableName(customersConfig),
            snapshotId = pr.snapshotId.get,
            snapshotTag = None,
            parentSnapshotId = None, // First execution!
            snapshotTimestampMs = System.currentTimeMillis(),
            recordsWritten = 1,
            manifestListLocation = "",
            summary = Map.empty
          )
        )
      ),
      FlowResult(
        flowName = "od_orders_first",
        batchId = "test",
        success = true
      )
    )

    val flowConfigs = Seq(customersConfig, ordersConfig)
    val plan = ExecutionPlan(
      Seq(
        ExecutionGroup(Seq(customersConfig), parallel = false),
        ExecutionGroup(Seq(ordersConfig), parallel = false)
      )
    )

    val detector =
      new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
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

    val customers1 =
      Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val pr1 = writer.writeFullLoad(customers1, customersConfig)

    val orders = Seq((100, 1), (101, 2)).toDF("id", "customer_id")
    writer.writeFullLoad(orders, ordersConfig)

    val customers2 = Seq((1, "Alice")).toDF("id", "name")
    val pr2 = writer.writeFullLoad(customers2, customersConfig)

    val flowResults = Seq(
      FlowResult(
        flowName = "od_cust_ign",
        batchId = "test",
        success = true,
        icebergMetadata = Some(
          IcebergFlowMetadata(
            tableName = tableManager.resolveTableName(customersConfig),
            snapshotId = pr2.snapshotId.get,
            snapshotTag = None,
            parentSnapshotId = pr1.snapshotId,
            snapshotTimestampMs = System.currentTimeMillis(),
            recordsWritten = 1,
            manifestListLocation = "",
            summary = Map.empty
          )
        )
      ),
      FlowResult(
        flowName = "od_orders_ign",
        batchId = "test",
        success = true
      )
    )

    val flowConfigs = Seq(customersConfig, ordersConfig)
    val plan = ExecutionPlan(
      Seq(
        ExecutionGroup(Seq(customersConfig), parallel = false),
        ExecutionGroup(Seq(ordersConfig), parallel = false)
      )
    )

    val detector =
      new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
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
      FlowResult(
        flowName = "od_parent_delta",
        batchId = "test",
        success = true,
        icebergMetadata = Some(
          IcebergFlowMetadata(
            tableName = tableManager.resolveTableName(parentConfig),
            snapshotId = pr.snapshotId.get,
            snapshotTag = None,
            parentSnapshotId = Some(999L), // has previous
            snapshotTimestampMs = System.currentTimeMillis(),
            recordsWritten = 1,
            manifestListLocation = "",
            summary = Map.empty
          )
        )
      ),
      FlowResult(
        flowName = "od_child_delta",
        batchId = "test",
        success = true
      )
    )

    val flowConfigs = Seq(parentConfig, childConfig)
    val plan = ExecutionPlan(
      Seq(
        ExecutionGroup(Seq(parentConfig), parallel = false),
        ExecutionGroup(Seq(childConfig), parallel = false)
      )
    )

    val detector =
      new OrphanDetector(spark, icebergConfig, flowConfigs, flowResults)
    val reports = detector.detectAndResolveOrphans(plan)

    // Delta parent cannot remove records, so no orphans detected
    reports shouldBe empty
  }

  private val allTables = Seq(
    "od_customers",
    "od_orders",
    "od_cust_del",
    "od_orders_del",
    "od_cust_first",
    "od_orders_first",
    "od_cust_ign",
    "od_orders_ign",
    "od_parent_delta",
    "od_child_delta",
    "od_parent_comp",
    "od_child_comp"
  )

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

    // Batch 1: parent has 3 composite keys
    val parentData1 = Seq(("US", 1), ("US", 2), ("EU", 1)).toDF("region", "dept_id")
    val parentResult1 = writer.writeFullLoad(parentData1, parentConfig)

    val childData = Seq(
      (100, "US", 1),
      (101, "US", 2),
      (102, "EU", 1)
    ).toDF("emp_id", "region", "dept_id")
    writer.writeFullLoad(childData, childConfig)

    // Batch 2: parent removes (US, 2)
    val parentData2 = Seq(("US", 1), ("EU", 1)).toDF("region", "dept_id")
    val parentResult2 = writer.writeFullLoad(parentData2, parentConfig)

    val parentFlowResult = FlowResult(
      flowName = "od_parent_comp",
      batchId = "b2",
      success = true,
      inputRecords = 2,
      mergedRecords = 2,
      validRecords = 2,
      rejectedRecords = 0,
      rejectionRate = 0.0,
      executionTimeMs = 100,
      rejectionReasons = Map.empty,
      icebergMetadata = Some(
        IcebergFlowMetadata(
          tableName = "orphan_catalog.default.od_parent_comp",
          snapshotId = parentResult2.snapshotId.get,
          snapshotTag = None,
          parentSnapshotId = parentResult1.snapshotId,
          snapshotTimestampMs = System.currentTimeMillis(),
          recordsWritten = 2,
          manifestListLocation = "",
          summary = Map.empty
        )
      )
    )
    val childFlowResult = FlowResult(
      flowName = "od_child_comp",
      batchId = "b2",
      success = true,
      inputRecords = 3,
      mergedRecords = 3,
      validRecords = 3,
      rejectedRecords = 0,
      rejectionRate = 0.0,
      executionTimeMs = 100,
      rejectionReasons = Map.empty
    )

    val plan = ExecutionPlan(
      Seq(
        ExecutionGroup(Seq(parentConfig), parallel = false),
        ExecutionGroup(Seq(childConfig), parallel = false)
      )
    )

    val detector =
      new OrphanDetector(spark, icebergConfig, Seq(parentConfig, childConfig), Seq(parentFlowResult, childFlowResult))
    val reports = detector.detectAndResolveOrphans(plan)

    reports should have size 1
    reports.head.orphanCount shouldBe 1 // emp 101 with (US, 2)
    reports.head.actionTaken shouldBe "warn"
  }

  override def afterAll(): Unit = {
    allTables.foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS orphan_catalog.default.$t")
    }
    super.afterAll()
  }
}
