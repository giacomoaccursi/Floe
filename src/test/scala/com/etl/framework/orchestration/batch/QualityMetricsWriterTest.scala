package com.etl.framework.orchestration.batch

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import com.etl.framework.iceberg.OrphanReport
import com.etl.framework.orchestration.flow.FlowResult
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class QualityMetricsWriterTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val warehousePath = Files.createTempDirectory("quality-metrics-test").toString

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("QualityMetricsWriterTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.qm_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.qm_catalog.type", "hadoop")
    .config("spark.sql.catalog.qm_catalog.warehouse", warehousePath)
    .getOrCreate()

  private val tableName = "quality_metrics"
  private val fullTableName = s"qm_catalog.default.$tableName"

  private def globalConfig(enabled: Boolean = true): GlobalConfig =
    TestFixtures
      .globalConfig(iceberg = IcebergConfig(catalogName = "qm_catalog", warehouse = warehousePath))
      .copy(processing = ProcessingConfig(qualityMetricsTable = if (enabled) Some(tableName) else None))

  private val flowConfigs = Seq(
    TestFixtures.flowConfig("flow_a"),
    TestFixtures.flowConfig("flow_b"),
    TestFixtures.flowConfig("flow_c"),
    TestFixtures.flowConfig("flow_d"),
    TestFixtures.flowConfig("flow_e", loadMode = LoadMode.Delta)
  )

  private def writer(enabled: Boolean = true) =
    new QualityMetricsWriter(globalConfig(enabled), flowConfigs)

  private def flowResult(
      name: String,
      input: Long = 100,
      valid: Long = 95,
      rejected: Long = 5,
      success: Boolean = true
  ): FlowResult = FlowResult(
    flowName = name,
    batchId = "test",
    success = success,
    inputRecords = input,
    validRecords = valid,
    rejectedRecords = rejected,
    rejectionRate = if (input > 0) rejected.toDouble / input else 0.0,
    executionTimeMs = 500
  )

  "QualityMetricsWriter" should "create table and write metrics with all columns" in {
    val w = writer()
    val results = Seq(flowResult("flow_a"), flowResult("flow_b", input = 200, valid = 190, rejected = 10))

    w.write("batch_001", results, Seq.empty, 1500L, batchSuccess = true)

    val df = spark.sql(s"SELECT * FROM $fullTableName WHERE batch_id = 'batch_001' ORDER BY flow_name")
    df.count() shouldBe 2

    val row = df.collect()(0)
    row.getAs[String]("batch_id") shouldBe "batch_001"
    row.getAs[Boolean]("batch_success") shouldBe true
    row.getAs[String]("flow_name") shouldBe "flow_a"
    row.getAs[String]("load_mode") shouldBe "full"
    row.getAs[Long]("input_records") shouldBe 100
    row.getAs[Long]("valid_records") shouldBe 95
    row.getAs[Long]("rejected_records") shouldBe 5
    row.getAs[Long]("records_written") shouldBe 95 // defaults to validRecords when no iceberg metadata
    row.getAs[Boolean]("success") shouldBe true
  }

  it should "include load_mode from flow config" in {
    val w = writer()
    w.write("batch_lm", Seq(flowResult("flow_e")), Seq.empty, 500L, batchSuccess = true)

    val row = spark.sql(s"SELECT load_mode FROM $fullTableName WHERE batch_id = 'batch_lm'").collect().head
    row.getAs[String]("load_mode") shouldBe "delta"
  }

  it should "set batch_success to false on failed batch" in {
    val w = writer()
    w.write("batch_fail", Seq(flowResult("flow_a", success = false)), Seq.empty, 300L, batchSuccess = false)

    val row =
      spark.sql(s"SELECT batch_success, success FROM $fullTableName WHERE batch_id = 'batch_fail'").collect().head
    row.getAs[Boolean]("batch_success") shouldBe false
    row.getAs[Boolean]("success") shouldBe false
  }

  it should "append metrics across multiple batches" in {
    val w = writer()
    w.write("batch_002", Seq(flowResult("flow_c")), Seq.empty, 1000L, batchSuccess = true)
    w.write("batch_003", Seq(flowResult("flow_c")), Seq.empty, 1200L, batchSuccess = true)

    val df = spark.sql(s"SELECT * FROM $fullTableName WHERE flow_name = 'flow_c'")
    df.count() shouldBe 2
    df.select("batch_id").distinct().count() shouldBe 2
  }

  it should "include orphan counts per flow" in {
    val w = writer()
    val orphans = Seq(
      OrphanReport("flow_d", "fk1", "parent", orphanCount = 3, removedParentKeyCount = 1, actionTaken = "warn"),
      OrphanReport(
        "flow_d",
        "fk2",
        "parent2",
        orphanCount = 7,
        removedParentKeyCount = 2,
        actionTaken = "delete",
        deletedChildKeyCount = 7
      )
    )

    w.write("batch_004", Seq(flowResult("flow_d")), orphans, 800L, batchSuccess = true)

    val row = spark
      .sql(s"SELECT orphan_count FROM $fullTableName WHERE batch_id = 'batch_004' AND flow_name = 'flow_d'")
      .collect()
      .head
    row.getAs[Long]("orphan_count") shouldBe 10
  }

  it should "do nothing when qualityMetricsTable is not configured" in {
    val w = writer(enabled = false)
    noException should be thrownBy w.write("batch_005", Seq(flowResult("flow_e")), Seq.empty, 500L, batchSuccess = true)
  }

  it should "handle empty flow results" in {
    val w = writer()
    w.write("batch_006", Seq.empty, Seq.empty, 100L, batchSuccess = true)

    val df = spark.sql(s"SELECT * FROM $fullTableName WHERE batch_id = 'batch_006'")
    df.count() shouldBe 1
    val row = df.collect().head
    row.getAs[String]("flow_name") shouldBe "batch_summary"
    row.getAs[Boolean]("batch_success") shouldBe true
    row.getAs[String]("load_mode") shouldBe "none"
  }

  override def afterAll(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $fullTableName")
    super.afterAll()
  }
}
