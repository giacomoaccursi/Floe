package com.etl.framework.orchestration.batch

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import com.etl.framework.iceberg.{IcebergFlowMetadata, OrphanReport}
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
      .globalConfig(
        iceberg = IcebergConfig(catalogName = "qm_catalog", warehouse = warehousePath)
      )
      .copy(
        processing = ProcessingConfig(
          qualityMetricsTable = if (enabled) Some(tableName) else None
        )
      )

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

  "QualityMetricsWriter" should "create table and write metrics for a batch" in {
    val writer = new QualityMetricsWriter(globalConfig())
    val results = Seq(flowResult("flow_a"), flowResult("flow_b", input = 200, valid = 190, rejected = 10))

    writer.write("batch_001", results, Seq.empty, 1500L)

    val df = spark.sql(s"SELECT * FROM $fullTableName ORDER BY flow_name")
    df.count() shouldBe 2

    val rows = df.collect()
    rows(0).getAs[String]("batch_id") shouldBe "batch_001"
    rows(0).getAs[String]("flow_name") shouldBe "flow_a"
    rows(0).getAs[Long]("input_records") shouldBe 100
    rows(0).getAs[Long]("rejected_records") shouldBe 5
    rows(1).getAs[String]("flow_name") shouldBe "flow_b"
    rows(1).getAs[Long]("input_records") shouldBe 200
  }

  it should "append metrics across multiple batches" in {
    val writer = new QualityMetricsWriter(globalConfig())

    writer.write("batch_002", Seq(flowResult("flow_c")), Seq.empty, 1000L)
    writer.write("batch_003", Seq(flowResult("flow_c")), Seq.empty, 1200L)

    val df = spark.sql(s"SELECT * FROM $fullTableName WHERE flow_name = 'flow_c'")
    df.count() shouldBe 2
    df.select("batch_id").distinct().count() shouldBe 2
  }

  it should "include orphan counts per flow" in {
    val writer = new QualityMetricsWriter(globalConfig())
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

    writer.write("batch_004", Seq(flowResult("flow_d")), orphans, 800L)

    val row = spark
      .sql(s"SELECT orphan_count FROM $fullTableName WHERE batch_id = 'batch_004' AND flow_name = 'flow_d'")
      .collect()
      .head
    row.getAs[Long]("orphan_count") shouldBe 10 // 3 + 7
  }

  it should "do nothing when qualityMetricsTable is not configured" in {
    val writer = new QualityMetricsWriter(globalConfig(enabled = false))

    // Should not throw, should not create table
    noException should be thrownBy writer.write("batch_005", Seq(flowResult("flow_e")), Seq.empty, 500L)
  }

  it should "handle empty flow results" in {
    val writer = new QualityMetricsWriter(globalConfig())

    writer.write("batch_006", Seq.empty, Seq.empty, 100L)

    val df = spark.sql(s"SELECT * FROM $fullTableName WHERE batch_id = 'batch_006'")
    df.count() shouldBe 1
    df.collect().head.getAs[String]("flow_name") shouldBe "batch_summary"
  }

  override def afterAll(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $fullTableName")
    super.afterAll()
  }
}
