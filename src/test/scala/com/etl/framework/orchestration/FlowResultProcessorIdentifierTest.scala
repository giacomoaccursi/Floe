package com.etl.framework.orchestration

import com.etl.framework.TestFixtures
import com.etl.framework.config.{DdlMode, IcebergConfig}
import com.etl.framework.orchestration.batch.FlowGroupExecutor
import com.etl.framework.orchestration.flow.FlowResult
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import scala.concurrent.ExecutionContext

class FlowResultProcessorIdentifierTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val warehouse = Files.createTempDirectory("flow-result-identifier")
  private val icebergConfig = IcebergConfig(
    catalogName = "processor_catalog",
    ddlMode = DdlMode.Automatic,
    warehouse = warehouse.toString
  )

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("FlowResultProcessorIdentifierTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.processor_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.processor_catalog.type", "hadoop")
    .config("spark.sql.catalog.processor_catalog.warehouse", warehouse.toString)
    .getOrCreate()

  import spark.implicits._

  "FlowResultProcessor" should "load a pinned snapshot for a flow name that requires quoting" in {
    val tableName = "processor_catalog.default.`order items`"
    spark.sql(s"CREATE TABLE $tableName (id INT) USING iceberg")
    spark.sql(s"INSERT INTO $tableName VALUES (1)")
    val snapshotId = spark.sql(s"SELECT snapshot_id FROM $tableName.refs WHERE name = 'main'").first().getLong(0)
    val flow = TestFixtures.flowConfig("order items", primaryKey = Seq.empty)
    val globalConfig = TestFixtures.globalConfig(iceberg = icebergConfig)
    val groupExecutor = new FlowGroupExecutor(globalConfig, None, ExecutionContext.global)
    val processor = new FlowResultProcessor(globalConfig, Seq(flow), groupExecutor)
    val result = FlowResult.success(
      flow.name,
      "quoted-attempt",
      inputRecords = 1L,
      mergedRecords = 1L,
      validRecords = 1L,
      rejectedRecords = 0L,
      rejectionReasons = Map.empty,
      resultingSnapshotId = Some(snapshotId)
    )

    processor.processGroupResults(Seq(result), BatchState(Seq.empty, Map.empty), "quoted-attempt") match {
      case processor.ContinueWith(state) =>
        state.validatedFlows(flow.name).select("id").as[Int].head() shouldBe 1
      case stopped => fail(s"Expected ContinueWith, got $stopped")
    }
  }

  override def afterAll(): Unit = {
    try { val _ = spark.sql("DROP TABLE IF EXISTS processor_catalog.default.`order items`") }
    finally spark.stop()
    super.afterAll()
  }
}
