package com.etl.framework.orchestration

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}
import scala.collection.mutable

class BatchListenerTest extends AnyFlatSpec with Matchers {

  private val warehousePath = Files.createTempDirectory("listener-test-warehouse").toString

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("BatchListenerTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
    .config("spark.sql.catalog.spark_catalog.warehouse", warehousePath)
    .config("spark.sql.defaultCatalog", "spark_catalog")
    .getOrCreate()

  import spark.implicits._

  private class RecordingListener extends BatchListener {
    val completed: mutable.ListBuffer[IngestionResult] = mutable.ListBuffer()
    val failed: mutable.ListBuffer[IngestionResult] = mutable.ListBuffer()

    override def onBatchCompleted(result: IngestionResult): Unit = completed += result
    override def onBatchFailed(result: IngestionResult): Unit = failed += result
  }

  private def createFlow(tempDir: String): FlowConfig = {
    val inputPath = s"$tempDir/input/test_flow"
    Files.createDirectories(Paths.get(inputPath))
    Seq(("1", "Alice"), ("2", "Bob"))
      .toDF("id", "name")
      .write
      .mode("overwrite")
      .format("csv")
      .option("header", "true")
      .save(inputPath)

    TestFixtures
      .flowConfig(
        name = "test_flow",
        enforceSchema = true,
        allowExtraColumns = false,
        sourcePath = inputPath,
        columns = Seq(
          ColumnConfig("id", "string", nullable = false),
          ColumnConfig("name", "string", nullable = true)
        )
      )
      .copy(source = SourceConfig(path = inputPath, format = Some(FileFormat.CSV), options = Map("header" -> "true")))
  }

  "BatchListener" should "receive onBatchCompleted for successful batch" in {
    val tempDir = Files.createTempDirectory("listener-success-test").toString
    val listener = new RecordingListener()

    val flow = createFlow(tempDir)
    val globalConfig = TestFixtures.globalConfig(
      outputPath = s"$tempDir/output",
      rejectedPath = s"$tempDir/rejected",
      metadataPath = s"$tempDir/metadata",
      iceberg = IcebergConfig(warehouse = warehousePath)
    )

    val orchestrator = FlowOrchestrator(globalConfig, Seq(flow), batchListeners = Seq(listener))
    val result = orchestrator.execute()

    result.success shouldBe true
    listener.completed should have size 1
    listener.failed shouldBe empty
    listener.completed.head.batchId shouldBe result.batchId
  }

  it should "receive onBatchCompleted for empty batch" in {
    val tempDir = Files.createTempDirectory("listener-empty-test").toString
    val listener = new RecordingListener()

    val globalConfig = TestFixtures.globalConfig(
      outputPath = s"$tempDir/output",
      rejectedPath = s"$tempDir/rejected",
      metadataPath = s"$tempDir/metadata",
      iceberg = IcebergConfig(warehouse = warehousePath)
    )

    val orchestrator = FlowOrchestrator(globalConfig, Seq.empty, batchListeners = Seq(listener))
    orchestrator.execute()

    listener.completed should have size 1
    listener.failed shouldBe empty
  }

  it should "support multiple listeners" in {
    val tempDir = Files.createTempDirectory("listener-multi-test").toString
    val listener1 = new RecordingListener()
    val listener2 = new RecordingListener()

    val flow = createFlow(tempDir)
    val globalConfig = TestFixtures.globalConfig(
      outputPath = s"$tempDir/output",
      rejectedPath = s"$tempDir/rejected",
      metadataPath = s"$tempDir/metadata",
      iceberg = IcebergConfig(warehouse = warehousePath)
    )

    val orchestrator = FlowOrchestrator(globalConfig, Seq(flow), batchListeners = Seq(listener1, listener2))
    orchestrator.execute()

    listener1.completed should have size 1
    listener2.completed should have size 1
  }

  it should "not fail the batch when a listener throws" in {
    val tempDir = Files.createTempDirectory("listener-error-test").toString
    val failingListener = new BatchListener {
      override def onBatchCompleted(result: IngestionResult): Unit = throw new RuntimeException("listener boom")
      override def onBatchFailed(result: IngestionResult): Unit = throw new RuntimeException("listener boom")
    }
    val recordingListener = new RecordingListener()

    val flow = createFlow(tempDir)
    val globalConfig = TestFixtures.globalConfig(
      outputPath = s"$tempDir/output",
      rejectedPath = s"$tempDir/rejected",
      metadataPath = s"$tempDir/metadata",
      iceberg = IcebergConfig(warehouse = warehousePath)
    )

    val orchestrator =
      FlowOrchestrator(globalConfig, Seq(flow), batchListeners = Seq(failingListener, recordingListener))
    val result = orchestrator.execute()

    result.success shouldBe true
    recordingListener.completed should have size 1
  }
}
