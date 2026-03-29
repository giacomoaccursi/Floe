package com.etl.framework.orchestration

import com.etl.framework.config._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}
import scala.util.Try

class BatchMetadataTest extends AnyFlatSpec with Matchers {

  private val warehousePath = Files.createTempDirectory("iceberg-warehouse").toString

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("BatchMetadataTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
    .config("spark.sql.catalog.spark_catalog.warehouse", warehousePath)
    .config("spark.sql.defaultCatalog", "spark_catalog")
    .getOrCreate()

  import spark.implicits._

  private def createFlow(flowName: String, tempDir: String): FlowConfig = {
    val inputPath = s"$tempDir/input/$flowName"
    Files.createDirectories(Paths.get(inputPath))

    val testData = Seq(
      (s"${flowName}_1", "value_1"),
      (s"${flowName}_2", "value_2"),
      (s"${flowName}_3", "value_3")
    ).toDF("id", "value")

    testData.write
      .mode("overwrite")
      .format("csv")
      .option("header", "true")
      .save(inputPath)

    FlowConfig(
      name = flowName,
      description = s"Test flow $flowName",
      version = "1.0",
      owner = "test",
      source = SourceConfig(
        `type` = SourceType.File,
        path = inputPath,
        format = FileFormat.CSV,
        options = Map("header" -> "true")
      ),
      schema = SchemaConfig(
        enforceSchema = true,
        allowExtraColumns = false,
        columns = Seq(
          ColumnConfig("id", "string", nullable = false, "Primary key"),
          ColumnConfig("value", "string", nullable = true, "Value")
        )
      ),
      loadMode = LoadModeConfig(`type` = LoadMode.Full),
      validation = ValidationConfig(
        primaryKey = Seq("id"),
        foreignKeys = Seq.empty,
        rules = Seq.empty
      ),
      output = OutputConfig(
        rejectedPath = Some(s"$tempDir/rejected/$flowName")
      )
    )
  }

  private def createGlobalConfig(
      tempDir: String,
      batchIdFormat: String = "yyyyMMdd_HHmmss"
  ): GlobalConfig = {
    GlobalConfig(
      paths = PathsConfig(
        outputPath = s"$tempDir/output",
        rejectedPath = s"$tempDir/rejected",
        metadataPath = s"$tempDir/metadata"
      ),
      processing = ProcessingConfig(
        batchIdFormat = batchIdFormat
      ),
      performance = PerformanceConfig(
        parallelFlows = false,
      ),
      iceberg = IcebergConfig(warehouse = warehousePath)
    )
  }

  private def cleanupTempDir(tempDir: String): Unit = {
    Try {
      import scala.collection.JavaConverters._
      val dirPath = Paths.get(tempDir)
      if (Files.exists(dirPath)) {
        Files
          .walk(dirPath)
          .iterator()
          .asScala
          .toSeq
          .reverse
          .foreach(p => Files.deleteIfExists(p))
      }
    }
  }

  "Batch metadata" should "generate unique batch IDs across executions" in {
    val tempDir = Files.createTempDirectory("batch-id-test").toString
    try {
      val flow = createFlow("test_flow", tempDir)
      val globalConfig = createGlobalConfig(tempDir, "timestamp")

      val batchIds = (1 to 3).map { _ =>
        val orchestrator = FlowOrchestrator(globalConfig, Seq(flow))
        val result = orchestrator.execute()
        Thread.sleep(10)
        result.batchId
      }

      batchIds.distinct.size shouldBe batchIds.size
    } finally {
      cleanupTempDir(tempDir)
    }
  }

  it should "follow configured format for timestamp" in {
    val tempDir = Files.createTempDirectory("batch-format-test").toString
    try {
      val flow = createFlow("test_flow", tempDir)
      val globalConfig = createGlobalConfig(tempDir, "timestamp")

      val orchestrator = FlowOrchestrator(globalConfig, Seq(flow))
      val result = orchestrator.execute()

      result.batchId.forall(_.isDigit) shouldBe true
      result.batchId.length should be >= 13
    } finally {
      cleanupTempDir(tempDir)
    }
  }

  it should "follow configured format for datetime" in {
    val tempDir = Files.createTempDirectory("batch-format-test").toString
    try {
      val flow = createFlow("test_flow", tempDir)
      val globalConfig = createGlobalConfig(tempDir, "yyyyMMdd_HHmmss")

      val orchestrator = FlowOrchestrator(globalConfig, Seq(flow))
      val result = orchestrator.execute()

      result.batchId should fullyMatch regex """\d{8}_\d{6}"""
    } finally {
      cleanupTempDir(tempDir)
    }
  }

  it should "contain all required fields in summary metadata" in {
    val tempDir =
      Files.createTempDirectory("metadata-completeness-test").toString
    try {
      val flows = (1 to 2).map(i => createFlow(s"flow_$i", tempDir))
      val globalConfig = createGlobalConfig(tempDir)

      val orchestrator = FlowOrchestrator(globalConfig, flows)
      val result = orchestrator.execute()

      val metadataPath =
        Paths.get(s"$tempDir/metadata/${result.batchId}/summary.json")
      Files.exists(metadataPath) shouldBe true

      val metadataContent = new String(Files.readAllBytes(metadataPath))

      metadataContent should include("batch_id")
      metadataContent should include("total_input_records")
      metadataContent should include("total_valid_records")
      metadataContent should include("total_rejected_records")
      metadataContent should include("overall_rejection_rate")
      metadataContent should include("flows_processed")
      metadataContent should include("success")

      flows.foreach { flow =>
        metadataContent should (
          include(s""""flow_name":"${flow.name}"""") or
            include(s""""flow_name" : "${flow.name}"""")
        )
      }
    } finally {
      cleanupTempDir(tempDir)
    }
  }

  it should "contain required fields in per-flow metadata" in {
    val tempDir =
      Files.createTempDirectory("per-flow-metadata-test").toString
    try {
      val flow = createFlow("test_flow", tempDir)
      val globalConfig = createGlobalConfig(tempDir)

      val orchestrator = FlowOrchestrator(globalConfig, Seq(flow))
      val result = orchestrator.execute()

      val flowMetadataPath = Paths.get(
        s"$tempDir/metadata/${result.batchId}/flows/${flow.name}.json"
      )
      Files.exists(flowMetadataPath) shouldBe true

      val metadataContent =
        new String(Files.readAllBytes(flowMetadataPath))

      metadataContent should include("flow_name")
      metadataContent should include("batch_id")
      metadataContent should include("success")
      metadataContent should include("input_records")
      metadataContent should include("valid_records")
      metadataContent should include("rejected_records")
      metadataContent should include("rejection_rate")
      metadataContent should include("execution_time_ms")
    } finally {
      cleanupTempDir(tempDir)
    }
  }

  it should "produce valid metadata for empty flow list" in {
    val tempDir =
      Files.createTempDirectory("empty-metadata-test").toString
    try {
      val globalConfig = createGlobalConfig(tempDir)

      val orchestrator = FlowOrchestrator(globalConfig, Seq.empty)
      val result = orchestrator.execute()

      val metadataPath =
        Paths.get(s"$tempDir/metadata/${result.batchId}/summary.json")
      Files.exists(metadataPath) shouldBe true

      val metadataContent = new String(Files.readAllBytes(metadataPath))

      metadataContent should (
        include(""""flows_processed":0""") or
          include(""""flows_processed" : 0""")
      )
    } finally {
      cleanupTempDir(tempDir)
    }
  }
}
