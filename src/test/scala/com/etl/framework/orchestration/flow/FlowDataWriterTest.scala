package com.etl.framework.orchestration.flow

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import com.etl.framework.iceberg.{IcebergTableManager, IcebergTableWriter}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class FlowDataWriterTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("FlowDataWriterTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  import spark.implicits._

  private var tempDir: java.nio.file.Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("flow_data_writer_test")
  }

  override def afterAll(): Unit = {
    import scala.reflect.io.Directory
    new Directory(tempDir.toFile).deleteRecursively()
    super.afterAll()
  }

  private def createGlobalConfig(): GlobalConfig =
    TestFixtures.globalConfig(
      outputPath = tempDir.resolve("output").toString,
      rejectedPath = tempDir.resolve("rejected").toString,
      metadataPath = tempDir.resolve("metadata").toString,
      batchIdFormat = "yyyyMMdd",
      iceberg = IcebergConfig(warehouse = tempDir.resolve("warehouse").toString)
    )

  private def createFlowConfig(name: String, rejectedPath: Option[String] = None): FlowConfig =
    TestFixtures.flowConfig(
      name = name,
      primaryKey = Seq.empty,
      sourcePath = "/tmp/test_source",
      format = FileFormat.Parquet,
      output = OutputConfig(rejectedPath = rejectedPath)
    )

  private def createWriter(flowConfig: FlowConfig, globalConfig: GlobalConfig): FlowDataWriter = {
    val icebergConfig = globalConfig.iceberg
    val tableManager = new IcebergTableManager(spark, icebergConfig)
    val icebergWriter = new IcebergTableWriter(spark, icebergConfig, tableManager)
    new FlowDataWriter(flowConfig, globalConfig, icebergWriter)
  }

  "FlowDataWriter.writeRejected" should "write rejected records to the configured rejectedPath" in {
    val rejectedPath = tempDir.resolve("custom_rejected").toString
    val flowConfig = createFlowConfig("test_flow", rejectedPath = Some(rejectedPath))
    val globalConfig = createGlobalConfig()
    val writer = createWriter(flowConfig, globalConfig)

    val rejectedDF = Seq(("id1", "bad value"), ("id2", "another bad value")).toDF("id", "data")
    writer.writeRejected(rejectedDF, "batch_001")

    val written = spark.read.parquet(rejectedPath)
    written.count() shouldBe 2L
  }

  it should "add BATCH_ID audit column to rejected records" in {
    val rejectedPath = tempDir.resolve("rejected_audit").toString
    val flowConfig = createFlowConfig("audit_flow", rejectedPath = Some(rejectedPath))
    val globalConfig = createGlobalConfig()
    val writer = createWriter(flowConfig, globalConfig)

    val rejectedDF = Seq(("id1", "bad")).toDF("id", "reason")
    writer.writeRejected(rejectedDF, "batch_42")

    val written = spark.read.parquet(rejectedPath)
    written.columns should contain("_batch_id")
    written.first().getAs[String]("_batch_id") shouldBe "batch_42"
  }

  it should "handle empty rejected DataFrame without error" in {
    val rejectedPath = tempDir.resolve("rejected_empty").toString
    val flowConfig = createFlowConfig("empty_flow", rejectedPath = Some(rejectedPath))
    val globalConfig = createGlobalConfig()
    val writer = createWriter(flowConfig, globalConfig)

    val emptyDF = spark.createDataFrame(
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      org.apache.spark.sql.types.StructType(
        Seq(
          org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.StringType)
        )
      )
    )

    noException should be thrownBy writer.writeRejected(emptyDF, "batch_000")

    val written = spark.read.parquet(rejectedPath)
    written.count() shouldBe 0L
  }

  "FlowDataWriter.writeWarnings" should "write warnings to default path when warningsPath is not configured" in {
    val flowConfig = createFlowConfig("warn_flow")
    val globalConfig = createGlobalConfig()
    val writer = createWriter(flowConfig, globalConfig)

    val warnedDF = Seq(("id1", "rule1", "msg1")).toDF("pk", "_warning_rule", "_warning_message")
    writer.writeWarnings(warnedDF, "batch_w1")

    val expectedPath = s"${globalConfig.paths.outputPath}/warnings/warn_flow"
    val written = spark.read.parquet(expectedPath)
    written.count() shouldBe 1L
    written.columns should contain("_batch_id")
  }

  it should "write warnings to custom warningsPath when configured" in {
    val customWarningsPath = tempDir.resolve("custom_warnings").toString
    val flowConfig = createFlowConfig("warn_flow2")
    val globalConfig = createGlobalConfig().copy(
      paths = createGlobalConfig().paths.copy(warningsPath = Some(customWarningsPath))
    )
    val writer = createWriter(flowConfig, globalConfig)

    val warnedDF = Seq(("id1", "rule1", "msg1")).toDF("pk", "_warning_rule", "_warning_message")
    writer.writeWarnings(warnedDF, "batch_w2")

    val expectedPath = s"$customWarningsPath/warn_flow2"
    val written = spark.read.parquet(expectedPath)
    written.count() shouldBe 1L
    written.first().getAs[String]("_batch_id") shouldBe "batch_w2"
  }
}
