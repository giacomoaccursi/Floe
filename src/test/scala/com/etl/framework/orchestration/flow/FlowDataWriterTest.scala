package com.etl.framework.orchestration.flow

import com.etl.framework.config._
import com.etl.framework.core.AdditionalTableMetadata
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
    GlobalConfig(
      paths = PathsConfig(
        outputPath = tempDir.resolve("output").toString,
        rejectedPath = tempDir.resolve("rejected").toString,
        metadataPath = tempDir.resolve("metadata").toString
      ),
      processing = ProcessingConfig(
        batchIdFormat = "yyyyMMdd",
        failOnValidationError = false,
        maxRejectionRate = 0.1
      ),
      performance = PerformanceConfig(parallelFlows = false, parallelNodes = false),
      iceberg = IcebergConfig(warehouse = tempDir.resolve("warehouse").toString)
    )

  private def createFlowConfig(name: String, rejectedPath: Option[String] = None): FlowConfig =
    FlowConfig(
      name = name,
      description = "Test flow",
      version = "1.0",
      owner = "test",
      source = SourceConfig(SourceType.File, "/tmp/test_source", FileFormat.Parquet, Map.empty),
      schema = SchemaConfig(enforceSchema = false, allowExtraColumns = true, Seq.empty),
      loadMode = LoadModeConfig(LoadMode.Full),
      validation = ValidationConfig(Seq.empty, Seq.empty, Seq.empty),
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

  it should "add REJECTED_AT and BATCH_ID audit columns to rejected records" in {
    val rejectedPath = tempDir.resolve("rejected_audit").toString
    val flowConfig = createFlowConfig("audit_flow", rejectedPath = Some(rejectedPath))
    val globalConfig = createGlobalConfig()
    val writer = createWriter(flowConfig, globalConfig)

    val rejectedDF = Seq(("id1", "bad")).toDF("id", "reason")
    writer.writeRejected(rejectedDF, "batch_42")

    val written = spark.read.parquet(rejectedPath)
    written.columns should contain("_rejected_at")
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
      org.apache.spark.sql.types.StructType(Seq(
        org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.StringType)
      ))
    )

    noException should be thrownBy writer.writeRejected(emptyDF, "batch_000")

    val written = spark.read.parquet(rejectedPath)
    written.count() shouldBe 0L
  }

  "FlowDataWriter.writeAdditionalTable" should "write data to the specified output path" in {
    val outputPath = tempDir.resolve("additional_table_out").toString
    val flowConfig = createFlowConfig("addl_flow")
    val globalConfig = createGlobalConfig()
    val writer = createWriter(flowConfig, globalConfig)

    val data = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
    writer.writeAdditionalTable("my_table", data, Some(outputPath), None)

    val written = spark.read.parquet(outputPath)
    written.count() shouldBe 3L
    written.columns should contain allOf("id", "value")
  }

  it should "partition data when dagMetadata specifies partitionBy" in {
    val outputPath = tempDir.resolve("additional_partitioned").toString
    val flowConfig = createFlowConfig("partitioned_flow")
    val globalConfig = createGlobalConfig()
    val writer = createWriter(flowConfig, globalConfig)

    val data = Seq((1, "2024"), (2, "2024"), (3, "2025")).toDF("id", "year")
    val dagMetadata = AdditionalTableMetadata(
      primaryKey = Seq("id"),
      partitionBy = Seq("year")
    )
    writer.writeAdditionalTable("partitioned_table", data, Some(outputPath), Some(dagMetadata))

    val written = spark.read.parquet(outputPath)
    written.count() shouldBe 3L

    val partitionDirs = new java.io.File(outputPath).listFiles().filter(_.isDirectory).map(_.getName)
    partitionDirs should contain("year=2024")
    partitionDirs should contain("year=2025")
  }

  it should "write without partitioning when dagMetadata has empty partitionBy" in {
    val outputPath = tempDir.resolve("additional_no_partition").toString
    val flowConfig = createFlowConfig("no_partition_flow")
    val globalConfig = createGlobalConfig()
    val writer = createWriter(flowConfig, globalConfig)

    val data = Seq((1, "x"), (2, "y")).toDF("id", "value")
    val dagMetadata = AdditionalTableMetadata(
      primaryKey = Seq("id"),
      partitionBy = Seq.empty
    )
    writer.writeAdditionalTable("no_partition_table", data, Some(outputPath), Some(dagMetadata))

    val written = spark.read.parquet(outputPath)
    written.count() shouldBe 2L
  }
}
