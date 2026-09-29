package com.etl.framework.orchestration

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import com.etl.framework.io.readers.{DataReader, DataReaderFactory}
import com.etl.framework.orchestration.batch.BatchIdGenerator
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Try

class BatchMetadataTest extends AnyFlatSpec with Matchers {

  private val warehousePath = Files.createTempDirectory("iceberg-warehouse").toString

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("BatchMetadataTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.floe", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.floe.type", "hadoop")
    .config("spark.sql.catalog.floe.warehouse", warehousePath)
    .getOrCreate()

  import spark.implicits._

  private def createFlow(flowName: String, tempDir: String): FlowConfig = {
    val inputPath = s"$tempDir/input/$flowName"
    Files.createDirectories(Paths.get(inputPath))
    Seq(
      (s"${flowName}_1", "value_1"),
      (s"${flowName}_2", "value_2"),
      (s"${flowName}_3", "value_3")
    ).toDF("id", "value").write.mode("overwrite").format("csv").option("header", "true").save(inputPath)

    TestFixtures
      .flowConfig(
        name = flowName,
        enforceSchema = true,
        allowExtraColumns = false,
        sourcePath = inputPath,
        columns = Seq(
          ColumnConfig("id", "string", nullable = false, "Primary key"),
          ColumnConfig("value", "string", nullable = true, "Value")
        ),
        output = OutputConfig(rejectedPath = Some(s"$tempDir/rejected/$flowName"))
      )
      .copy(source = SourceConfig(path = inputPath, format = Some(FileFormat.CSV), options = Map("header" -> "true")))
  }

  private def createGlobalConfig(tempDir: String, batchIdFormat: String = "yyyyMMdd_HHmmss"): GlobalConfig =
    TestFixtures.globalConfig(
      outputPath = s"$tempDir/output",
      rejectedPath = s"$tempDir/rejected",
      metadataPath = s"$tempDir/metadata",
      batchIdFormat = batchIdFormat,
      iceberg = IcebergConfig(warehouse = warehousePath)
    )

  private def reportPath(tempDir: String, result: IngestionResult) =
    Paths.get(
      tempDir,
      "metadata",
      result.pipelineId,
      result.logicalRunId,
      result.attemptId,
      "summary.json"
    )

  private def cleanupTempDir(tempDir: String): Unit = {
    val _ = Try {
      import scala.collection.JavaConverters._
      val dirPath = Paths.get(tempDir)
      if (Files.exists(dirPath))
        Files.walk(dirPath).iterator().asScala.toSeq.reverse.foreach(path => Files.deleteIfExists(path))
    }
  }

  "Batch metadata" should "generate a distinct logical run and attempt for every local execution" in {
    val tempDir = Files.createTempDirectory("execution-id-test").toString
    try {
      val flow = createFlow("unique_execution", tempDir)
      val orchestratorResults = (1 to 3).map(_ => FlowOrchestrator(createGlobalConfig(tempDir), Seq(flow)).execute())

      orchestratorResults.map(_.logicalRunId).distinct should have size 3
      orchestratorResults.map(_.attemptId).distinct should have size 3
      all(orchestratorResults.map(_.pipelineId)) shouldBe "local"
    } finally cleanupTempDir(tempDir)
  }

  it should "generate distinct formatted logical IDs even within one timestamp second" in {
    val ids = (1 to 100).map(_ => BatchIdGenerator.generate("yyyyMMdd_HHmmss"))
    ids.distinct.size shouldBe ids.size
    all(ids) should fullyMatch regex """\d{8}_\d{6}_[0-9a-f]{32}"""
  }

  it should "honor an explicit immutable execution request" in {
    val tempDir = Files.createTempDirectory("explicit-request-test").toString
    try {
      val request = ExecutionRequest(
        pipelineId = "orders-prod",
        logicalRunId = "scheduled-2026-09-29",
        attemptId = "attempt-01",
        effectiveAt = Instant.parse("2026-09-29T08:00:00Z")
      )
      val result = FlowOrchestrator(
        createGlobalConfig(tempDir),
        Seq.empty,
        pipelineId = request.pipelineId
      ).execute(request)

      result.request shouldBe request
      result.status shouldBe ExecutionStatus.Succeeded
      Files.exists(reportPath(tempDir, result)) shouldBe true
    } finally cleanupTempDir(tempDir)
  }

  it should "reject a request for a different pipeline before executing a flow" in {
    val tempDir = Files.createTempDirectory("pipeline-mismatch-test").toString
    try {
      val request = ExecutionRequest.create("different", "run-1", Instant.parse("2026-09-29T08:00:00Z"))
      val orchestrator = FlowOrchestrator(createGlobalConfig(tempDir), Seq.empty, pipelineId = "expected")

      val error = the[IllegalArgumentException] thrownBy orchestrator.execute(request)
      error.getMessage should include("does not match")
    } finally cleanupTempDir(tempDir)
  }

  it should "write a versioned report with execution identity and typed outcomes" in {
    val tempDir = Files.createTempDirectory("report-completeness-test").toString
    try {
      val flows = (1 to 2).map(i => createFlow(s"report_flow_$i", tempDir))
      val result = FlowOrchestrator(createGlobalConfig(tempDir), flows, pipelineId = "report-pipeline").execute()

      val path = reportPath(tempDir, result)
      Files.exists(path) shouldBe true
      val content = Files.readString(path)
      Seq(
        "contract_version",
        "pipeline_id",
        "logical_run_id",
        "attempt_id",
        "effective_at",
        "status",
        "data_outcome",
        "total_input_records",
        "total_valid_records",
        "total_rejected_records"
      ).foreach(content should include(_))
      flows.foreach(flow => content should include(flow.name))
    } finally cleanupTempDir(tempDir)
  }

  it should "write per-flow diagnostics under the attempt identifier" in {
    val tempDir = Files.createTempDirectory("per-flow-metadata-test").toString
    try {
      val flow = createFlow("per_flow_diagnostics", tempDir)
      val result = FlowOrchestrator(createGlobalConfig(tempDir), Seq(flow)).execute()
      val path = Paths.get(tempDir, "metadata", result.attemptId, "flows", s"${flow.name}.json")

      Files.exists(path) shouldBe true
      val content = Files.readString(path)
      Seq("flow_name", "batch_id", "success", "load_mode", "data_outcome").foreach(content should include(_))
    } finally cleanupTempDir(tempDir)
  }

  it should "report a partial failure without retrying either flow" in {
    val tempDir = Files.createTempDirectory("partial-failure-test").toString
    try {
      val reads = new AtomicInteger(0)
      val readerFactory: DataReaderFactory.ReaderFactory = (source, _, session) =>
        new DataReader {
          override def read() = {
            reads.incrementAndGet()
            if (source.path == "failing") throw new RuntimeException("source unavailable")
            import session.implicits._
            Seq(("id-1", "value")).toDF("id", "value")
          }
        }
      val stable = createFlow("partial_stable", tempDir).copy(
        source = SourceConfig(SourceType.Custom("controlled"), path = "stable")
      )
      val failing = createFlow("partial_failing", tempDir).copy(
        source = SourceConfig(SourceType.Custom("controlled"), path = "failing"),
        dependsOn = Seq(stable.name)
      )

      val result = FlowOrchestrator(
        createGlobalConfig(tempDir),
        Seq(stable, failing),
        customReaders = Map("controlled" -> readerFactory)
      ).execute()

      result.success shouldBe false
      result.status shouldBe ExecutionStatus.FailedPartial
      result.flowResults.map(_.dataOutcome) should contain(DataOutcome.Committed)
      reads.get() shouldBe 2
    } finally cleanupTempDir(tempDir)
  }

  it should "keep committed data successful with warnings when a diagnostic output fails" in {
    val tempDir = Files.createTempDirectory("diagnostic-warning-test").toString
    try {
      val base = createFlow("diagnostic_warning_flow", tempDir)
      val flow = base.copy(
        validation = base.validation.copy(
          rules = Seq(
            ValidationRule(
              `type` = ValidationRuleType.Regex,
              column = Some("value"),
              pattern = Some("accepted"),
              onFailure = OnFailureAction.Reject
            )
          )
        ),
        output = base.output.copy(rejectedPath = Some("unsupported-fs://bucket/rejected"))
      )

      val result = FlowOrchestrator(createGlobalConfig(tempDir), Seq(flow)).execute()

      result.success shouldBe true
      result.status shouldBe ExecutionStatus.SucceededWithWarnings
      result.flowResults.head.warnings.mkString(" ") should include("Failed to write rejected records")
      result.flowResults.head.dataOutcome shouldBe DataOutcome.Committed
    } finally cleanupTempDir(tempDir)
  }
}
