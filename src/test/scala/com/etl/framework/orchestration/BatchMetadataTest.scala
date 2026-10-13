package com.etl.framework.orchestration

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import com.etl.framework.io.readers.{DataReader, DataReaderFactory}
import com.etl.framework.iceberg.{IcebergTableManager, IcebergTableWriter}
import com.etl.framework.orchestration.batch.{BatchIdGenerator, BatchMetadataWriter, FlowGroupExecutor}
import com.etl.framework.orchestration.flow.FlowResult
import com.etl.framework.pipeline.{DerivedTableDefinition, DerivedTableResult}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Try
import scala.concurrent.ExecutionContext

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
      iceberg = IcebergConfig(ddlMode = DdlMode.Automatic, warehouse = warehousePath)
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
      val orchestrator = FlowOrchestrator(createGlobalConfig(tempDir), Seq.empty, pipelineId = "orders-prod")
      val definition = orchestrator.executionDefinition
      val request = ExecutionRequest(
        pipelineId = definition.pipelineId,
        logicalRunId = "scheduled-2026-09-29",
        attemptId = "attempt-01",
        effectiveAt = Instant.parse("2026-09-29T08:00:00Z"),
        codeVersion = definition.codeVersion,
        configDigest = definition.configDigest
      )
      val result = orchestrator.execute(request)

      result.request shouldBe request
      result.status shouldBe ExecutionStatus.Succeeded
      Files.exists(reportPath(tempDir, result)) shouldBe true
    } finally cleanupTempDir(tempDir)
  }

  it should "reject reuse of a single-attempt orchestrator" in {
    val tempDir = Files.createTempDirectory("single-use-orchestrator-test").toString
    try {
      val orchestrator = FlowOrchestrator(createGlobalConfig(tempDir), Seq.empty)

      orchestrator.execute().success shouldBe true

      val error = intercept[IllegalStateException](orchestrator.execute())
      error.getMessage should include("single-use")
    } finally cleanupTempDir(tempDir)
  }

  it should "reject destructive orphan cleanup through the direct orchestrator API" in {
    val tempDir = Files.createTempDirectory("direct-orphan-policy-test").toString
    try {
      val parent = TestFixtures.flowConfig("direct_orphan_parent", sourcePath = "/must-not-be-read/parent")
      val child = TestFixtures.flowConfig(
        "direct_orphan_child",
        sourcePath = "/must-not-be-read/child",
        foreignKeys = Seq(
          ForeignKeyConfig(
            columns = Seq("parent_id"),
            references = ReferenceConfig(parent.name, Seq("id")),
            onOrphan = OrphanAction.Delete
          )
        )
      )

      val error = intercept[IllegalArgumentException] {
        FlowOrchestrator(createGlobalConfig(tempDir), Seq(parent, child)).execute()
      }

      error.getMessage should include("unsupported onOrphan=delete")
    } finally cleanupTempDir(tempDir)
  }

  it should "reject case-insensitive flow name collisions through the direct orchestrator API" in {
    val tempDir = Files.createTempDirectory("direct-flow-collision-test").toString
    try {
      val first = TestFixtures.flowConfig("Direct_Collision", sourcePath = "/must-not-be-read/first")
      val second = TestFixtures.flowConfig("direct_collision", sourcePath = "/must-not-be-read/second")

      val error = intercept[IllegalArgumentException] {
        FlowOrchestrator(createGlobalConfig(tempDir), Seq(first, second)).execute()
      }

      error.getMessage should include("collide case-insensitively")
    } finally cleanupTempDir(tempDir)
  }

  it should "reject derived targets that collide with primary flows before execution" in {
    val tempDir = Files.createTempDirectory("direct-derived-flow-collision-test").toString
    try {
      val flow = TestFixtures.flowConfig("orders", sourcePath = "/must-not-be-read/orders")
      val derived = DerivedTableDefinition("ORDERS", Seq.empty, _ => spark.emptyDataFrame)

      val error = intercept[IllegalArgumentException] {
        FlowOrchestrator(createGlobalConfig(tempDir), Seq(flow), derivedTables = Seq(derived)).execute()
      }

      error.getMessage should include("Derived table 'ORDERS' collides with a managed table")
    } finally cleanupTempDir(tempDir)
  }

  it should "reject derived targets that collide with quality metrics before execution" in {
    val tempDir = Files.createTempDirectory("direct-derived-metrics-collision-test").toString
    try {
      val baseConfig = createGlobalConfig(tempDir)
      val config = baseConfig.copy(
        processing = baseConfig.processing.copy(qualityMetricsTable = Some("quality_metrics"))
      )
      val derived = DerivedTableDefinition("QUALITY_METRICS", Seq.empty, _ => spark.emptyDataFrame)

      val error = intercept[IllegalArgumentException] {
        FlowOrchestrator(config, Seq.empty, derivedTables = Seq(derived)).execute()
      }

      error.getMessage should include("Derived table 'QUALITY_METRICS' collides with a managed table")
    } finally cleanupTempDir(tempDir)
  }

  it should "reject invalid derived dependency graphs before primary flow execution" in {
    val tempDir = Files.createTempDirectory("direct-derived-graph-test").toString
    try {
      val flow = TestFixtures.flowConfig("orders", sourcePath = "/must-not-be-read/orders")
      val derived = DerivedTableDefinition("order_summary", Seq("missing"), _ => spark.emptyDataFrame)

      val error = intercept[IllegalArgumentException] {
        FlowOrchestrator(createGlobalConfig(tempDir), Seq(flow), derivedTables = Seq(derived)).execute()
      }

      error.getMessage should include("unknown dependency 'missing'")
    } finally cleanupTempDir(tempDir)
  }

  it should "reject unknown flow dependencies through the direct orchestrator API" in {
    val tempDir = Files.createTempDirectory("direct-unknown-flow-dependency-test").toString
    try {
      val flow = TestFixtures
        .flowConfig("orders", sourcePath = "/must-not-be-read/orders")
        .copy(dependsOn = Seq("missing"))

      val error = intercept[IllegalArgumentException] {
        FlowOrchestrator(createGlobalConfig(tempDir), Seq(flow)).execute()
      }

      error.getMessage should include("dependsOn references unknown flow 'missing'")
    } finally cleanupTempDir(tempDir)
  }

  it should "reject flow dependency cycles through the direct orchestrator API" in {
    val tempDir = Files.createTempDirectory("direct-flow-cycle-test").toString
    try {
      val first = TestFixtures
        .flowConfig("first", sourcePath = "/must-not-be-read/first")
        .copy(dependsOn = Seq("second"))
      val second = TestFixtures
        .flowConfig("second", sourcePath = "/must-not-be-read/second")
        .copy(dependsOn = Seq("first"))

      val error = intercept[IllegalArgumentException] {
        FlowOrchestrator(createGlobalConfig(tempDir), Seq(first, second)).execute()
      }

      error.getMessage should include("Circular dependency")
    } finally cleanupTempDir(tempDir)
  }

  it should "reject missing referenced columns through the direct orchestrator API" in {
    val tempDir = Files.createTempDirectory("direct-fk-contract-test").toString
    try {
      val parent = TestFixtures.flowConfig(
        "customers",
        columns = Seq(ColumnConfig("id", "string", nullable = false)),
        enforceSchema = true,
        sourcePath = "/must-not-be-read/customers"
      )
      val child = TestFixtures.flowConfig(
        "orders",
        columns = Seq(ColumnConfig("id", "string", nullable = false), ColumnConfig("customer_id", "string", true)),
        enforceSchema = true,
        foreignKeys = Seq(
          ForeignKeyConfig(Seq("customer_id"), ReferenceConfig("customers", Seq("missing_id")))
        ),
        sourcePath = "/must-not-be-read/orders"
      )

      val error = intercept[IllegalArgumentException] {
        FlowOrchestrator(createGlobalConfig(tempDir), Seq(parent, child)).execute()
      }

      error.getMessage should include("references columns not declared by flow 'customers': missing_id")
    } finally cleanupTempDir(tempDir)
  }

  it should "reject unregistered custom readers through the direct orchestrator API" in {
    val tempDir = Files.createTempDirectory("direct-custom-reader-test").toString
    try {
      val base = TestFixtures.flowConfig("orders", sourcePath = "/must-not-be-read/orders")
      val flow = base.copy(source = base.source.copy(`type` = SourceType.Custom("missing-reader")))

      val error = intercept[IllegalArgumentException] {
        FlowOrchestrator(createGlobalConfig(tempDir), Seq(flow)).execute()
      }

      error.getMessage should include("custom source type 'missing-reader' has no registered data reader")
    } finally cleanupTempDir(tempDir)
  }

  it should "reject missing validation domains through the direct orchestrator API" in {
    val tempDir = Files.createTempDirectory("direct-domain-test").toString
    try {
      val flow = TestFixtures.flowConfig(
        "orders",
        primaryKey = Seq.empty,
        columns = Seq(ColumnConfig("country", "string", nullable = true)),
        enforceSchema = true,
        rules = Seq(
          ValidationRule(
            ValidationRuleType.Domain,
            column = Some("country"),
            domainName = Some("countries")
          )
        ),
        sourcePath = "/must-not-be-read/orders"
      )

      val error = intercept[IllegalArgumentException] {
        FlowOrchestrator(createGlobalConfig(tempDir), Seq(flow)).execute()
      }

      error.getMessage should include("domain 'countries' is not configured")
    } finally cleanupTempDir(tempDir)
  }

  it should "reject unresolved custom validators through the direct orchestrator API" in {
    val tempDir = Files.createTempDirectory("direct-custom-validator-test").toString
    try {
      val flow = TestFixtures.flowConfig(
        "orders",
        rules = Seq(
          ValidationRule(
            ValidationRuleType.Custom,
            `class` = Some("com.example.validators.DoesNotExist")
          )
        ),
        sourcePath = "/must-not-be-read/orders"
      )

      val error = intercept[IllegalArgumentException] {
        FlowOrchestrator(createGlobalConfig(tempDir), Seq(flow)).execute()
      }

      error.getMessage should include("neither registered nor a loadable Validator class")
    } finally cleanupTempDir(tempDir)
  }

  it should "reject a request for a different pipeline before executing a flow" in {
    val tempDir = Files.createTempDirectory("pipeline-mismatch-test").toString
    try {
      val orchestrator = FlowOrchestrator(createGlobalConfig(tempDir), Seq.empty, pipelineId = "expected")
      val expected = orchestrator.executionDefinition
      val request = PipelineDefinition("different", expected.codeVersion, expected.configDigest)
        .newRequest("run-1", Instant.parse("2026-09-29T08:00:00Z"))

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
      val path = Paths.get(
        tempDir,
        "metadata",
        result.pipelineId,
        result.logicalRunId,
        result.attemptId,
        "flows",
        s"${flow.name}.json"
      )

      Files.exists(path) shouldBe true
      val content = Files.readString(path)
      Seq("flow_name", "logical_run_id", "attempt_id", "success", "load_mode", "data_outcome")
        .foreach(content should include(_))
    } finally cleanupTempDir(tempDir)
  }

  it should "redact diagnostic secrets even when callers construct result case classes directly" in {
    val tempDir = Files.createTempDirectory("redacted-report-test").toString
    try {
      val config = createGlobalConfig(tempDir)
      val request = ExecutionRequest(
        pipelineId = "redaction-pipeline",
        logicalRunId = "logical-1",
        attemptId = "attempt-1",
        effectiveAt = Instant.parse("2026-10-01T10:00:00Z"),
        codeVersion = "artifact-v1",
        configDigest = "a" * 64
      )
      val secret = "report-secret-value"
      val result = IngestionResult(
        request = request,
        flowResults = Seq(
          FlowResult(
            flowName = "orders",
            batchId = request.attemptId,
            success = false,
            error = Some(s"password=$secret"),
            warnings = Seq(s"Authorization: Bearer $secret")
          )
        ),
        success = false,
        error = Some(s"jdbc:postgresql://alice:$secret@db/orders"),
        derivedTableResults = Seq(
          DerivedTableResult("daily_orders", success = false, error = Some(s"token=$secret"))
        ),
        status = ExecutionStatus.Failed,
        warnings = Seq(s"client_secret=$secret")
      )

      new BatchMetadataWriter(config, Seq.empty).writeBatchMetadata(request, result, executionTimeMs = 10L)

      val content = Files.readString(reportPath(tempDir, result))
      content should not include secret
      content should include("[REDACTED]")
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

  it should "load a dependency at the snapshot produced by its flow instead of mutable HEAD" in {
    val tempDir = Files.createTempDirectory("pinned-dependency-test").toString
    try {
      val flow = createFlow("pinned_dependency", tempDir)
      val config = createGlobalConfig(tempDir)
      val manager = new IcebergTableManager(spark, config.iceberg)
      val writer = new IcebergTableWriter(spark, config.iceberg, manager)
      val first = writer.writeFullLoad(Seq(("old", "v1")).toDF("id", "value"), flow)
      writer.writeFullLoad(Seq(("new", "v2")).toDF("id", "value"), flow)

      val groupExecutor = new FlowGroupExecutor(config, None, ExecutionContext.global)
      val processor = new FlowResultProcessor(config, Seq(flow), groupExecutor)
      val produced = FlowResult.success(
        flow.name,
        "attempt-pinned",
        1L,
        1L,
        1L,
        0L,
        Map.empty,
        first.icebergMetadata,
        first.resultingSnapshotId
      )

      processor.processGroupResults(Seq(produced), BatchState(Seq.empty, Map.empty), "attempt-pinned") match {
        case processor.ContinueWith(state) =>
          state.validatedFlows(flow.name).select("id").as[String].collect().toSeq shouldBe Seq("old")
        case _ => fail("Expected the successful dependency to be available")
      }
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

  it should "surface a quality metrics write failure without downgrading committed data" in {
    val tempDir = Files.createTempDirectory("quality-metrics-warning-test").toString
    val metricsTable = "quality_metrics_warning"
    try {
      spark.sql("CREATE NAMESPACE IF NOT EXISTS floe.default")
      spark.sql(
        s"CREATE TABLE floe.default.$metricsTable (required_extra STRING NOT NULL) USING iceberg"
      )

      val flow = createFlow("quality_metrics_warning_flow", tempDir)
      val baseConfig = createGlobalConfig(tempDir)
      val config = baseConfig.copy(
        processing = baseConfig.processing.copy(qualityMetricsTable = Some(metricsTable))
      )

      val result = FlowOrchestrator(config, Seq(flow)).execute()

      result.success shouldBe true
      result.status shouldBe ExecutionStatus.SucceededWithWarnings
      result.warnings.mkString(" ") should include("Quality metrics write failed")
      result.flowResults.head.dataOutcome shouldBe DataOutcome.Committed
      val report = Files.readString(reportPath(tempDir, result))
      report should include("\"status\":\"SUCCEEDED_WITH_WARNINGS\"")
      report should include("Quality metrics write failed")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS floe.default.$metricsTable")
      cleanupTempDir(tempDir)
    }
  }
}
