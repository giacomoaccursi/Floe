package com.etl.framework.orchestration

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import com.etl.framework.io.readers.{DataReader, DataReaderFactory}
import com.etl.framework.iceberg.MaintenanceStatus
import com.etl.framework.orchestration.batch.BatchIdGenerator
import com.etl.framework.orchestration.maintenance.MaintenanceWorker
import com.etl.framework.orchestration.state.{
  InMemoryRunStore,
  MaintenanceTaskRecord,
  OperationStatus,
  ReleaseManifest,
  RunRecord,
  RunStatus,
  SnapshotPinnedReader
}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
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
        output = OutputConfig(
          rejectedPath = Some(s"$tempDir/rejected/$flowName")
        )
      )
      .copy(source = SourceConfig(path = inputPath, format = Some(FileFormat.CSV), options = Map("header" -> "true")))
  }

  private def createGlobalConfig(
      tempDir: String,
      batchIdFormat: String = "yyyyMMdd_HHmmss"
  ): GlobalConfig =
    TestFixtures.globalConfig(
      outputPath = s"$tempDir/output",
      rejectedPath = s"$tempDir/rejected",
      metadataPath = s"$tempDir/metadata",
      batchIdFormat = batchIdFormat,
      iceberg = IcebergConfig(warehouse = warehousePath)
    )

  private def cleanupTempDir(tempDir: String): Unit = {
    val _ = Try {
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

  it should "derive a stable pipeline identity from config and an explicit code version" in {
    val base = TestFixtures.flowConfig("pipeline_identity")
    val first = base.copy(preValidationTransformation = Some(context => context))
    val second = base.copy(preValidationTransformation = Some(context => context))
    val config = createGlobalConfig(Files.createTempDirectory("pipeline-id-test").toString)

    ReleaseManifest.pipelineId(config, Seq(first), Seq("derived"), "release-a") shouldBe
      ReleaseManifest.pipelineId(config, Seq(second), Seq("derived"), "release-a")
    ReleaseManifest.pipelineId(config, Seq(first), Seq("derived"), "release-a") should not be
      ReleaseManifest.pipelineId(config, Seq(first), Seq("derived"), "release-b")
  }

  it should "generate distinct IDs even within one timestamp second" in {
    val ids = (1 to 100).map(_ => BatchIdGenerator.generate("yyyyMMdd_HHmmss"))
    ids.distinct.size shouldBe ids.size
  }

  it should "follow configured format for timestamp" in {
    val tempDir = Files.createTempDirectory("batch-format-test").toString
    try {
      val flow = createFlow("test_flow", tempDir)
      val globalConfig = createGlobalConfig(tempDir, "timestamp")

      val orchestrator = FlowOrchestrator(globalConfig, Seq(flow))
      val result = orchestrator.execute()

      result.batchId should fullyMatch regex """\d{13,}_[0-9a-f]{32}"""
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

      result.batchId should fullyMatch regex """\d{8}_\d{6}_[0-9a-f]{32}"""
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

  it should "serialize load_mode as string in per-flow metadata" in {
    val tempDir = Files.createTempDirectory("load-mode-metadata-test").toString
    try {
      val flow = createFlow("test_flow", tempDir)
      val globalConfig = createGlobalConfig(tempDir)

      val orchestrator = FlowOrchestrator(globalConfig, Seq(flow))
      val result = orchestrator.execute()

      val flowMetadataPath = Paths.get(
        s"$tempDir/metadata/${result.batchId}/flows/${flow.name}.json"
      )
      val metadataContent = new String(Files.readAllBytes(flowMetadataPath))

      // load_mode should be the string "full", not an object like {} or {"name":"full"}
      metadataContent should include(""""load_mode":"full"""")
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

  it should "queue maintenance outside ingestion and persist worker failures as warnings" in {
    val tempDir = Files.createTempDirectory("maintenance-status-test").toString
    try {
      val flow = createFlow("maintenance_flow", tempDir)
      val globalConfig = createGlobalConfig(tempDir)
      val runStore = new InMemoryRunStore()

      val result = FlowOrchestrator(
        globalConfig,
        Seq(flow),
        runStore = runStore
      ).execute()

      result.success shouldBe true
      result.status shouldBe RunStatus.Published
      result.maintenanceResults should have size 1
      result.maintenanceResults.head.targetName shouldBe flow.name
      result.maintenanceResults.head.targetType shouldBe "flow"
      result.maintenanceResults.head.status shouldBe MaintenanceStatus.Queued

      val worker = new MaintenanceWorker(
        globalConfig.iceberg,
        runStore,
        executor = Some(_ => throw new RuntimeException("compaction unavailable"))
      )
      val workerResults = worker.runPending()
      workerResults.head.status shouldBe MaintenanceStatus.Failed
      workerResults.head.error should contain("compaction unavailable")
      runStore.getRun(result.batchId).get.status shouldBe RunStatus.SucceededWithWarnings
      runStore.getMaintenanceTasks(Some(result.batchId)).head.status shouldBe MaintenanceStatus.Failed

      val metadataPath = Paths.get(s"$tempDir/metadata/${result.batchId}/summary.json")
      val metadataContent = new String(Files.readAllBytes(metadataPath))
      metadataContent should include("queued")
    } finally {
      cleanupTempDir(tempDir)
    }
  }

  it should "not run maintenance before its batch is published" in {
    val store = new InMemoryRunStore()
    val now = Instant.now()
    val run = RunRecord.planned("maintenance-publication-gate", "pipeline", now)
    store.createRun(run)
    store.transitionRun(run.batchId, run.version, RunStatus.Running) shouldBe true
    store.enqueueMaintenance(
      MaintenanceTaskRecord.queued(run.batchId, "customers", "flow", "floe.default.customers")
    )

    val executed = new AtomicBoolean(false)
    val worker = new MaintenanceWorker(
      createGlobalConfig(warehousePath).iceberg,
      store,
      executor = Some(_ => executed.set(true))
    )

    worker.runPending() shouldBe empty
    executed.get() shouldBe false
    store.getMaintenanceTasks(Some(run.batchId)).head.status shouldBe MaintenanceStatus.Queued

    val running = store.getRun(run.batchId).get
    store.transitionRun(run.batchId, running.version, RunStatus.Published) shouldBe true
    worker.runPending() should have size 1
    executed.get() shouldBe true
  }

  it should "publish an atomic release manifest and durable operation state" in {
    val tempDir = Files.createTempDirectory("release-manifest-test").toString
    try {
      val flow = createFlow("published_flow", tempDir)
      val globalConfig = createGlobalConfig(tempDir)
      val runStore = new InMemoryRunStore()

      val result = FlowOrchestrator(globalConfig, Seq(flow), runStore = runStore).execute()

      result.status shouldBe RunStatus.Published
      val persisted = runStore.getRun(result.batchId).get
      persisted.status shouldBe RunStatus.Published
      persisted.lease shouldBe None
      val operation = runStore.getOperations(result.batchId).head
      operation.status shouldBe OperationStatus.Committed
      operation.snapshotId shouldBe defined

      val manifest = ReleaseManifest.fromJson(persisted.releaseManifest.get)
      manifest.batchId shouldBe result.batchId
      manifest.targets.map(_.targetName) should contain only "published_flow"
      new SnapshotPinnedReader(manifest).table("published_flow").count() shouldBe 3L
    } finally {
      cleanupTempDir(tempDir)
    }
  }

  it should "resume a partial batch without rewriting an already committed target" in {
    val tempDir = Files.createTempDirectory("batch-resume-test").toString
    try {
      val stable = createFlow("resume_stable", tempDir).copy(
        source = SourceConfig(
          `type` = SourceType.Custom("resumable"),
          path = "stable",
          options = Map("replayToken" -> "immutable-input-v1")
        )
      )
      val unstable = createFlow("resume_unstable", tempDir).copy(
        source = SourceConfig(
          `type` = SourceType.Custom("resumable"),
          path = "unstable",
          options = Map("replayToken" -> "immutable-input-v1")
        )
      )
      val failOnce = new AtomicBoolean(true)
      val readerFactory: DataReaderFactory.ReaderFactory = (source, _, session) =>
        new DataReader {
          override def read() = {
            if (source.path == "unstable" && failOnce.compareAndSet(true, false))
              throw new RuntimeException("transient source outage")
            import session.implicits._
            Seq((source.path + "_1", "value")).toDF("id", "value")
          }
        }
      val globalConfig = createGlobalConfig(tempDir)
      val runStore = new InMemoryRunStore()

      val first = FlowOrchestrator(
        globalConfig,
        Seq(stable, unstable),
        customReaders = Map("resumable" -> readerFactory),
        runStore = runStore
      ).execute()

      first.status shouldBe RunStatus.FailedPartial
      val stableBefore = runStore.getOperations(first.batchId).find(_.targetName == stable.name).get.snapshotId
      stableBefore shouldBe defined

      val resumed = FlowOrchestrator(
        globalConfig,
        Seq(stable, unstable),
        customReaders = Map("resumable" -> readerFactory),
        runStore = runStore
      ).resume(first.batchId)

      resumed.status shouldBe RunStatus.Published
      resumed.success shouldBe true
      val operations = runStore.getOperations(first.batchId).map(operation => operation.targetName -> operation).toMap
      operations(stable.name).snapshotId shouldBe stableBefore
      operations(stable.name).status shouldBe OperationStatus.ReconciledCommitted
      operations(unstable.name).status shouldBe OperationStatus.Committed
      spark.table(globalConfig.iceberg.fullTableName(stable.name)).count() shouldBe 1L
      spark.table(globalConfig.iceberg.fullTableName(unstable.name)).count() shouldBe 1L
    } finally {
      cleanupTempDir(tempDir)
    }
  }

  it should "replay immutable inputs under a new linked batch" in {
    val tempDir = Files.createTempDirectory("batch-replay-test").toString
    try {
      val flow = createFlow("replay_flow", tempDir)
      val globalConfig = createGlobalConfig(tempDir)
      val runStore = new InMemoryRunStore()

      val first = FlowOrchestrator(globalConfig, Seq(flow), runStore = runStore).execute()
      val replayed = FlowOrchestrator(globalConfig, Seq(flow), runStore = runStore).replay(first.batchId)

      replayed.success shouldBe true
      replayed.status shouldBe RunStatus.Published
      replayed.batchId should not be first.batchId
      runStore.getRun(replayed.batchId).get.replayOf should contain(first.batchId)
      runStore.getOperations(replayed.batchId).head.operationId should not be
        runStore.getOperations(first.batchId).head.operationId
    } finally {
      cleanupTempDir(tempDir)
    }
  }

  it should "publish committed data with warnings when a diagnostic side output fails" in {
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
      val globalConfig = createGlobalConfig(tempDir)
      val runStore = new InMemoryRunStore()

      val result = FlowOrchestrator(globalConfig, Seq(flow), runStore = runStore).execute()

      result.success shouldBe true
      result.status shouldBe RunStatus.SucceededWithWarnings
      result.flowResults.head.warnings.mkString(" ") should include("Failed to write rejected records")
      runStore.getOperations(result.batchId).head.status shouldBe OperationStatus.Committed
      spark.table(globalConfig.iceberg.fullTableName(flow.name)).count() shouldBe 0L
    } finally {
      cleanupTempDir(tempDir)
    }
  }
}
