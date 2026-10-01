package com.etl.framework.orchestration.recovery

import com.etl.framework.TestFixtures
import com.etl.framework.config.{DdlMode, IcebergConfig}
import com.etl.framework.iceberg.{
  CommitContext,
  DuplicateOperationCommitException,
  IcebergTableManager,
  IcebergTableWriter
}
import com.etl.framework.orchestration.PipelineDefinition
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Instant

class AttemptInspectorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val root: Path = Files.createTempDirectory("attempt-inspector-test")
  private val warehouse = root.resolve("warehouse")
  private val metadata = root.resolve("metadata")
  private var spark: SparkSession = _
  private var icebergConfig: IcebergConfig = _
  private var tableManager: IcebergTableManager = _
  private var writer: IcebergTableWriter = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("AttemptInspectorTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.inspect_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.inspect_catalog.type", "hadoop")
      .config("spark.sql.catalog.inspect_catalog.warehouse", warehouse.toString)
      .getOrCreate()
    icebergConfig = IcebergConfig(
      catalogName = "inspect_catalog",
      ddlMode = DdlMode.Automatic,
      warehouse = warehouse.toString,
      enableSnapshotTagging = false
    )
    tableManager = new IcebergTableManager(spark, icebergConfig)
    writer = new IcebergTableWriter(spark, icebergConfig, tableManager)
  }

  "AttemptInspector" should "report observed, absent, and missing evidence without declaring retry safety" in {
    val session = spark
    import session.implicits._

    val observedFlow = TestFixtures.flowConfig("inspect_observed")
    val unmatchedFlow = TestFixtures.flowConfig("inspect_unmatched")
    val definition = PipelineDefinition("inspect-pipeline", "artifact-v1", "a" * 64)
    val request = definition.newRequest("logical-run-1", Instant.parse("2026-10-01T00:00:00Z"))
    val otherRequest = definition.newRequest("logical-run-other", Instant.parse("2026-10-01T01:00:00Z"))

    writer.writeFullLoad(
      Seq((1, "observed")).toDF("id", "value"),
      observedFlow,
      CommitContext.forFlow(request, observedFlow.name, "full")
    )
    writer.writeFullLoad(
      Seq((2, "other")).toDF("id", "value"),
      unmatchedFlow,
      CommitContext.forFlow(otherRequest, unmatchedFlow.name, "full")
    )
    val report = metadata.resolve(request.artifactKey).resolve("summary.json")
    Files.createDirectories(report.getParent)
    Files.write(report, "{}".getBytes(StandardCharsets.UTF_8)) shouldBe report

    val inspection = new AttemptInspector(spark, icebergConfig, metadata.toString).inspect(
      request,
      Seq(
        target(observedFlow.name),
        target(unmatchedFlow.name),
        target("inspect_missing")
      )
    )

    inspection.complete shouldBe true
    inspection.retrySafe shouldBe false
    inspection.assessment shouldBe "PARTIAL_COMMIT_EVIDENCE"
    inspection.report.status shouldBe "PRESENT"
    inspection.targets.map(target => target.targetName -> target.status).toMap shouldBe Map(
      "inspect_observed" -> "COMMIT_OBSERVED",
      "inspect_unmatched" -> "NO_COMMIT_EVIDENCE",
      "inspect_missing" -> "TABLE_MISSING"
    )
    val observed = inspection.targets.find(_.targetName == observedFlow.name).get
    observed.tableUuid shouldBe defined
    observed.matchingSnapshots should have size 1
    observed.matchingSnapshots.head.relationToMain shouldBe "CURRENT_MAIN"
  }

  it should "surface duplicate operation commits and their relationship to main" in {
    val session = spark
    import session.implicits._

    val flow = TestFixtures.flowConfig("inspect_duplicate")
    val request = PipelineDefinition("inspect-pipeline", "artifact-v1", "b" * 64)
      .newRequest("logical-run-duplicate", Instant.parse("2026-10-01T02:00:00Z"))
    val context = CommitContext.forFlow(request, flow.name, "full")
    writer.writeFullLoad(Seq((1, "first")).toDF("id", "value"), flow, context)
    an[DuplicateOperationCommitException] should be thrownBy writer.writeFullLoad(
      Seq((2, "second")).toDF("id", "value"),
      flow,
      context
    )

    val inspection = new AttemptInspector(spark, icebergConfig, metadata.toString)
      .inspect(request, Seq(target(flow.name)))
    val evidence = inspection.targets.head

    inspection.complete shouldBe true
    inspection.assessment shouldBe "ANOMALOUS_MULTIPLE_COMMITS"
    evidence.status shouldBe "MULTIPLE_COMMITS_OBSERVED"
    evidence.matchingSnapshots.map(_.relationToMain).toSet shouldBe Set("ANCESTOR_OF_MAIN", "CURRENT_MAIN")
  }

  private def target(name: String): InspectionTarget =
    InspectionTarget(name, icebergConfig.fullTableName(name), "full")

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }
}
