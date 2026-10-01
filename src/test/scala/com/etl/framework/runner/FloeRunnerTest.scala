package com.etl.framework.runner

import com.etl.framework.orchestration.ExecutionRequestJson
import com.etl.framework.pipeline.IngestionPipeline
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Instant

class FloeRunnerTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val root: Path = Files.createTempDirectory("floe-runner-test")
  private val configDirectory = root.resolve("config")
  private val failingConfigDirectory = root.resolve("failing-config")
  private val requestPath = root.resolve("request.json")
  private val failingRequestPath = root.resolve("failing-request.json")

  implicit private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("FloeRunnerTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()
    Files.createDirectories(configDirectory.resolve("flows"))
    Files.createDirectories(failingConfigDirectory.resolve("flows"))
    val yaml =
      s"""paths:
         |  outputPath: "${root.resolve("output")}"
         |  rejectedPath: "${root.resolve("rejected")}"
         |  metadataPath: "${root.resolve("metadata")}"
         |iceberg:
         |  catalogMode: configure
         |  ddlMode: automatic
         |  catalogType: hadoop
         |  catalogName: runner_catalog
         |  warehouse: "${root.resolve("warehouse")}"
         |""".stripMargin
    Files.write(configDirectory.resolve("global.yaml"), yaml.getBytes(StandardCharsets.UTF_8))
    Files.write(failingConfigDirectory.resolve("global.yaml"), yaml.getBytes(StandardCharsets.UTF_8))
    val failingFlow =
      s"""name: missing-input
         |source:
         |  type: file
         |  path: "${root.resolve("does-not-exist.csv")}"
         |  format: csv
         |schema:
         |  enforceSchema: true
         |  columns:
         |    - name: id
         |      type: integer
         |      nullable: false
         |loadMode:
         |  type: full
         |validation:
         |  primaryKey: [id]
         |""".stripMargin
    Files.write(
      failingConfigDirectory.resolve("flows").resolve("missing-input.yaml"),
      failingFlow.getBytes(StandardCharsets.UTF_8)
    )
    ()
  }

  "FloeRunner" should "validate a YAML-only pipeline and expose its immutable digest" in {
    val (exitCode, stdout, stderr) = execute(
      Seq(
        "validate",
        "--config",
        configDirectory.toString,
        "--pipeline-id",
        "runner-pipeline",
        "--code-version",
        "artifact-v1"
      )
    )

    withClue(stderr) {
      exitCode shouldBe FloeRunner.ExitCode.Success
    }
    stdout should include("\"status\":\"STATIC_VALID\"")
    stdout should include("\"config_digest\"")
    stderr shouldBe empty
    spark.sparkContext.isStopped shouldBe false
  }

  it should "execute a persisted request and leave a caller-owned SparkSession running" in {
    val pipeline = IngestionPipeline
      .builder()
      .withConfigDirectory(configDirectory.toString)
      .withPipelineId("runner-pipeline")
      .withCodeVersion("artifact-v1")
      .build()
    val request = pipeline.executionDefinition.newRequest("logical-run-1", Instant.parse("2026-10-01T08:00:00Z"))
    Files.write(requestPath, ExecutionRequestJson.toJson(request).getBytes(StandardCharsets.UTF_8))

    val (exitCode, stdout, stderr) = execute(
      Seq("run", "--config", configDirectory.toString, "--request", requestPath.toString)
    )

    exitCode shouldBe FloeRunner.ExitCode.Success
    stdout should include("\"status\":\"SUCCEEDED\"")
    stdout should include("\"attempt_id\":\"")
    stderr shouldBe empty
    spark.sparkContext.isStopped shouldBe false
    Files.exists(
      root
        .resolve("metadata")
        .resolve(request.pipelineId)
        .resolve(request.logicalRunId)
        .resolve(request.attemptId)
        .resolve("summary.json")
    ) shouldBe true
  }

  it should "return a usage error without starting work" in {
    val (exitCode, _, stderr) = execute(Seq("run", "--config", configDirectory.toString))

    exitCode shouldBe FloeRunner.ExitCode.Usage
    stderr should include("--request")
  }

  it should "inspect a completed attempt without mutating it or authorizing a retry" in {
    val pipeline = IngestionPipeline
      .builder()
      .withConfigDirectory(configDirectory.toString)
      .withPipelineId("runner-pipeline")
      .withCodeVersion("artifact-v1")
      .build()
    val request = pipeline.executionDefinition.newRequest("logical-inspect", Instant.parse("2026-10-01T08:30:00Z"))
    Files.write(requestPath, ExecutionRequestJson.toJson(request).getBytes(StandardCharsets.UTF_8))
    val (runExitCode, _, runStderr) = execute(
      Seq("run", "--config", configDirectory.toString, "--request", requestPath.toString)
    )
    withClue(runStderr) {
      runExitCode shouldBe FloeRunner.ExitCode.Success
    }

    val (exitCode, stdout, stderr) = execute(
      Seq("inspect", "--config", configDirectory.toString, "--request", requestPath.toString)
    )

    exitCode shouldBe FloeRunner.ExitCode.Success
    stdout should include("\"status\":\"INSPECTION_COMPLETED\"")
    stdout should include("\"assessment\":\"NO_TRACKED_MUTATIONS\"")
    stdout should include("\"retry_safe\":false")
    stdout should include("\"status\":\"PRESENT\"")
    stderr shouldBe empty
  }

  it should "translate a normally returned pipeline failure into a non-zero exit code" in {
    val pipeline = IngestionPipeline
      .builder()
      .withConfigDirectory(failingConfigDirectory.toString)
      .withPipelineId("failing-pipeline")
      .withCodeVersion("artifact-v1")
      .build()
    val request = pipeline.executionDefinition.newRequest("logical-run-2", Instant.parse("2026-10-01T09:00:00Z"))
    Files.write(failingRequestPath, ExecutionRequestJson.toJson(request).getBytes(StandardCharsets.UTF_8))

    val (exitCode, stdout, stderr) = execute(
      Seq("run", "--config", failingConfigDirectory.toString, "--request", failingRequestPath.toString)
    )

    withClue(stderr) {
      exitCode shouldBe FloeRunner.ExitCode.Failed
    }
    stdout should include("\"status\":\"FAILED\"")
    stdout should include("\"success\":false")
    stderr shouldBe empty
  }

  private def execute(arguments: Seq[String]): (Int, String, String) = {
    val stdout = new ByteArrayOutputStream()
    val stderr = new ByteArrayOutputStream()
    val exitCode = FloeRunner.execute(arguments, new PrintStream(stdout), new PrintStream(stderr))
    (
      exitCode,
      stdout.toString(StandardCharsets.UTF_8.name()),
      stderr.toString(StandardCharsets.UTF_8.name())
    )
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}
