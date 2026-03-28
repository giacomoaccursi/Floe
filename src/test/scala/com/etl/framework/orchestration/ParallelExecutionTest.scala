package com.etl.framework.orchestration

import com.etl.framework.config._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}
import scala.util.Try

class ParallelExecutionTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("ParallelExecutionTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  private def createIndependentFlow(
      flowName: String,
      tempDir: String
  ): FlowConfig = {
    val inputPath = s"$tempDir/input/$flowName"
    Files.createDirectories(Paths.get(inputPath))

    Seq(
      (s"${flowName}_1", "value_1"),
      (s"${flowName}_2", "value_2"),
      (s"${flowName}_3", "value_3")
    ).toDF("id", "value").write
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
        options = Map("header" -> "true"),
        filePattern = None
      ),
      schema = SchemaConfig(
        enforceSchema = true,
        allowExtraColumns = false,
        columns = Seq(
          ColumnConfig("id", "string", nullable = false, None, "PK"),
          ColumnConfig("value", "string", nullable = true, None, "Value")
        )
      ),
      loadMode = LoadModeConfig(`type` = LoadMode.Full),
      validation = ValidationConfig(
        primaryKey = Seq("id"),
        foreignKeys = Seq.empty,
        rules = Seq.empty
      ),
      output = OutputConfig(
        path = Some(s"$tempDir/output/$flowName"),
        rejectedPath = Some(s"$tempDir/rejected/$flowName")
      )
    )
  }

  private def createGlobalConfig(
      tempDir: String,
      parallel: Boolean
  ): GlobalConfig = GlobalConfig(
    paths = PathsConfig(
      outputPath = s"$tempDir/output",
      rejectedPath = s"$tempDir/rejected",
      metadataPath = s"$tempDir/metadata"
    ),
    processing = ProcessingConfig("yyyyMMdd_HHmmss", false, 1.0),
    performance = PerformanceConfig(
      parallelFlows = parallel,
      parallelNodes = false
    ),
    iceberg = IcebergConfig(warehouse = "/tmp/test-warehouse")
  )

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

  "Parallel execution" should "group independent flows when parallel is enabled" in {
    val tempDir =
      Files.createTempDirectory("parallel-exec-test").toString
    try {
      val flows = (1 to 3).map(i =>
        createIndependentFlow(s"independent_flow_$i", tempDir)
      )
      val globalConfig = createGlobalConfig(tempDir, parallel = true)

      val orchestrator = FlowOrchestrator(globalConfig, flows)
      val plan = orchestrator.buildExecutionPlan()

      plan.groups should have size 1
      plan.groups.head.flows should have size 3
      plan.groups.head.parallel shouldBe true
    } finally {
      cleanupTempDir(tempDir)
    }
  }

  it should "mark flows sequential when parallel is disabled" in {
    val tempDir =
      Files.createTempDirectory("sequential-exec-test").toString
    try {
      val flows = (1 to 3).map(i =>
        createIndependentFlow(s"independent_flow_$i", tempDir)
      )
      val globalConfig = createGlobalConfig(tempDir, parallel = false)

      val orchestrator = FlowOrchestrator(globalConfig, flows)
      val plan = orchestrator.buildExecutionPlan()

      plan.groups should not be empty
      plan.groups.forall(_.parallel == false) shouldBe true
    } finally {
      cleanupTempDir(tempDir)
    }
  }

  it should "place dependent flows in separate groups" in {
    val tempDir =
      Files.createTempDirectory("dependent-flows-test").toString
    try {
      val flowA = createIndependentFlow("flow_a", tempDir)

      val inputPathB = s"$tempDir/input/flow_b"
      Files.createDirectories(Paths.get(inputPathB))
      Seq(("b_1", "flow_a_1", "value_b1"), ("b_2", "flow_a_2", "value_b2"))
        .toDF("id", "a_id", "value")
        .write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(inputPathB)

      val flowB = FlowConfig(
        name = "flow_b",
        description = "Test flow B",
        version = "1.0",
        owner = "test",
        source = SourceConfig(
          `type` = SourceType.File,
          path = inputPathB,
          format = FileFormat.CSV,
          options = Map("header" -> "true"),
          filePattern = None
        ),
        schema = SchemaConfig(
          enforceSchema = true,
          allowExtraColumns = false,
          columns = Seq(
            ColumnConfig("id", "string", nullable = false, None, "PK"),
            ColumnConfig("a_id", "string", nullable = false, None, "FK"),
            ColumnConfig(
              "value",
              "string",
              nullable = true,
              None,
              "Value"
            )
          )
        ),
        loadMode = LoadModeConfig(`type` = LoadMode.Full),
        validation = ValidationConfig(
          primaryKey = Seq("id"),
          foreignKeys = Seq(
            ForeignKeyConfig(
              "fk_a",
              "a_id",
              ReferenceConfig("flow_a", "id")
            )
          ),
          rules = Seq.empty
        ),
        output = OutputConfig(
          path = Some(s"$tempDir/output/flow_b"),
          rejectedPath = Some(s"$tempDir/rejected/flow_b")
        )
      )

      val globalConfig = createGlobalConfig(tempDir, parallel = true)
      val orchestrator =
        FlowOrchestrator(globalConfig, Seq(flowA, flowB))
      val plan = orchestrator.buildExecutionPlan()

      plan.groups.size should be >= 2

      val flowAGroupIndex =
        plan.groups.indexWhere(_.flows.exists(_.name == "flow_a"))
      val flowBGroupIndex =
        plan.groups.indexWhere(_.flows.exists(_.name == "flow_b"))

      flowAGroupIndex should be >= 0
      flowBGroupIndex should be >= 0
      flowAGroupIndex should be < flowBGroupIndex
    } finally {
      cleanupTempDir(tempDir)
    }
  }

  it should "include all flows in execution plan" in {
    val tempDir =
      Files.createTempDirectory("all-flows-test").toString
    try {
      val flowNames = (1 to 4).map(i => s"flow_$i")
      val flows =
        flowNames.map(name => createIndependentFlow(name, tempDir))
      val globalConfig = createGlobalConfig(tempDir, parallel = true)

      val orchestrator = FlowOrchestrator(globalConfig, flows)
      val plan = orchestrator.buildExecutionPlan()

      val allFlowsInPlan =
        plan.groups.flatMap(_.flows.map(_.name)).toSet

      allFlowsInPlan shouldBe flowNames.toSet
    } finally {
      cleanupTempDir(tempDir)
    }
  }
}
