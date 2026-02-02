package com.etl.framework.orchestration

import com.etl.framework.config._
import com.etl.framework.TestConfig
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.nio.file.{Files, Paths}
import scala.util.Try

/**
 * Property-based tests for parallel execution of independent flows
 * Feature: spark-etl-framework, Property 6: Parallel Execution of Independent Flows
 * Validates: Requirements 3.3
 * 
 * Test configuration: Uses minimalTestCount (5 cases) - parallel execution tests are expensive
 */
object ParallelExecutionProperties extends Properties("ParallelExecution") {
  
  // Configure test parameters for expensive parallel execution tests
  override def overrideParameters(p: Parameters): Parameters = TestConfig.minimalParams
  
  // Create a minimal Spark session for testing
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("ParallelExecutionPropertiesTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  
  import spark.implicits._
  
  // Generator for flow names
  val flowNameGen: Gen[String] = for {
    prefix <- Gen.oneOf("flow", "table", "dataset")
    suffix <- Gen.choose(1, 1000)
  } yield s"${prefix}_$suffix"
  
  // Generator for column names
  val columnNameGen: Gen[String] = for {
    name <- Gen.oneOf("id", "name", "value", "status", "code")
  } yield name
  
  // Helper to create a simple flow with no dependencies
  def createIndependentFlow(flowName: String, tempDir: String): FlowConfig = {
    // Create test data file
    val inputPath = s"$tempDir/input/$flowName"
    Files.createDirectories(Paths.get(inputPath))
    
    val testData = Seq(
      (s"${flowName}_1", s"value_1"),
      (s"${flowName}_2", s"value_2"),
      (s"${flowName}_3", s"value_3")
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
        `type` = "file",
        path = inputPath,
        format = "csv",
        options = Map("header" -> "true"),
        filePattern = None
      ),
      schema = SchemaConfig(
        enforceSchema = true,
        allowExtraColumns = false,
        columns = Seq(
          ColumnConfig("id", "string", nullable = false, None, "Primary key"),
          ColumnConfig("value", "string", nullable = true, None, "Value")
        )
      ),
      loadMode = LoadModeConfig(
        `type` = "full",
        keyColumns = Seq.empty
      ),
      validation = ValidationConfig(
        primaryKey = Seq("id"),
        foreignKeys = Seq.empty,  // No dependencies - independent flow
        rules = Seq.empty
      ),
      output = OutputConfig(
        path = Some(s"$tempDir/output/$flowName"),
        rejectedPath = Some(s"$tempDir/rejected/$flowName"),
        format = "parquet",
        partitionBy = Seq.empty,
        compression = "snappy",
        options = Map.empty
      )
    )
  }
  
  // Helper to create GlobalConfig with parallel execution enabled
  def createGlobalConfig(tempDir: String, parallel: Boolean): GlobalConfig = {
    GlobalConfig(
      paths = PathsConfig(
        validatedPath = s"$tempDir/validated",
        rejectedPath = s"$tempDir/rejected",
        metadataPath = s"$tempDir/metadata",
      ),
      processing = ProcessingConfig(
        batchIdFormat = "yyyyMMdd_HHmmss",
        executionMode = if (parallel) "parallel" else "sequential",
        failOnValidationError = false,
        maxRejectionRate = 1.0,
      ),
      performance = PerformanceConfig(
        parallelFlows = parallel,
        parallelNodes = false,
        broadcastThreshold = 10485760L,
        cacheValidated = false,
        shufflePartitions = 2
      ),
      monitoring = MonitoringConfig(
        enabled = false,
        metricsExporter = None,
        metricsEndpoint = None,
        logLevel = "INFO"
      ),
      security = SecurityConfig(
        encryptionEnabled = false,
        kmsKeyId = None,
        authenticationEnabled = false
      )
    )
  }
  
  // Helper to clean up temp directory
  def cleanupTempDir(tempDir: String): Unit = {
    Try {
      import scala.collection.JavaConverters._
      val dirPath = Paths.get(tempDir)
      if (Files.exists(dirPath)) {
        Files.walk(dirPath)
          .iterator()
          .asScala
          .toSeq
          .reverse
          .foreach(p => Files.deleteIfExists(p))
      }
    }
  }
  
  /**
   * Property 6: Parallel Execution of Independent Flows
   * For any set of independent flows (no FK dependencies) with parallel execution enabled,
   * the flows should be grouped together for potential parallel execution.
   */
  property("independent_flows_grouped_for_parallel_execution") = forAll(Gen.choose(2, 5)) { flowCount =>
    val tempDir = Files.createTempDirectory("parallel-exec-test").toString
    
    try {
      // Create independent flows (no FK dependencies)
      val flowNames = (1 to flowCount).map(i => s"independent_flow_$i")
      val flows = flowNames.map(name => createIndependentFlow(name, tempDir))
      
      // Create config with parallel execution enabled
      val globalConfig = createGlobalConfig(tempDir, parallel = true)
      
      val orchestrator = new FlowOrchestrator(globalConfig, flows)
      val plan = orchestrator.buildExecutionPlan()
      
      // All independent flows should be in a single group with parallel=true
      val result = plan.groups.size == 1 &&
        plan.groups.head.flows.size == flowCount &&
        plan.groups.head.parallel == true
      
      cleanupTempDir(tempDir)
      result
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
  
  /**
   * Property 6: Sequential Execution When Parallel Disabled
   * For any set of independent flows with parallel execution disabled,
   * the flows should be marked for sequential execution.
   */
  property("independent_flows_sequential_when_parallel_disabled") = forAll(Gen.choose(2, 5)) { flowCount =>
    val tempDir = Files.createTempDirectory("sequential-exec-test").toString
    
    try {
      // Create independent flows (no FK dependencies)
      val flowNames = (1 to flowCount).map(i => s"independent_flow_$i")
      val flows = flowNames.map(name => createIndependentFlow(name, tempDir))
      
      // Create config with parallel execution disabled
      val globalConfig = createGlobalConfig(tempDir, parallel = false)
      
      val orchestrator = new FlowOrchestrator(globalConfig, flows)
      val plan = orchestrator.buildExecutionPlan()
      
      // Flows should be grouped but marked as sequential
      val result = plan.groups.nonEmpty &&
        plan.groups.forall(_.parallel == false)
      
      cleanupTempDir(tempDir)
      result
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
  
  /**
   * Property 6: Dependent Flows Not Grouped Together
   * For any flows with dependencies, they should not be in the same execution group.
   */
  property("dependent_flows_in_separate_groups") = forAll(Gen.const(())) { _ =>
    val tempDir = Files.createTempDirectory("dependent-flows-test").toString
    
    try {
      // Create flow A (no dependencies)
      val flowA = createIndependentFlow("flow_a", tempDir)
      
      // Create flow B that depends on flow A
      val inputPathB = s"$tempDir/input/flow_b"
      Files.createDirectories(Paths.get(inputPathB))
      
      val testDataB = Seq(
        ("b_1", "flow_a_1", "value_b1"),
        ("b_2", "flow_a_2", "value_b2")
      ).toDF("id", "a_id", "value")
      
      testDataB.write
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
          `type` = "file",
          path = inputPathB,
          format = "csv",
          options = Map("header" -> "true"),
          filePattern = None
        ),
        schema = SchemaConfig(
          enforceSchema = true,
          allowExtraColumns = false,
          columns = Seq(
            ColumnConfig("id", "string", nullable = false, None, "Primary key"),
            ColumnConfig("a_id", "string", nullable = false, None, "Foreign key to flow_a"),
            ColumnConfig("value", "string", nullable = true, None, "Value")
          )
        ),
        loadMode = LoadModeConfig(
          `type` = "full",
          keyColumns = Seq.empty
        ),
        validation = ValidationConfig(
          primaryKey = Seq("id"),
          foreignKeys = Seq(
            ForeignKeyConfig("fk_a", "a_id", ReferenceConfig("flow_a", "id"))
          ),
          rules = Seq.empty
        ),
        output = OutputConfig(
          path = Some(s"$tempDir/output/flow_b"),
          rejectedPath = Some(s"$tempDir/rejected/flow_b"),
          format = "parquet",
          partitionBy = Seq.empty,
          compression = "snappy",
          options = Map.empty
        )
      )
      
      // Create config with parallel execution enabled
      val globalConfig = createGlobalConfig(tempDir, parallel = true)
      
      val orchestrator = new FlowOrchestrator(globalConfig, Seq(flowA, flowB))
      val plan = orchestrator.buildExecutionPlan()
      
      // Should have at least 2 groups since flows have dependencies
      // flow_a must execute before flow_b (in an earlier group)
      val flowAGroupIndex = plan.groups.indexWhere(_.flows.exists(_.name == "flow_a"))
      val flowBGroupIndex = plan.groups.indexWhere(_.flows.exists(_.name == "flow_b"))
      
      val result = plan.groups.size >= 2 &&
        flowAGroupIndex >= 0 &&
        flowBGroupIndex >= 0 &&
        flowAGroupIndex < flowBGroupIndex
      
      cleanupTempDir(tempDir)
      result
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
  
  /**
   * Property 6: All Flows Executed Regardless of Grouping
   * For any set of flows, all flows should be included in the execution plan.
   */
  property("all_flows_included_in_execution_plan") = forAll(Gen.choose(1, 5)) { flowCount =>
    val tempDir = Files.createTempDirectory("all-flows-test").toString
    
    try {
      // Create independent flows
      val flowNames = (1 to flowCount).map(i => s"flow_$i")
      val flows = flowNames.map(name => createIndependentFlow(name, tempDir))
      
      val globalConfig = createGlobalConfig(tempDir, parallel = true)
      
      val orchestrator = new FlowOrchestrator(globalConfig, flows)
      val plan = orchestrator.buildExecutionPlan()
      
      // All flows should be in the plan
      val allFlowsInPlan = plan.groups.flatMap(_.flows.map(_.name)).toSet
      val expectedFlows = flowNames.toSet
      
      val result = allFlowsInPlan == expectedFlows
      
      cleanupTempDir(tempDir)
      result
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
}

