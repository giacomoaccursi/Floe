package com.etl.framework.orchestration

import com.etl.framework.config._
import com.etl.framework.TestConfig
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}
import scala.util.Try

/**
 * Property-based tests for rollback on failure
 * Feature: spark-etl-framework, Property 27: Rollback on Failure
 * Validates: Requirements 20.2
 * 
 * Test configuration: Uses minimalTestCount (5 cases) - rollback tests are very expensive
 */
object RollbackProperties extends Properties("Rollback") {
  
  // Configure test parameters for very expensive full flow executions with rollback
  override def overrideParameters(p: Parameters): Parameters = TestConfig.minimalParams
  
  // Create a minimal Spark session for testing
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("RollbackPropertiesTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  
  import spark.implicits._
  
  // Helper to create a simple flow
  def createFlow(flowName: String, tempDir: String, willFail: Boolean = false): FlowConfig = {
    // Create test data file
    val inputPath = s"$tempDir/input/$flowName"
    Files.createDirectories(Paths.get(inputPath))
    
    val testData = if (willFail) {
      // Create data that will fail validation (duplicate PKs)
      Seq(
        (s"${flowName}_1", s"value_1"),
        (s"${flowName}_1", s"value_2"),  // Duplicate PK
        (s"${flowName}_2", s"value_3")
      ).toDF("id", "value")
    } else {
      // Create valid data
      Seq(
        (s"${flowName}_1", s"value_1"),
        (s"${flowName}_2", s"value_2"),
        (s"${flowName}_3", s"value_3")
      ).toDF("id", "value")
    }
    
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
        foreignKeys = Seq.empty,
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
  
  // Helper to create GlobalConfig
  def createGlobalConfig(tempDir: String, rollbackOnFailure: Boolean, failOnValidationError: Boolean): GlobalConfig = {
    GlobalConfig(
      spark = SparkConfig(
        appName = "test",
        master = "local[*]",
        config = Map.empty
      ),
      paths = PathsConfig(
        inputBase = s"$tempDir/input",
        outputBase = s"$tempDir/output",
        validatedPath = s"$tempDir/validated",
        rejectedPath = s"$tempDir/rejected",
        metadataPath = s"$tempDir/metadata",
        modelPath = s"$tempDir/model",
        stagingPath = s"$tempDir/staging",
        checkpointPath = s"$tempDir/checkpoint"
      ),
      processing = ProcessingConfig(
        batchIdFormat = "yyyyMMdd_HHmmss",
        executionMode = "sequential",
        failOnValidationError = failOnValidationError,
        maxRejectionRate = 0.0,  // Any rejection will trigger failure if failOnValidationError=true
        rollbackOnFailure = rollbackOnFailure,
        checkpointEnabled = false,
        checkpointInterval = "5m"
      ),
      performance = PerformanceConfig(
        parallelFlows = false,
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
  
  // Helper to check if output exists
  def outputExists(tempDir: String, flowName: String): Boolean = {
    val outputPath = Paths.get(s"$tempDir/output/$flowName")
    Files.exists(outputPath) && Files.list(outputPath).findAny().isPresent
  }
  
  /**
   * Property 27: Rollback on Failure
   * For any flow execution that fails with rollback_on_failure=true,
   * all previously completed flows in the same batch should be rolled back.
   */
  property("rollback_deletes_completed_flow_outputs") = forAll(Gen.choose(2, 4)) { successfulFlowCount =>
    val tempDir = Files.createTempDirectory("rollback-test").toString
    
    try {
      // Create successful flows
      val successfulFlows = (1 to successfulFlowCount).map { i =>
        createFlow(s"success_flow_$i", tempDir, willFail = false)
      }
      
      // Create a failing flow (will have duplicate PKs causing validation failure)
      val failingFlow = createFlow("failing_flow", tempDir, willFail = true)
      
      // All flows together
      val allFlows = successfulFlows :+ failingFlow
      
      // Create config with rollback enabled and fail on validation error
      val globalConfig = createGlobalConfig(tempDir, rollbackOnFailure = true, failOnValidationError = true)
      
      val orchestrator = new FlowOrchestrator(globalConfig, allFlows)
      val result = orchestrator.execute()
      
      // Execution should fail
      val executionFailed = !result.success
      
      // All outputs should be deleted (rolled back)
      val allOutputsDeleted = successfulFlows.forall { flow =>
        !outputExists(tempDir, flow.name)
      }
      
      // Metadata should indicate rollback
      val metadataPath = Paths.get(s"$tempDir/metadata/${result.batchId}/summary.json")
      val metadataExists = Files.exists(metadataPath)
      
      cleanupTempDir(tempDir)
      
      executionFailed && allOutputsDeleted && metadataExists
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
  
  /**
   * Property 27: No Rollback When Disabled
   * For any flow execution that fails with rollback_on_failure=false,
   * previously completed flows should NOT be rolled back.
   */
  property("no_rollback_when_disabled") = forAll(Gen.choose(2, 4)) { successfulFlowCount =>
    val tempDir = Files.createTempDirectory("no-rollback-test").toString
    
    try {
      // Create successful flows
      val successfulFlows = (1 to successfulFlowCount).map { i =>
        createFlow(s"success_flow_$i", tempDir, willFail = false)
      }
      
      // Create a failing flow
      val failingFlow = createFlow("failing_flow", tempDir, willFail = true)
      
      // All flows together
      val allFlows = successfulFlows :+ failingFlow
      
      // Create config with rollback DISABLED
      val globalConfig = createGlobalConfig(tempDir, rollbackOnFailure = false, failOnValidationError = true)
      
      val orchestrator = new FlowOrchestrator(globalConfig, allFlows)
      val result = orchestrator.execute()
      
      // Execution should fail
      val executionFailed = !result.success
      
      // Successful flow outputs should still exist (NOT rolled back)
      val outputsStillExist = successfulFlows.forall { flow =>
        outputExists(tempDir, flow.name)
      }
      
      cleanupTempDir(tempDir)
      
      executionFailed && outputsStillExist
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
  
  /**
   * Property 27: Rollback Preserves Metadata
   * For any rollback, metadata should be written indicating the rollback occurred.
   */
  property("rollback_writes_metadata") = {
    val tempDir = Files.createTempDirectory("rollback-metadata-test").toString
    
    try {
      // Create one successful flow and one failing flow
      val successFlow = createFlow("success_flow", tempDir, willFail = false)
      val failFlow = createFlow("fail_flow", tempDir, willFail = true)
      
      // Create config with rollback enabled
      val globalConfig = createGlobalConfig(tempDir, rollbackOnFailure = true, failOnValidationError = true)
      
      val orchestrator = new FlowOrchestrator(globalConfig, Seq(successFlow, failFlow))
      val result = orchestrator.execute()
      
      // Check that metadata was written
      val metadataPath = Paths.get(s"$tempDir/metadata/${result.batchId}/summary.json")
      val metadataExists = Files.exists(metadataPath)
      
      // Read metadata and check for rollback indicator
      val metadataContent = if (metadataExists) {
        new String(Files.readAllBytes(metadataPath))
      } else {
        ""
      }
      
      val containsRollbackInfo = metadataContent.contains("rolled_back") || 
                                  metadataContent.contains("rollback")
      
      cleanupTempDir(tempDir)
      
      !result.success && metadataExists && containsRollbackInfo
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
  
  /**
   * Property 27: Successful Execution Has No Rollback
   * For any successful execution, no rollback should occur.
   */
  property("successful_execution_no_rollback") = forAll(Gen.choose(1, 5)) { flowCount =>
    val tempDir = Files.createTempDirectory("success-no-rollback-test").toString
    
    try {
      // Create all successful flows
      val flows = (1 to flowCount).map { i =>
        createFlow(s"flow_$i", tempDir, willFail = false)
      }
      
      // Create config with rollback enabled (but shouldn't trigger)
      val globalConfig = createGlobalConfig(tempDir, rollbackOnFailure = true, failOnValidationError = false)
      
      val orchestrator = new FlowOrchestrator(globalConfig, flows)
      val result = orchestrator.execute()
      
      // Execution should succeed
      val executionSucceeded = result.success
      
      // All outputs should exist
      val allOutputsExist = flows.forall { flow =>
        outputExists(tempDir, flow.name)
      }
      
      cleanupTempDir(tempDir)
      
      executionSucceeded && allOutputsExist
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
  
  /**
   * Property 27: Rollback Handles Empty Flow List
   * For any empty flow list, rollback should handle gracefully.
   */
  property("rollback_handles_empty_flows") = {
    val tempDir = Files.createTempDirectory("empty-flows-test").toString
    
    try {
      val globalConfig = createGlobalConfig(tempDir, rollbackOnFailure = true, failOnValidationError = true)
      
      val orchestrator = new FlowOrchestrator(globalConfig, Seq.empty)
      val result = orchestrator.execute()
      
      // Should succeed with no flows
      val succeeded = result.success
      val noFlowResults = result.flowResults.isEmpty
      
      cleanupTempDir(tempDir)
      
      succeeded && noFlowResults
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
}

