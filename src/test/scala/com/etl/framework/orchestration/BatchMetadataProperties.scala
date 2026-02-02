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
 * Property-based tests for batch metadata generation
 * Feature: spark-etl-framework, Property 25: Batch ID Uniqueness
 * Feature: spark-etl-framework, Property 26: Metadata Recording Completeness
 * Validates: Requirements 19.1, 19.2
 * 
 * Test configuration: Uses minimalTestCount (5 cases) - metadata tests are expensive
 */
object BatchMetadataProperties extends Properties("BatchMetadata") {
  
  // Configure test parameters for very expensive full flow executions
  override def overrideParameters(p: Parameters): Parameters = TestConfig.minimalParams
  
  // Create a minimal Spark session for testing
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("BatchMetadataPropertiesTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  
  import spark.implicits._
  
  // Helper to create a simple flow
  def createFlow(flowName: String, tempDir: String): FlowConfig = {
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
  def createGlobalConfig(tempDir: String, batchIdFormat: String): GlobalConfig = {
    GlobalConfig(
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
        batchIdFormat = batchIdFormat,
        executionMode = "sequential",
        failOnValidationError = false,
        maxRejectionRate = 1.0,
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
  
  /**
   * Property 25: Batch ID Uniqueness
   * For any two batch executions, the generated batch_ids should be unique.
   */
  property("batch_ids_are_unique_across_executions") = forAll(Gen.choose(2, 5)) { executionCount =>
    val tempDir = Files.createTempDirectory("batch-id-test").toString
    
    try {
      // Create a simple flow
      val flow = createFlow("test_flow", tempDir)
      
      // Create config with timestamp format (most likely to be unique)
      val globalConfig = createGlobalConfig(tempDir, "timestamp")
      
      // Execute multiple times and collect batch IDs
      val batchIds = (1 to executionCount).map { _ =>
        val orchestrator = new FlowOrchestrator(globalConfig, Seq(flow))
        val result = orchestrator.execute()
        
        // Small delay to ensure timestamp changes
        Thread.sleep(10)
        
        result.batchId
      }
      
      // All batch IDs should be unique
      val allUnique = batchIds.distinct.size == batchIds.size
      
      cleanupTempDir(tempDir)
      allUnique
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
  
  /**
   * Property 25: Batch ID Format Consistency
   * For any batch execution, the batch_id should follow the configured format.
   */
  property("batch_id_follows_configured_format") = forAll(Gen.oneOf("timestamp", "datetime", "yyyyMMdd_HHmmss")) { format =>
    val tempDir = Files.createTempDirectory("batch-format-test").toString
    
    try {
      val flow = createFlow("test_flow", tempDir)
      val globalConfig = createGlobalConfig(tempDir, format)
      
      val orchestrator = new FlowOrchestrator(globalConfig, Seq(flow))
      val result = orchestrator.execute()
      
      val batchId = result.batchId
      
      // Verify format
      val validFormat = format match {
        case "timestamp" =>
          // Should be all digits
          batchId.forall(_.isDigit) && batchId.length >= 13
        
        case "datetime" | "yyyyMMdd_HHmmss" =>
          // Should match pattern: 8 digits + underscore + 6 digits
          batchId.matches("\\d{8}_\\d{6}")
        
        case _ =>
          // Any non-empty string is valid
          batchId.nonEmpty
      }
      
      cleanupTempDir(tempDir)
      validFormat
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
  
  /**
   * Property 26: Metadata Recording Completeness
   * For any flow execution, the metadata should contain input_records, valid_records,
   * rejected_records, and rejection_rate.
   */
  property("metadata_contains_required_fields") = forAll(Gen.choose(1, 3)) { flowCount =>
    val tempDir = Files.createTempDirectory("metadata-completeness-test").toString
    
    try {
      // Create flows
      val flows = (1 to flowCount).map { i =>
        createFlow(s"flow_$i", tempDir)
      }
      
      val globalConfig = createGlobalConfig(tempDir, "yyyyMMdd_HHmmss")
      
      val orchestrator = new FlowOrchestrator(globalConfig, flows)
      val result = orchestrator.execute()
      
      // Check that metadata file exists
      val metadataPath = Paths.get(s"$tempDir/metadata/${result.batchId}/summary.json")
      val metadataExists = Files.exists(metadataPath)
      
      val testResult = if (!metadataExists) {
        false
      } else {
        // Read and parse metadata
        val metadataContent = new String(Files.readAllBytes(metadataPath))
        
        // Check for required fields
        val hasRequiredFields = 
          metadataContent.contains("batch_id") &&
          metadataContent.contains("total_input_records") &&
          metadataContent.contains("total_valid_records") &&
          metadataContent.contains("total_rejected_records") &&
          metadataContent.contains("overall_rejection_rate") &&
          metadataContent.contains("flows_processed") &&
          metadataContent.contains("success")
        
        // Check that each flow has metadata
        val hasFlowMetadata = flows.forall { flow =>
          metadataContent.contains(s""""flow_name":"${flow.name}"""") ||
          metadataContent.contains(s""""flow_name" : "${flow.name}"""")
        }
        
        hasRequiredFields && hasFlowMetadata
      }
      
      cleanupTempDir(tempDir)
      testResult
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
  
  /**
   * Property 26: Per-Flow Metadata Contains Required Fields
   * For any flow execution, the per-flow metadata should contain all required fields.
   */
  property("per_flow_metadata_contains_required_fields") = {
    val tempDir = Files.createTempDirectory("per-flow-metadata-test").toString
    
    try {
      val flow = createFlow("test_flow", tempDir)
      val globalConfig = createGlobalConfig(tempDir, "yyyyMMdd_HHmmss")
      
      val orchestrator = new FlowOrchestrator(globalConfig, Seq(flow))
      val result = orchestrator.execute()
      
      // Check that per-flow metadata file exists
      val flowMetadataPath = Paths.get(s"$tempDir/metadata/${result.batchId}/flows/${flow.name}.json")
      val metadataExists = Files.exists(flowMetadataPath)
      
      val testResult = if (!metadataExists) {
        false
      } else {
        // Read and parse metadata
        val metadataContent = new String(Files.readAllBytes(flowMetadataPath))
        
        // Check for required fields
        metadataContent.contains("flow_name") &&
        metadataContent.contains("batch_id") &&
        metadataContent.contains("success") &&
        metadataContent.contains("input_records") &&
        metadataContent.contains("valid_records") &&
        metadataContent.contains("rejected_records") &&
        metadataContent.contains("rejection_rate") &&
        metadataContent.contains("execution_time_ms")
      }
      
      cleanupTempDir(tempDir)
      testResult
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
  
  /**
   * Property 26: Latest Symlink Points to Most Recent Batch
   * For any batch execution, the "latest" symlink should point to the most recent batch.
   */
  property("latest_symlink_points_to_most_recent_batch") = {
    val tempDir = Files.createTempDirectory("latest-symlink-test").toString
    
    try {
      val flow = createFlow("test_flow", tempDir)
      val globalConfig = createGlobalConfig(tempDir, "timestamp")
      
      // Execute first batch
      val orchestrator1 = new FlowOrchestrator(globalConfig, Seq(flow))
      val result1 = orchestrator1.execute()
      
      // Small delay
      Thread.sleep(50)
      
      // Execute second batch
      val orchestrator2 = new FlowOrchestrator(globalConfig, Seq(flow))
      val result2 = orchestrator2.execute()
      
      // Check that latest symlink exists and points to second batch
      val latestPath = Paths.get(s"$tempDir/metadata/latest")
      val latestExists = Files.exists(latestPath)
      
      val pointsToSecondBatch = if (latestExists && Files.isSymbolicLink(latestPath)) {
        val target = Files.readSymbolicLink(latestPath)
        target.toString.contains(result2.batchId)
      } else {
        false
      }
      
      cleanupTempDir(tempDir)
      
      latestExists && pointsToSecondBatch
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
  
  /**
   * Property 26: Metadata Records Rejection Reasons
   * For any flow with rejected records, the metadata should contain rejection reasons with counts.
   */
  property("metadata_records_rejection_reasons") = {
    val tempDir = Files.createTempDirectory("rejection-reasons-test").toString
    
    try {
      // Create flow with data that will have duplicate PKs (causing rejections)
      val inputPath = s"$tempDir/input/test_flow"
      Files.createDirectories(Paths.get(inputPath))
      
      val testData = Seq(
        ("id_1", "value_1"),
        ("id_1", "value_2"),  // Duplicate PK
        ("id_2", "value_3")
      ).toDF("id", "value")
      
      testData.write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(inputPath)
      
      val flow = FlowConfig(
        name = "test_flow",
        description = "Test flow",
        version = "1.0",
        owner = "test",
        source = SourceConfig("file", inputPath, "csv", Map("header" -> "true"), None),
        schema = SchemaConfig(true, false, Seq(
          ColumnConfig("id", "string", false, None, "PK"),
          ColumnConfig("value", "string", true, None, "Value")
        )),
        loadMode = LoadModeConfig("full", Seq.empty),
        validation = ValidationConfig(Seq("id"), Seq.empty, Seq.empty),
        output = OutputConfig(
          Some(s"$tempDir/output/test_flow"),
          Some(s"$tempDir/rejected/test_flow"),
          "parquet", Seq.empty, "snappy", Map.empty
        )
      )
      
      val globalConfig = createGlobalConfig(tempDir, "yyyyMMdd_HHmmss")
      
      val orchestrator = new FlowOrchestrator(globalConfig, Seq(flow))
      val result = orchestrator.execute()
      
      // Check that metadata contains rejection reasons
      val metadataPath = Paths.get(s"$tempDir/metadata/${result.batchId}/summary.json")
      val metadataContent = new String(Files.readAllBytes(metadataPath))
      
      val hasRejectionReasons = metadataContent.contains("rejection_reasons")
      
      cleanupTempDir(tempDir)
      hasRejectionReasons
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
  
  /**
   * Property 26: Empty Flow List Produces Valid Metadata
   * For any empty flow list, metadata should still be generated with appropriate values.
   */
  property("empty_flows_produces_valid_metadata") = {
    val tempDir = Files.createTempDirectory("empty-metadata-test").toString
    
    try {
      val globalConfig = createGlobalConfig(tempDir, "yyyyMMdd_HHmmss")
      
      val orchestrator = new FlowOrchestrator(globalConfig, Seq.empty)
      val result = orchestrator.execute()
      
      // Check that metadata exists
      val metadataPath = Paths.get(s"$tempDir/metadata/${result.batchId}/summary.json")
      val metadataExists = Files.exists(metadataPath)
      
      val testResult = if (!metadataExists) {
        false
      } else {
        val metadataContent = new String(Files.readAllBytes(metadataPath))
        
        // Should have zero flows processed
        metadataContent.contains(""""flows_processed":0""") ||
        metadataContent.contains(""""flows_processed" : 0""")
      }
      
      cleanupTempDir(tempDir)
      testResult
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        cleanupTempDir(tempDir)
        false
    }
  }
}

