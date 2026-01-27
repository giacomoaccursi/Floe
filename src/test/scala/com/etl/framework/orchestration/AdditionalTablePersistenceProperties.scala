package com.etl.framework.orchestration


import com.etl.framework.TestConfig
import com.etl.framework.config._
import com.etl.framework.core.{AdditionalTableMetadata, TransformationContext}
import com.etl.framework.orchestration.flow.FlowExecutor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters
import org.scalacheck.{Gen, Properties}

import java.nio.file.{Files, Paths}
import scala.util.Try

/**
 * Property-based tests for additional table persistence
 * Feature: spark-etl-framework, Property 20: Additional Table Persistence
 * Validates: Requirements 13.1
 * 
 * Test configuration: Uses expensiveTestCount (10 cases) due to file I/O and Spark operations
 */
object AdditionalTablePersistenceProperties extends Properties("AdditionalTablePersistence") {
  
  // Configure test parameters for expensive operations
  override def overrideParameters(p: Parameters): Parameters = TestConfig.expensiveParams
  
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("AdditionalTablePersistenceProperties")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  
  import spark.implicits._
  
  // Generator for test data
  val testDataGen: Gen[Seq[(String, Int, String, Double)]] = for {
    size <- Gen.choose(10, 50)
    data <- Gen.listOfN(size, for {
      id <- Gen.identifier.map(s => s"id_$s")
      count <- Gen.choose(1, 1000)
      category <- Gen.oneOf("A", "B", "C", "D")
      amount <- Gen.choose(1.0, 10000.0)
    } yield (id, count, category, amount))
  } yield data.distinct
  
  // Generator for table names
  val tableNameGen: Gen[String] = for {
    prefix <- Gen.oneOf("summary", "aggregate", "derived", "enriched")
    suffix <- Gen.identifier.map(_.take(10))
  } yield s"${prefix}_${suffix}"
  
  // Generator for DAG metadata
  // Note: Only use "category" for partitioning since that's the only column in the aggregated DataFrame
  val dagMetadataGen: Gen[Option[AdditionalTableMetadata]] = Gen.option(for {
    pkSize <- Gen.choose(1, 2)
    pkCols <- Gen.listOfN(pkSize, Gen.const("category"))
    numJoinKeys <- Gen.choose(0, 2)
    joinKeys <- Gen.listOfN(numJoinKeys, for {
      flow <- Gen.const("customers")
      keys <- Gen.listOfN(1, Gen.const("customer_id"))
    } yield (flow, keys))
    description <- Gen.option(Gen.alphaNumStr)
    usePartitioning <- Gen.oneOf(true, false)
  } yield AdditionalTableMetadata(
    primaryKey = pkCols.distinct,
    joinKeys = joinKeys.toMap,
    description = description,
    partitionBy = if (usePartitioning) Seq("category") else Seq.empty
  ))
  
  // Temporary directory for test outputs
  def createTempDir(): String = Files.createTempDirectory("etl_test_").toString
  
  // Helper to create test configuration
  def createTestConfig(flowName: String, tempDir: String): (FlowConfig, GlobalConfig) = {
    val globalConfig = GlobalConfig(
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
        executionMode = "batch",
        failOnValidationError = false,
        maxRejectionRate = 0.1,
        checkpointEnabled = false,
        checkpointInterval = "10m"
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
    
    val flowConfig = FlowConfig(
      name = flowName,
      description = "Test flow",
      version = "1.0",
      owner = "test",
      source = SourceConfig(
        `type` = "file",
        path = s"$tempDir/input/$flowName",
        format = "parquet",
        options = Map.empty,
        filePattern = None
      ),
      schema = SchemaConfig(
        enforceSchema = true,
        allowExtraColumns = false,
        columns = Seq(
          ColumnConfig("id", "string", false, None, "ID"),
          ColumnConfig("count", "integer", false, None, "Count"),
          ColumnConfig("category", "string", false, None, "Category"),
          ColumnConfig("amount", "double", false, None, "Amount")
        )
      ),
      loadMode = LoadModeConfig(
        `type` = "full",
        keyColumns = Seq.empty,
        mergeStrategy = None,
        updateTimestampColumn = None,
        validFromColumn = None,
        validToColumn = None,
        isCurrentColumn = None,
        compareColumns = Seq.empty
      ),
      validation = ValidationConfig(
        primaryKey = Seq("id"),
        foreignKeys = Seq.empty,
        rules = Seq.empty
      ),
      output = OutputConfig(
        path = None,
        rejectedPath = None,
        format = "parquet",
        partitionBy = Seq.empty,
        compression = "snappy",
        options = Map.empty
      ),
      preValidationTransformation = None,
      postValidationTransformation = None
    )
    
    (flowConfig, globalConfig)
  }
  
  /**
   * Property 20: Additional Table Persistence
   * For any transformation that calls ctx.addTable(), 
   * the table should be automatically saved to Parquet at the specified or default path.
   */
  property("additional_table_is_persisted_to_parquet") = forAll(
    testDataGen,
    tableNameGen,
    dagMetadataGen
  ) { (testData, tableName, dagMetadata) =>
    
    val tempDir = createTempDir()
    val flowName = "test_flow"
    val batchId = s"test_batch_${System.currentTimeMillis()}"
    val (flowConfig, globalConfig) = createTestConfig(flowName, tempDir)
    
    // Create test data
    val df = testData.toDF("id", "count", "category", "amount")
    
    // Write source data
    val sourcePath = s"$tempDir/input/$flowName"
    df.write.mode(SaveMode.Overwrite).parquet(sourcePath)
    
    // Create transformation that adds a table
    val transformation: TransformationContext => DataFrame = { ctx =>
      // Create an additional table (aggregation)
      val additionalTable = ctx.currentData
        .groupBy("category")
        .agg(
          count("*").as("record_count"),
          sum("amount").as("total_amount"),
          avg("amount").as("avg_amount")
        )
      
      ctx.addTable(
        tableName = tableName,
        data = additionalTable,
        outputPath = None, // Use default path
        dagMetadata = dagMetadata
      )
      
      ctx.currentData
    }
    
    // Create flow config with post-validation transformation
    val flowConfigWithTransform = flowConfig.copy(
      postValidationTransformation = Some(transformation)
    )
    
    // Execute flow
    val executor = new FlowExecutor(flowConfigWithTransform, globalConfig, Map.empty)
    val result = executor.execute(batchId)
    
    // Verify the additional table was persisted
    val expectedPath = s"${globalConfig.paths.validatedPath}/${flowName}_${tableName}"
    val tableExists = Try {
      val persistedDf = spark.read.parquet(expectedPath)
      persistedDf.count() > 0
    }.getOrElse(false)
    
    // Verify metadata was created
    val metadataPath = s"${globalConfig.paths.metadataPath}/$batchId/additional_tables/${tableName}.json"
    val metadataExists = Files.exists(Paths.get(metadataPath))
    
    // Cleanup
    Try {
      import org.apache.hadoop.fs.{FileSystem, Path}
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new Path(expectedPath), true)
      Files.deleteIfExists(Paths.get(metadataPath))
    }
    
    result.success && tableExists && metadataExists
  }
  
  /**
   * Property: Additional table with custom output path is persisted to specified location
   * For any transformation that calls ctx.addTable() with a custom outputPath,
   * the table should be saved to the specified path.
   */
  property("additional_table_with_custom_path_is_persisted_correctly") = forAll(
    testDataGen,
    tableNameGen,
    dagMetadataGen
  ) { (testData, tableName, dagMetadata) =>
    
    val tempDir = createTempDir()
    val flowName = "test_flow"
    val batchId = s"test_batch_${System.currentTimeMillis()}"
    val (flowConfig, globalConfig) = createTestConfig(flowName, tempDir)
    
    val customPath = s"$tempDir/custom_output/${tableName}"
    
    // Create test data
    val df = testData.toDF("id", "count", "category", "amount")
    
    // Write source data
    val sourcePath = s"$tempDir/input/$flowName"
    df.write.mode(SaveMode.Overwrite).parquet(sourcePath)
    
    // Create transformation that adds a table with custom path
    val transformation: TransformationContext => DataFrame = { ctx =>
      val additionalTable = ctx.currentData
        .groupBy("category")
        .agg(count("*").as("record_count"))
      
      ctx.addTable(
        tableName = tableName,
        data = additionalTable,
        outputPath = Some(customPath),
        dagMetadata = dagMetadata
      )
      
      ctx.currentData
    }
    
    // Create flow config with post-validation transformation
    val flowConfigWithTransform = flowConfig.copy(
      postValidationTransformation = Some(transformation)
    )
    
    // Execute flow
    val executor = new FlowExecutor(flowConfigWithTransform, globalConfig, Map.empty)
    val result = executor.execute(batchId)
    
    // Verify the additional table was persisted to custom path
    val tableExists = Try {
      val persistedDf = spark.read.parquet(customPath)
      persistedDf.count() > 0
    }.getOrElse(false)
    
    // Cleanup
    Try {
      import org.apache.hadoop.fs.{FileSystem, Path}
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new Path(customPath), true)
    }
    
    result.success && tableExists
  }
  
  /**
   * Property: Additional table metadata includes schema and statistics
   * For any additional table created, the metadata should contain schema information
   * and record count statistics.
   */
  property("additional_table_metadata_includes_schema_and_statistics") = forAll(
    testDataGen,
    tableNameGen,
    dagMetadataGen
  ) { (testData, tableName, dagMetadata) =>
    
    val tempDir = createTempDir()
    val flowName = "test_flow"
    val batchId = s"test_batch_${System.currentTimeMillis()}"
    val (flowConfig, globalConfig) = createTestConfig(flowName, tempDir)
    
    // Create test data
    val df = testData.toDF("id", "count", "category", "amount")
    
    // Write source data
    val sourcePath = s"$tempDir/input/$flowName"
    df.write.mode(SaveMode.Overwrite).parquet(sourcePath)
    
    // Create transformation that adds a table
    val transformation: TransformationContext => DataFrame = { ctx =>
      val additionalTable = ctx.currentData
        .groupBy("category")
        .agg(
          count("*").as("record_count"),
          sum("amount").as("total_amount")
        )
      
      ctx.addTable(
        tableName = tableName,
        data = additionalTable,
        outputPath = None,
        dagMetadata = dagMetadata
      )
      
      ctx.currentData
    }
    
    // Create flow config with post-validation transformation
    val flowConfigWithTransform = flowConfig.copy(
      postValidationTransformation = Some(transformation)
    )
    
    // Execute flow
    val executor = new FlowExecutor(flowConfigWithTransform, globalConfig, Map.empty)
    val result = executor.execute(batchId)
    
    // Read and verify metadata
    val metadataPath = s"${globalConfig.paths.metadataPath}/$batchId/additional_tables/${tableName}.json"
    val metadataValid = Try {
      val metadataContent = new String(Files.readAllBytes(Paths.get(metadataPath)))
      
      // Verify metadata contains required fields
      metadataContent.contains("\"table_name\"") &&
      metadataContent.contains("\"record_count\"") &&
      metadataContent.contains("\"schema\"") &&
      metadataContent.contains("\"fields\"") &&
      metadataContent.contains("\"" + tableName + "\"")
    }.getOrElse(false)
    
    // Cleanup
    Try {
      Files.deleteIfExists(Paths.get(metadataPath))
    }
    
    result.success && metadataValid
  }
  
  /**
   * Property: Additional table with dagMetadata includes join configuration
   * For any additional table created with dagMetadata, the metadata should include
   * primary key and join key information for Transformation integration.
   */
  property("additional_table_with_dag_metadata_includes_join_config") = forAll(
    testDataGen,
    tableNameGen
  ) { (testData, tableName) =>
    
    val tempDir = createTempDir()
    val flowName = "test_flow"
    val batchId = s"test_batch_${System.currentTimeMillis()}"
    val (flowConfig, globalConfig) = createTestConfig(flowName, tempDir)
    
    // Create DAG metadata with join keys
    val dagMetadata = Some(AdditionalTableMetadata(
      primaryKey = Seq("category"),
      joinKeys = Map(
        "customers" -> Seq("customer_id"),
        "orders" -> Seq("order_id")
      ),
      description = Some("Test aggregation table"),
      partitionBy = Seq("category")
    ))
    
    // Create test data
    val df = testData.toDF("id", "count", "category", "amount")
    
    // Write source data
    val sourcePath = s"$tempDir/input/$flowName"
    df.write.mode(SaveMode.Overwrite).parquet(sourcePath)
    
    // Create transformation that adds a table with DAG metadata
    val transformation: TransformationContext => DataFrame = { ctx =>
      val additionalTable = ctx.currentData
        .groupBy("category")
        .agg(count("*").as("record_count"))
      
      ctx.addTable(
        tableName = tableName,
        data = additionalTable,
        outputPath = None,
        dagMetadata = dagMetadata
      )
      
      ctx.currentData
    }
    
    // Create flow config with post-validation transformation
    val flowConfigWithTransform = flowConfig.copy(
      postValidationTransformation = Some(transformation)
    )
    
    // Execute flow
    val executor = new FlowExecutor(flowConfigWithTransform, globalConfig, Map.empty)
    val result = executor.execute(batchId)
    
    // Read and verify metadata includes DAG metadata
    val metadataPath = s"${globalConfig.paths.metadataPath}/$batchId/additional_tables/${tableName}.json"
    val dagMetadataValid = Try {
      val metadataContent = new String(Files.readAllBytes(Paths.get(metadataPath)))
      
      // Verify DAG metadata is present
      metadataContent.contains("\"dag_metadata\"") &&
      metadataContent.contains("\"primary_key\"") &&
      metadataContent.contains("\"join_keys\"") &&
      metadataContent.contains("\"customers\"") &&
      metadataContent.contains("\"orders\"")
    }.getOrElse(false)
    
    // Cleanup
    Try {
      Files.deleteIfExists(Paths.get(metadataPath))
    }
    
    result.success && dagMetadataValid
  }
  
  /**
   * Property: Multiple additional tables can be created in single transformation
   * For any transformation that calls ctx.addTable() multiple times,
   * all tables should be persisted correctly.
   */
  property("multiple_additional_tables_are_persisted") = forAll(
    testDataGen
  ) { testData =>
    
    val tempDir = createTempDir()
    val flowName = "test_flow"
    val batchId = s"test_batch_${System.currentTimeMillis()}"
    val (flowConfig, globalConfig) = createTestConfig(flowName, tempDir)
    
    val table1Name = "summary_by_category"
    val table2Name = "summary_by_amount_range"
    
    // Create test data
    val df = testData.toDF("id", "count", "category", "amount")
    
    // Write source data
    val sourcePath = s"$tempDir/input/$flowName"
    df.write.mode(SaveMode.Overwrite).parquet(sourcePath)
    
    // Create transformation that adds multiple tables
    val transformation: TransformationContext => DataFrame = { ctx =>
      // First additional table
      val table1 = ctx.currentData
        .groupBy("category")
        .agg(count("*").as("record_count"))
      
      ctx.addTable(
        tableName = table1Name,
        data = table1,
        outputPath = None,
        dagMetadata = None
      )
      
      // Second additional table
      val table2 = ctx.currentData
        .withColumn("amount_range", 
          when(col("amount") < 1000, "low")
          .when(col("amount") < 5000, "medium")
          .otherwise("high")
        )
        .groupBy("amount_range")
        .agg(count("*").as("record_count"))
      
      ctx.addTable(
        tableName = table2Name,
        data = table2,
        outputPath = None,
        dagMetadata = None
      )
      
      ctx.currentData
    }
    
    // Create flow config with post-validation transformation
    val flowConfigWithTransform = flowConfig.copy(
      postValidationTransformation = Some(transformation)
    )
    
    // Execute flow
    val executor = new FlowExecutor(flowConfigWithTransform, globalConfig, Map.empty)
    val result = executor.execute(batchId)
    
    // Verify both tables were persisted
    val table1Path = s"${globalConfig.paths.validatedPath}/${flowName}_${table1Name}"
    val table2Path = s"${globalConfig.paths.validatedPath}/${flowName}_${table2Name}"
    
    val table1Exists = Try {
      spark.read.parquet(table1Path).count() > 0
    }.getOrElse(false)
    
    val table2Exists = Try {
      spark.read.parquet(table2Path).count() > 0
    }.getOrElse(false)
    
    // Cleanup
    Try {
      import org.apache.hadoop.fs.{FileSystem, Path}
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new Path(table1Path), true)
      fs.delete(new Path(table2Path), true)
    }
    
    result.success && table1Exists && table2Exists
  }
}
