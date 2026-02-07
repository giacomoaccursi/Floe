package com.etl.framework.aggregation

import com.etl.framework.config._
import com.etl.framework.config.FileFormat
import com.etl.framework.config.JoinStrategy
import com.etl.framework.TestConfig
import com.etl.framework.config.FileFormat.Parquet
import com.etl.framework.core.AdditionalTableMetadata
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import java.nio.file.{Files, Paths, StandardOpenOption}

/** Property-based tests for auto-discovery of additional tables Feature:
  * spark-etl-framework, Property 31: Auto-Discovery of Additional Tables
  * Validates: Requirements 24.1, 24.2
  *
  * Test configuration: Uses standardTestCount (20 cases)
  */
object AutoDiscoveryProperties extends Properties("AutoDiscovery") {

  // Configure test parameters
  override def overrideParameters(p: Parameters): Parameters =
    TestConfig.standardParams

  // Create a minimal Spark session for testing
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("AutoDiscoveryPropertiesTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  // Generator for table names with timestamp to ensure uniqueness
  val tableNameGen: Gen[String] = for {
    prefix <- Gen.oneOf("summary", "aggregate", "derived", "enriched")
    suffix <- Gen.choose(1, 100000)
    timestamp <- Gen.const(System.nanoTime())
  } yield s"${prefix}_${suffix}_${timestamp}"

  // Generator for additional table metadata
  val additionalTableMetadataGen: Gen[AdditionalTableMetadata] = for {
    pkCount <- Gen.choose(1, 3)
    pkColumns <- Gen
      .listOfN(pkCount, Gen.alphaNumStr.suchThat(_.nonEmpty))
      .map(_.distinct)
    joinKeyCount <- Gen.choose(0, 3)
    joinKeys <-
      if (joinKeyCount > 0) {
        Gen
          .listOfN(
            joinKeyCount,
            for {
              flowName <- Gen.alphaNumStr.suchThat(_.nonEmpty)
              keyCount <- Gen.choose(1, 2)
              keys <- Gen
                .listOfN(keyCount, Gen.alphaNumStr.suchThat(_.nonEmpty))
            } yield (flowName, keys)
          )
          .map(_.toMap)
      } else {
        Gen.const(Map.empty[String, Seq[String]])
      }
    description <- Gen.option(Gen.alphaNumStr)
    partitionCount <- Gen.choose(0, 2)
    partitionBy <- Gen.listOfN(
      partitionCount,
      Gen.alphaNumStr.suchThat(_.nonEmpty)
    )
  } yield AdditionalTableMetadata(
    primaryKey = pkColumns,
    joinKeys = joinKeys,
    description = description,
    partitionBy = partitionBy
  )

  // Generator for additional table metadata file
  val additionalTableMetadataFileGen: Gen[AdditionalTableMetadataFile] = for {
    tableName <- tableNameGen
    metadata <- additionalTableMetadataGen
  } yield AdditionalTableMetadataFile(
    tableName = tableName,
    path = s"/data/validated/$tableName",
    dagMetadata = metadata
  )

  // Generator for GlobalConfig with temporary metadata path (unique per test)
  def globalConfigWithTempPathGen: Gen[GlobalConfig] = for {
    uniqueId <- Gen.const(
      s"dag-test-${System.nanoTime()}-${java.util.UUID.randomUUID()}"
    )
    tempDir <- Gen.const(Files.createTempDirectory(uniqueId))
  } yield GlobalConfig(
    paths = PathsConfig(
      validatedPath = "/data/validated",
      rejectedPath = "/data/rejected",
      metadataPath = tempDir.toString
    ),
    processing = ProcessingConfig(
      batchIdFormat = "yyyyMMdd_HHmmss",
      failOnValidationError = false,
      maxRejectionRate = 0.1
    ),
    performance = PerformanceConfig(
      parallelFlows = false,
      parallelNodes = false
    )
  )

  /** Writes additional table metadata to the metadata directory
    */
  def writeAdditionalTableMetadata(
      metadataPath: String,
      tables: Seq[AdditionalTableMetadataFile]
  ): Unit = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints)

    // Create latest/additional_tables directory
    val additionalTablesDir =
      Paths.get(s"$metadataPath/latest/additional_tables")
    Files.createDirectories(additionalTablesDir)

    // Write metadata for each table
    tables.foreach { table =>
      val metadata = Map(
        "table_name" -> table.tableName,
        "table_type" -> "additional",
        "path" -> table.path,
        "dag_metadata" -> Map(
          "primary_key" -> table.dagMetadata.primaryKey,
          "join_keys" -> table.dagMetadata.joinKeys,
          "description" -> table.dagMetadata.description.getOrElse(""),
          "partition_by" -> table.dagMetadata.partitionBy
        )
      )

      val jsonString = write(metadata)
      val metadataFile = additionalTablesDir.resolve(s"${table.tableName}.json")
      Files.write(
        metadataFile,
        jsonString.getBytes,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING
      )
    }
  }

  /** Cleans up temporary metadata directory with retry logic
    */
  def cleanupMetadataDir(metadataPath: String): Unit = {
    def attemptCleanup(retries: Int = 3): Unit = {
      try {
        val path = Paths.get(metadataPath)
        if (Files.exists(path)) {
          import scala.collection.JavaConverters._
          // Wait a bit to ensure all file handles are released
          Thread.sleep(50)

          val filesToDelete = Files
            .walk(path)
            .iterator()
            .asScala
            .toSeq
            .reverse

          filesToDelete.foreach { p =>
            try {
              Files.deleteIfExists(p)
            } catch {
              case _: Exception if retries > 0 =>
                // Retry on failure
                Thread.sleep(100)
                if (retries == 1) {
                  // Last attempt - force delete
                  p.toFile.delete()
                }
            }
          }
        }
      } catch {
        case _: Exception if retries > 0 =>
          Thread.sleep(100)
          attemptCleanup(retries - 1)
        case _: Exception =>
        // Ignore final cleanup errors - temp dirs will be cleaned by OS
      }
    }

    attemptCleanup()
  }

  /** Property 31: Auto-Discovery of Additional Tables For any additional tables
    * created with dagMetadata, when auto-discovery is enabled, the
    * DAG_Orchestrator should discover and create DAG nodes for them.
    */
  property("auto_discovery_creates_dag_nodes_for_additional_tables") = forAll(
    Gen
      .listOfN(3, additionalTableMetadataFileGen)
      .map(tables =>
        tables.groupBy(_.tableName).map(_._2.head).toSeq
      ), // Ensure unique table names
    globalConfigWithTempPathGen
  ) { (additionalTables, globalConfig) =>
    var metadataPathToClean: Option[String] = None
    try {
      metadataPathToClean = Some(globalConfig.paths.metadataPath)
      // Write additional table metadata
      writeAdditionalTableMetadata(
        globalConfig.paths.metadataPath,
        additionalTables
      )

      // Create DAG config with empty nodes (will be populated by auto-discovery)
      val dagConfig = AggregationConfig(
        name = "test_aggregation",
        description = "Test auto-discovery",
        version = "1.0",
        batchModel = ModelConfig(
          `class` = "com.example.BatchModel",
          mappingFile = None,
          mapperClass = None
        ),
        finalModel = ModelConfig(
          `class` = "com.example.FinalModel",
          mappingFile = None,
          mapperClass = None
        ),
        output = DAGOutputConfig(
          batch = OutputConfig(
            path = Some("/data/model/batch"),
            rejectedPath = None,
            format = FileFormat.Parquet,
            partitionBy = Seq.empty,
            compression = "snappy",
            options = Map.empty
          ),
          `final` = OutputConfig(
            path = Some("/data/model/final"),
            rejectedPath = None,
            format = FileFormat.Parquet,
            partitionBy = Seq.empty,
            compression = "snappy",
            options = Map.empty
          )
        ),
        nodes = Seq.empty // No configured nodes
      )

      // Create table discovery component
      val tableDiscovery = new AdditionalTableDiscovery(globalConfig)

      // Discover additional tables
      val discoveredNodes = tableDiscovery.discoverAdditionalTables()

      // Verify that all additional tables were discovered
      val discoveredTableNames = discoveredNodes.map(_.sourceFlow).toSet
      val expectedTableNames = additionalTables.map(_.tableName).toSet

      val allTablesDiscovered =
        expectedTableNames.subsetOf(discoveredTableNames)

      // Verify that discovered nodes have correct properties
      val nodesHaveCorrectProperties = discoveredNodes.forall { node =>
        val table = additionalTables.find(_.tableName == node.sourceFlow)
        table.exists { t =>
          // Node ID should be table_name_node
          node.id == s"${t.tableName}_node" &&
          // Source path should match
          node.sourcePath == t.path &&
          // Dependencies should be inferred from join keys
          node.dependencies.size == t.dagMetadata.joinKeys.size
        }
      }

      allTablesDiscovered && nodesHaveCorrectProperties

    } finally {
      // Cleanup with proper error handling
      metadataPathToClean.foreach(cleanupMetadataDir)
    }
  }

  /** Property 31: Auto-Discovery Infers Dependencies from Join Keys For any
    * additional table with join keys, auto-discovery should infer dependencies
    * based on the join keys.
    */
  property("auto_discovery_infers_dependencies_from_join_keys") = forAll(
    additionalTableMetadataFileGen.suchThat(_.dagMetadata.joinKeys.nonEmpty),
    globalConfigWithTempPathGen
  ) { (additionalTable, globalConfig) =>
    var metadataPathToClean: Option[String] = None
    try {
      metadataPathToClean = Some(globalConfig.paths.metadataPath)
      // Write additional table metadata
      writeAdditionalTableMetadata(
        globalConfig.paths.metadataPath,
        Seq(additionalTable)
      )

      // Create DAG config
      val dagConfig = AggregationConfig(
        name = "test_aggregation",
        description = "Test dependency inference",
        version = "1.0",
        batchModel = ModelConfig("com.example.BatchModel", None, None),
        finalModel = ModelConfig("com.example.FinalModel", None, None),
        output = DAGOutputConfig(
          batch = OutputConfig(
            Some("/data/model/batch"),
            None,
            FileFormat.Parquet,
            Seq.empty,
            "snappy",
            Map.empty
          ),
          `final` = OutputConfig(
            Some("/data/model/final"),
            None,
            FileFormat.Parquet,
            Seq.empty,
            "snappy",
            Map.empty
          )
        ),
        nodes = Seq.empty
      )

      // Create table discovery component
      val tableDiscovery = new AdditionalTableDiscovery(globalConfig)

      // Discover additional tables
      val discoveredNodes = tableDiscovery.discoverAdditionalTables()

      // Find the discovered node for this table
      val discoveredNode =
        discoveredNodes.find(_.sourceFlow == additionalTable.tableName)

      discoveredNode.exists { node =>
        // Dependencies should be flow_name_node for each join key
        val expectedDependencies = additionalTable.dagMetadata.joinKeys.keys
          .map(flow => s"${flow}_node")
          .toSet
        val actualDependencies = node.dependencies.toSet

        expectedDependencies == actualDependencies
      }

    } finally {
      metadataPathToClean.foreach(cleanupMetadataDir)
    }
  }

  /** Property 31: Auto-Discovery Creates Join Configuration For any additional
    * table with join keys, auto-discovery should create a join configuration.
    */
  property("auto_discovery_creates_join_configuration") = forAll(
    additionalTableMetadataFileGen.suchThat(_.dagMetadata.joinKeys.nonEmpty),
    globalConfigWithTempPathGen
  ) { (additionalTable, globalConfig) =>
    var metadataPathToClean: Option[String] = None
    try {
      metadataPathToClean = Some(globalConfig.paths.metadataPath)
      // Write additional table metadata
      writeAdditionalTableMetadata(
        globalConfig.paths.metadataPath,
        Seq(additionalTable)
      )

      // Create DAG config
      val dagConfig = AggregationConfig(
        name = "test_aggregation",
        description = "Test join configuration",
        version = "1.0",
        batchModel = ModelConfig("com.example.BatchModel", None, None),
        finalModel = ModelConfig("com.example.FinalModel", None, None),
        output = DAGOutputConfig(
          batch = OutputConfig(
            Some("/data/model/batch"),
            None,
            FileFormat.Parquet,
            Seq.empty,
            "snappy",
            Map.empty
          ),
          `final` = OutputConfig(
            Some("/data/model/final"),
            None,
            FileFormat.Parquet,
            Seq.empty,
            "snappy",
            Map.empty
          )
        ),
        nodes = Seq.empty
      )

      // Create table discovery component
      val tableDiscovery = new AdditionalTableDiscovery(globalConfig)

      // Discover additional tables
      val discoveredNodes = tableDiscovery.discoverAdditionalTables()

      // Find the discovered node for this table
      val discoveredNode =
        discoveredNodes.find(_.sourceFlow == additionalTable.tableName)

      discoveredNode.exists { node =>
        node.join.exists { joinConfig =>
          // Join should be left_outer
          joinConfig.`type` == "left_outer" &&
          // Strategy should be nest
          joinConfig.strategy == JoinStrategy.Nest &&
          // Nest as should be the table name
          joinConfig.nestAs.contains(additionalTable.tableName) &&
          // Parent should be first join key flow
          additionalTable.dagMetadata.joinKeys.headOption.exists {
            case (parentFlow, _) =>
              joinConfig.parent == s"${parentFlow}_node"
          }
        }
      }

    } finally {
      metadataPathToClean.foreach(cleanupMetadataDir)
    }
  }

  /** Property 31: Auto-Discovery Handles Missing Metadata Directory When the
    * metadata directory doesn't exist, auto-discovery should return empty list.
    */
  property("auto_discovery_handles_missing_metadata_directory") =
    forAll(globalConfigWithTempPathGen) { globalConfig =>
      var metadataPathToClean: Option[String] = None
      try {
        metadataPathToClean = Some(globalConfig.paths.metadataPath)
        // Don't create metadata directory

        // Create DAG config
        val dagConfig = AggregationConfig(
          name = "test_aggregation",
          description = "Test missing metadata",
          version = "1.0",
          batchModel = ModelConfig("com.example.BatchModel", None, None),
          finalModel = ModelConfig("com.example.FinalModel", None, None),
          output = DAGOutputConfig(
            batch = OutputConfig(
              Some("/data/model/batch"),
              None,
              Parquet,
              Seq.empty,
              "snappy",
              Map.empty
            ),
            `final` = OutputConfig(
              Some("/data/model/final"),
              None,
              Parquet,
              Seq.empty,
              "snappy",
              Map.empty
            )
          ),
          nodes = Seq.empty
        )

        // Create table discovery component
        val tableDiscovery = new AdditionalTableDiscovery(globalConfig)

        // Discover additional tables
        val discoveredNodes = tableDiscovery.discoverAdditionalTables()

        // Should return empty list
        discoveredNodes.isEmpty

      } finally {
        metadataPathToClean.foreach(cleanupMetadataDir)
      }
    }

  /** Property 31: Auto-Discovery Disabled Returns Empty List When
    * auto-discovery is disabled, no additional tables should be discovered.
    */
  property("auto_discovery_disabled_returns_empty_list") = forAll(
    Gen
      .listOfN(3, additionalTableMetadataFileGen)
      .map(tables =>
        tables.groupBy(_.tableName).map(_._2.head).toSeq
      ), // Ensure unique table names
    globalConfigWithTempPathGen
  ) { (additionalTables, globalConfig) =>
    var metadataPathToClean: Option[String] = None
    try {
      metadataPathToClean = Some(globalConfig.paths.metadataPath)
      // Write additional table metadata
      writeAdditionalTableMetadata(
        globalConfig.paths.metadataPath,
        additionalTables
      )

      // Create DAG config with some configured nodes
      val configuredNode = DAGNode(
        id = "configured_node",
        description = "Configured node",
        sourceFlow = "test_flow",
        sourcePath = "/data/validated/test_flow",
        dependencies = Seq.empty,
        join = None,
        select = Seq.empty,
        filters = Seq.empty
      )

      val dagConfig = AggregationConfig(
        name = "test_aggregation",
        description = "Test auto-discovery disabled",
        version = "1.0",
        batchModel = ModelConfig("com.example.BatchModel", None, None),
        finalModel = ModelConfig("com.example.FinalModel", None, None),
        output = DAGOutputConfig(
          batch = OutputConfig(
            Some("/data/model/batch"),
            None,
            FileFormat.Parquet,
            Seq.empty,
            "snappy",
            Map.empty
          ),
          `final` = OutputConfig(
            Some("/data/model/final"),
            None,
            FileFormat.Parquet,
            Seq.empty,
            "snappy",
            Map.empty
          )
        ),
        nodes = Seq(configuredNode)
      )

      // Create orchestrator with auto-discovery DISABLED
      val orchestrator = new DAGOrchestrator(
        dagConfig,
        globalConfig,
        autoDiscoverAdditionalTables = false
      )

      // Build execution plan - this should only include configured nodes
      val plan = orchestrator.buildExecutionPlan()

      // Flatten all nodes from all groups
      val allNodes = plan.groups.flatMap(_.nodes)

      // Should only have the configured node, no discovered nodes
      allNodes.size == 1 && allNodes.head.id == "configured_node"

    } finally {
      metadataPathToClean.foreach(cleanupMetadataDir)
    }
  }
}
