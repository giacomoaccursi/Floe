package com.etl.framework.aggregation

import com.etl.framework.config._
import com.etl.framework.core.AdditionalTableMetadata
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.util.Try

class AutoDiscoveryTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("AutoDiscoveryTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  private def createGlobalConfig(tempDir: String): GlobalConfig = GlobalConfig(
    paths = PathsConfig(
      outputPath = "/data/output",
      rejectedPath = "/data/rejected",
      metadataPath = tempDir
    ),
    processing = ProcessingConfig("yyyyMMdd_HHmmss", false, 0.1),
    performance = PerformanceConfig(false, false)
  )

  private def writeMetadata(
      metadataPath: String,
      tables: Seq[AdditionalTableMetadataFile]
  ): Unit = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints)

    val additionalTablesDir =
      Paths.get(s"$metadataPath/latest/additional_tables")
    Files.createDirectories(additionalTablesDir)

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
      val metadataFile =
        additionalTablesDir.resolve(s"${table.tableName}.json")
      Files.write(
        metadataFile,
        jsonString.getBytes,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING
      )
    }
  }

  private def cleanupDir(path: String): Unit = {
    Try {
      import scala.collection.JavaConverters._
      val dirPath = Paths.get(path)
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

  private val sampleTables = Seq(
    AdditionalTableMetadataFile(
      tableName = "summary_sales",
      path = "/data/validated/summary_sales",
      dagMetadata = AdditionalTableMetadata(
        primaryKey = Seq("category"),
        joinKeys = Map("customers" -> Seq("customer_id")),
        description = Some("Sales summary"),
        partitionBy = Seq.empty
      )
    ),
    AdditionalTableMetadataFile(
      tableName = "aggregate_orders",
      path = "/data/validated/aggregate_orders",
      dagMetadata = AdditionalTableMetadata(
        primaryKey = Seq("order_id"),
        joinKeys = Map("products" -> Seq("product_id")),
        description = Some("Order aggregates"),
        partitionBy = Seq("category")
      )
    )
  )

  "Auto-discovery" should "create DAG nodes for additional tables" in {
    val tempDir = Files.createTempDirectory("dag-test-").toString
    try {
      val globalConfig = createGlobalConfig(tempDir)
      writeMetadata(tempDir, sampleTables)

      val tableDiscovery = new AdditionalTableDiscovery(globalConfig)
      val discoveredNodes = tableDiscovery.discoverAdditionalTables()

      val discoveredNames = discoveredNodes.map(_.sourceFlow).toSet
      val expectedNames = sampleTables.map(_.tableName).toSet

      expectedNames.subsetOf(discoveredNames) shouldBe true

      discoveredNodes.foreach { node =>
        val table = sampleTables.find(_.tableName == node.sourceFlow)
        table shouldBe defined
        node.id shouldBe s"${table.get.tableName}_node"
        node.sourcePath shouldBe table.get.path
      }
    } finally {
      cleanupDir(tempDir)
    }
  }

  it should "infer dependencies from join keys" in {
    val tempDir = Files.createTempDirectory("dag-test-").toString
    try {
      val table = sampleTables.head // has joinKeys: customers -> customer_id
      val globalConfig = createGlobalConfig(tempDir)
      writeMetadata(tempDir, Seq(table))

      val tableDiscovery = new AdditionalTableDiscovery(globalConfig)
      val discoveredNodes = tableDiscovery.discoverAdditionalTables()

      val discoveredNode =
        discoveredNodes.find(_.sourceFlow == table.tableName)
      discoveredNode shouldBe defined

      val expectedDeps =
        table.dagMetadata.joinKeys.keys.map(f => s"${f}_node").toSet
      discoveredNode.get.dependencies.toSet shouldBe expectedDeps
    } finally {
      cleanupDir(tempDir)
    }
  }

  it should "create join configuration" in {
    val tempDir = Files.createTempDirectory("dag-test-").toString
    try {
      val table = sampleTables.head
      val globalConfig = createGlobalConfig(tempDir)
      writeMetadata(tempDir, Seq(table))

      val tableDiscovery = new AdditionalTableDiscovery(globalConfig)
      val discoveredNodes = tableDiscovery.discoverAdditionalTables()

      val discoveredNode =
        discoveredNodes.find(_.sourceFlow == table.tableName)
      discoveredNode shouldBe defined
      discoveredNode.get.join shouldBe defined

      val joinConfig = discoveredNode.get.join.get
      joinConfig.`type` shouldBe JoinType.LeftOuter
      joinConfig.strategy shouldBe JoinStrategy.Nest
      joinConfig.nestAs shouldBe Some(table.tableName)

      val firstJoinKeyFlow = table.dagMetadata.joinKeys.head._1
      joinConfig.parent shouldBe s"${firstJoinKeyFlow}_node"
    } finally {
      cleanupDir(tempDir)
    }
  }

  it should "handle missing metadata directory" in {
    val tempDir = Files.createTempDirectory("dag-test-").toString
    try {
      val globalConfig = createGlobalConfig(tempDir)
      // Don't create metadata files

      val tableDiscovery = new AdditionalTableDiscovery(globalConfig)
      val discoveredNodes = tableDiscovery.discoverAdditionalTables()

      discoveredNodes shouldBe empty
    } finally {
      cleanupDir(tempDir)
    }
  }

  it should "return empty list when auto-discovery is disabled" in {
    val tempDir = Files.createTempDirectory("dag-test-").toString
    try {
      val globalConfig = createGlobalConfig(tempDir)
      writeMetadata(tempDir, sampleTables)

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
        description = "Test",
        version = "1.0",
        nodes = Seq(configuredNode)
      )

      val orchestrator = new DAGOrchestrator(
        dagConfig,
        globalConfig,
        autoDiscoverAdditionalTables = false
      )
      val plan = orchestrator.buildExecutionPlan()

      val allNodes = plan.groups.flatMap(_.nodes)
      allNodes should have size 1
      allNodes.head.id shouldBe "configured_node"
    } finally {
      cleanupDir(tempDir)
    }
  }
}
