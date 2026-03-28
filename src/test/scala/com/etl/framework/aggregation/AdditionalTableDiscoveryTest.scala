package com.etl.framework.aggregation

import com.etl.framework.config._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.nio.file.{Files, Path}

class AdditionalTableDiscoveryTest extends AnyFlatSpec with Matchers {

  def createGlobalConfig(metadataPath: String): GlobalConfig = {
    GlobalConfig(
      paths = PathsConfig("/output", "/rejected", metadataPath),
      processing = ProcessingConfig("yyyyMMdd", failOnValidationError = false, maxRejectionRate = 0.1),
      performance = PerformanceConfig(parallelFlows = true, parallelNodes = true),
      iceberg = IcebergConfig(warehouse = "/tmp/test-warehouse")
    )
  }

  def createTempDir(): Path = {
    Files.createTempDirectory("additional_table_discovery_test")
  }

  def writeMetadataFile(dir: Path, fileName: String, content: String): Unit = {
    val additionalTablesDir = dir.resolve("latest").resolve("additional_tables")
    Files.createDirectories(additionalTablesDir)
    Files.write(additionalTablesDir.resolve(fileName), content.getBytes)
  }

  "AdditionalTableDiscovery" should "return empty when metadata directory does not exist" in {
    val globalConfig = createGlobalConfig("/tmp/nonexistent_path_12345")
    val discovery = new AdditionalTableDiscovery(globalConfig)

    val nodes = discovery.discoverAdditionalTables()

    nodes shouldBe empty
  }

  it should "return empty when additional_tables directory does not exist" in {
    val tempDir = createTempDir()
    try {
      Files.createDirectories(tempDir.resolve("latest"))

      val globalConfig = createGlobalConfig(tempDir.toString)
      val discovery = new AdditionalTableDiscovery(globalConfig)

      val nodes = discovery.discoverAdditionalTables()

      nodes shouldBe empty
    } finally {
      deleteRecursively(tempDir)
    }
  }

  it should "discover additional table from valid metadata file" in {
    val tempDir = createTempDir()
    try {
      val metadataJson =
        """{
          |  "table_name": "order_items",
          |  "path": "/data/order_items",
          |  "dag_metadata": {
          |    "primary_key": ["id"],
          |    "join_keys": {"orders": ["order_id"]},
          |    "description": "Order items table",
          |    "partition_by": ["date"]
          |  }
          |}""".stripMargin

      writeMetadataFile(tempDir, "order_items.json", metadataJson)

      val globalConfig = createGlobalConfig(tempDir.toString)
      val discovery = new AdditionalTableDiscovery(globalConfig)

      val nodes = discovery.discoverAdditionalTables()

      nodes should have size 1
      val node = nodes.head
      node.id shouldBe "order_items_node"
      node.sourceFlow shouldBe "order_items"
      node.sourcePath shouldBe "/data/order_items"
      node.description shouldBe "Order items table"
    } finally {
      deleteRecursively(tempDir)
    }
  }

  it should "infer dependencies from join keys" in {
    val tempDir = createTempDir()
    try {
      val metadataJson =
        """{
          |  "table_name": "details",
          |  "path": "/data/details",
          |  "dag_metadata": {
          |    "primary_key": ["id"],
          |    "join_keys": {"orders": ["order_id"], "customers": ["customer_id"]},
          |    "description": "Details table"
          |  }
          |}""".stripMargin

      writeMetadataFile(tempDir, "details.json", metadataJson)

      val globalConfig = createGlobalConfig(tempDir.toString)
      val discovery = new AdditionalTableDiscovery(globalConfig)

      val nodes = discovery.discoverAdditionalTables()

      nodes should have size 1
      val node = nodes.head
      // Dependencies should be inferred from join keys: orders_node, customers_node
      node.dependencies should contain allOf ("orders_node", "customers_node")
    } finally {
      deleteRecursively(tempDir)
    }
  }

  it should "infer join config from metadata" in {
    val tempDir = createTempDir()
    try {
      val metadataJson =
        """{
          |  "table_name": "line_items",
          |  "path": "/data/line_items",
          |  "dag_metadata": {
          |    "primary_key": ["id"],
          |    "join_keys": {"orders": ["order_id"]},
          |    "description": "Line items"
          |  }
          |}""".stripMargin

      writeMetadataFile(tempDir, "line_items.json", metadataJson)

      val globalConfig = createGlobalConfig(tempDir.toString)
      val discovery = new AdditionalTableDiscovery(globalConfig)

      val nodes = discovery.discoverAdditionalTables()
      val node = nodes.head

      node.join shouldBe defined
      val joinConfig = node.join.get
      joinConfig.`type` shouldBe JoinType.LeftOuter
      joinConfig.parent shouldBe "orders_node"
      joinConfig.strategy shouldBe JoinStrategy.Nest
      joinConfig.nestAs shouldBe Some("line_items")
      joinConfig.conditions should have size 1
      joinConfig.conditions.head.left shouldBe "order_id"
      joinConfig.conditions.head.right shouldBe "order_id"
    } finally {
      deleteRecursively(tempDir)
    }
  }

  it should "discover multiple additional tables" in {
    val tempDir = createTempDir()
    try {
      val metadata1 =
        """{
          |  "table_name": "table_a",
          |  "path": "/data/table_a",
          |  "dag_metadata": {
          |    "primary_key": ["id"],
          |    "join_keys": {"flow_x": ["x_id"]}
          |  }
          |}""".stripMargin

      val metadata2 =
        """{
          |  "table_name": "table_b",
          |  "path": "/data/table_b",
          |  "dag_metadata": {
          |    "primary_key": ["id"],
          |    "join_keys": {"flow_y": ["y_id"]}
          |  }
          |}""".stripMargin

      writeMetadataFile(tempDir, "table_a.json", metadata1)
      writeMetadataFile(tempDir, "table_b.json", metadata2)

      val globalConfig = createGlobalConfig(tempDir.toString)
      val discovery = new AdditionalTableDiscovery(globalConfig)

      val nodes = discovery.discoverAdditionalTables()

      nodes.size shouldBe 2
      nodes.map(_.sourceFlow) should contain allOf ("table_a", "table_b")
    } finally {
      deleteRecursively(tempDir)
    }
  }

  it should "skip files without dag_metadata" in {
    val tempDir = createTempDir()
    try {
      val metadataWithDag =
        """{
          |  "table_name": "with_dag",
          |  "path": "/data/with_dag",
          |  "dag_metadata": {
          |    "primary_key": ["id"],
          |    "join_keys": {"flow_x": ["x_id"]}
          |  }
          |}""".stripMargin

      val metadataWithoutDag =
        """{
          |  "table_name": "without_dag",
          |  "path": "/data/without_dag"
          |}""".stripMargin

      writeMetadataFile(tempDir, "with_dag.json", metadataWithDag)
      writeMetadataFile(tempDir, "without_dag.json", metadataWithoutDag)

      val globalConfig = createGlobalConfig(tempDir.toString)
      val discovery = new AdditionalTableDiscovery(globalConfig)

      val nodes = discovery.discoverAdditionalTables()

      nodes.size shouldBe 1
      nodes.head.sourceFlow shouldBe "with_dag"
    } finally {
      deleteRecursively(tempDir)
    }
  }

  it should "skip non-JSON files" in {
    val tempDir = createTempDir()
    try {
      val metadataJson =
        """{
          |  "table_name": "valid",
          |  "path": "/data/valid",
          |  "dag_metadata": {
          |    "primary_key": ["id"],
          |    "join_keys": {"flow_x": ["x_id"]}
          |  }
          |}""".stripMargin

      writeMetadataFile(tempDir, "valid.json", metadataJson)
      writeMetadataFile(tempDir, "readme.txt", "This is not JSON")

      val globalConfig = createGlobalConfig(tempDir.toString)
      val discovery = new AdditionalTableDiscovery(globalConfig)

      val nodes = discovery.discoverAdditionalTables()

      nodes.size shouldBe 1
      nodes.head.sourceFlow shouldBe "valid"
    } finally {
      deleteRecursively(tempDir)
    }
  }

  it should "handle malformed JSON gracefully" in {
    val tempDir = createTempDir()
    try {
      writeMetadataFile(tempDir, "bad.json", "{ invalid json }")

      val globalConfig = createGlobalConfig(tempDir.toString)
      val discovery = new AdditionalTableDiscovery(globalConfig)

      val nodes = discovery.discoverAdditionalTables()

      nodes shouldBe empty
    } finally {
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.isDirectory(path)) {
      val stream = Files.list(path)
      try {
        stream.forEach(p => deleteRecursively(p))
      } finally {
        stream.close()
      }
    }
    Files.deleteIfExists(path)
  }
}
