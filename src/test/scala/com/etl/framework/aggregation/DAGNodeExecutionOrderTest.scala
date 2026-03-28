package com.etl.framework.aggregation

import com.etl.framework.config._
import com.etl.framework.exceptions.CircularDependencyException
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DAGNodeExecutionOrderTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DAGNodeExecutionOrderTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  private val globalConfig = GlobalConfig(
    paths = PathsConfig(
      "/data/output",
      "/data/rejected",
      "/data/metadata"
    ),
    processing = ProcessingConfig("yyyyMMdd_HHmmss", false, 0.1),
    performance = PerformanceConfig(false, false),
    iceberg = IcebergConfig(warehouse = "/tmp/test-warehouse")
  )

  private def createNode(
      id: String,
      dependencies: Seq[String] = Seq.empty
  ): DAGNode = DAGNode(
    id = id,
    description = s"Node $id",
    sourceFlow = s"flow_${id}",
    sourcePath = s"/data/validated/flow_${id}",
    dependencies = dependencies,
    join = None,
    select = Seq.empty,
    filters = Seq.empty
  )

  private def createDagConfig(nodes: Seq[DAGNode]): AggregationConfig =
    AggregationConfig(
      name = "test_aggregation",
      description = "Test DAG",
      version = "1.0",
      nodes = nodes
    )

  "DAG execution order" should "respect dependencies" in {
    val nodeA = createNode("node_a")
    val nodeB = createNode("node_b", Seq("node_a"))
    val nodeC = createNode("node_c", Seq("node_b"))

    val dagConfig = createDagConfig(Seq(nodeA, nodeB, nodeC))
    val orchestrator = new DAGOrchestrator(
      dagConfig,
      globalConfig,
      autoDiscoverAdditionalTables = false
    )

    val plan = orchestrator.buildExecutionPlan()
    val executionOrder = plan.groups.flatMap(_.nodes.map(_.id))
    val positions = executionOrder.zipWithIndex.toMap

    positions("node_a") should be < positions("node_b")
    positions("node_b") should be < positions("node_c")
    executionOrder should have size 3
  }

  it should "contain all dependency edges in graph" in {
    val nodeA = createNode("node_a")
    val nodeB = createNode("node_b", Seq("node_a"))
    val nodeC = createNode("node_c", Seq("node_a", "node_b"))
    val nodes = Seq(nodeA, nodeB, nodeC)

    val graphBuilder = new DAGGraphBuilder(globalConfig)
    val graph = graphBuilder.buildDependencyGraph(nodes)

    graph should contain key "node_a"
    graph should contain key "node_b"
    graph should contain key "node_c"

    graph("node_a") shouldBe empty
    graph("node_b") should contain("node_a")
    graph("node_c") should contain allOf ("node_a", "node_b")
  }

  it should "detect circular dependencies" in {
    val nodeA = createNode("node_a", Seq("node_c"))
    val nodeB = createNode("node_b", Seq("node_a"))
    val nodeC = createNode("node_c", Seq("node_b"))

    val dagConfig = createDagConfig(Seq(nodeA, nodeB, nodeC))
    val orchestrator = new DAGOrchestrator(
      dagConfig,
      globalConfig,
      autoDiscoverAdditionalTables = false
    )

    intercept[CircularDependencyException] {
      orchestrator.buildExecutionPlan()
    }
  }

  it should "handle empty node list" in {
    val dagConfig = createDagConfig(Seq.empty)
    val orchestrator = new DAGOrchestrator(
      dagConfig,
      globalConfig,
      autoDiscoverAdditionalTables = false
    )

    try {
      val plan = orchestrator.buildExecutionPlan()
      plan.groups shouldBe empty
    } catch {
      case _: IllegalStateException =>
        // Acceptable for empty DAG
        succeed
    }
  }

  it should "produce single group for single node" in {
    val node = createNode("single_node")
    val dagConfig = createDagConfig(Seq(node))
    val orchestrator = new DAGOrchestrator(
      dagConfig,
      globalConfig,
      autoDiscoverAdditionalTables = false
    )

    val plan = orchestrator.buildExecutionPlan()

    plan.groups should have size 1
    plan.groups.head.nodes should have size 1
    plan.groups.head.nodes.head.id shouldBe "single_node"
  }
}
