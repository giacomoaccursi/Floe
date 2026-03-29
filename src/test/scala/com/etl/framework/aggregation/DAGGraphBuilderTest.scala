package com.etl.framework.aggregation

import com.etl.framework.config._
import com.etl.framework.exceptions.CircularDependencyException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DAGGraphBuilderTest extends AnyFlatSpec with Matchers {

  def createGlobalConfig(parallelFlows: Boolean = true): GlobalConfig = {
    GlobalConfig(
      paths = PathsConfig("/output", "/rejected", "/metadata"),
      performance = PerformanceConfig(parallelFlows = parallelFlows),
      iceberg = IcebergConfig(warehouse = "/tmp/test-warehouse")
    )
  }

  def leaf(id: String): DAGNode =
    DAGNode(id = id, sourceFlow = s"flow_$id")

  def child(id: String, withNode: String): DAGNode =
    DAGNode(
      id = id,
      sourceFlow = s"flow_$id",
      joins = Seq(JoinConfig(
        `type` = JoinType.LeftOuter,
        `with` = withNode,
        conditions = Seq(JoinCondition("id", "id")),
        strategy = JoinStrategy.Nest
      ))
    )

  "DAGGraphBuilder" should "build dependency graph with no joins" in {
    val builder = new DAGGraphBuilder(parallelNodes = false)
    val graph = builder.buildDependencyGraph(Seq(leaf("A"), leaf("B"), leaf("C")))

    graph.size shouldBe 3
    graph("A") shouldBe empty
    graph("B") shouldBe empty
    graph("C") shouldBe empty
  }

  it should "infer dependency from join parent" in {
    val builder = new DAGGraphBuilder(parallelNodes = false)
    val graph = builder.buildDependencyGraph(Seq(leaf("A"), child("B", "A")))

    graph("A") shouldBe empty
    graph("B") shouldBe Set("A")
  }

  it should "build linear chain from joins" in {
    val builder = new DAGGraphBuilder(parallelNodes = false)
    val graph = builder.buildDependencyGraph(Seq(leaf("A"), child("B", "A"), child("C", "B")))

    graph("A") shouldBe empty
    graph("B") shouldBe Set("A")
    graph("C") shouldBe Set("B")
  }

  it should "build diamond execution plan" in {
    val builder = new DAGGraphBuilder(parallelNodes = false)
    // A <- B <- C (linear chain)
    val nodes = Seq(leaf("A"), child("B", "A"), child("C", "B"))

    val plan = builder.buildExecutionPlan(nodes)

    val allIds = plan.groups.flatMap(_.nodes.map(_.id))
    allIds.indexOf("A") should be < allIds.indexOf("B")
    allIds.indexOf("B") should be < allIds.indexOf("C")
    plan.rootNode shouldBe "C"
  }

  it should "identify root node (node with no dependents)" in {
    val builder = new DAGGraphBuilder(parallelNodes = false)
    val plan = builder.buildExecutionPlan(Seq(leaf("A"), child("B", "A"), child("C", "B")))
    plan.rootNode shouldBe "C"
  }

  it should "detect circular dependency via join parents" in {
    val builder = new DAGGraphBuilder(parallelNodes = false)
    intercept[CircularDependencyException] {
      builder.buildExecutionPlan(Seq(child("A", "B"), child("B", "A")))
    }
  }

  it should "reject self-referential join" in {
    val builder = new DAGGraphBuilder(parallelNodes = false)
    intercept[Exception] {
      builder.buildExecutionPlan(Seq(child("A", "A")))
    }
  }

  it should "group independent nodes for parallel execution" in {
    val builder = new DAGGraphBuilder(parallelNodes = true)
    val plan = builder.buildExecutionPlan(Seq(leaf("A"), leaf("B"), leaf("C")))

    plan.groups.size shouldBe 1
    plan.groups.head.nodes.size shouldBe 3
    plan.groups.head.parallel shouldBe true
  }

  it should "disable parallel when parallelNodes is false" in {
    val builder = new DAGGraphBuilder(parallelNodes = false)
    val plan = builder.buildExecutionPlan(Seq(leaf("A"), leaf("B")))

    plan.groups.head.parallel shouldBe false
  }

  it should "handle single node DAG" in {
    val builder = new DAGGraphBuilder(parallelNodes = false)
    val plan = builder.buildExecutionPlan(Seq(leaf("A")))

    plan.groups.size shouldBe 1
    plan.rootNode shouldBe "A"
  }
}
