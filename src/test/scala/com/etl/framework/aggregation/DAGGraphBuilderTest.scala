package com.etl.framework.aggregation

import com.etl.framework.config._
import com.etl.framework.exceptions.CircularDependencyException
import com.etl.framework.util.TopologicalSorter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DAGGraphBuilderTest extends AnyFlatSpec with Matchers {

  def leaf(id: String): DAGNode =
    DAGNode(id = id, sourceFlow = s"flow_$id")

  def child(id: String, withNode: String): DAGNode =
    DAGNode(
      id = id,
      sourceFlow = s"flow_$id",
      joins = Seq(
        JoinConfig(
          `type` = JoinType.LeftOuter,
          `with` = withNode,
          conditions = Seq(JoinCondition("id", "id")),
          strategy = JoinStrategy.Nest
        )
      )
    )

  "DAGGraphBuilder" should "build dependency graph with no joins" in {
    val graph = TopologicalSorter.buildGraph[DAGNode](
      Seq(leaf("A"), leaf("B"), leaf("C")),
      _.id,
      _.joins.map(_.`with`).toSet
    )
    graph.size shouldBe 3
    graph("A") shouldBe empty
    graph("B") shouldBe empty
    graph("C") shouldBe empty
  }

  it should "infer dependency from join" in {
    val graph = TopologicalSorter.buildGraph[DAGNode](
      Seq(leaf("A"), child("B", "A")),
      _.id,
      _.joins.map(_.`with`).toSet
    )
    graph("A") shouldBe empty
    graph("B") shouldBe Set("A")
  }

  it should "build linear chain from joins" in {
    val graph = TopologicalSorter.buildGraph[DAGNode](
      Seq(leaf("A"), child("B", "A"), child("C", "B")),
      _.id,
      _.joins.map(_.`with`).toSet
    )
    graph("A") shouldBe empty
    graph("B") shouldBe Set("A")
    graph("C") shouldBe Set("B")
  }

  it should "build execution plan in correct order" in {
    val builder = new DAGGraphBuilder(parallelNodes = false)
    val plan = builder.buildExecutionPlan(Seq(leaf("A"), child("B", "A"), child("C", "B")))

    val allIds = plan.groups.flatMap(_.nodes.map(_.id))
    allIds.indexOf("A") should be < allIds.indexOf("B")
    allIds.indexOf("B") should be < allIds.indexOf("C")
    plan.rootNode shouldBe "C"
  }

  it should "identify root node" in {
    val builder = new DAGGraphBuilder(parallelNodes = false)
    val plan = builder.buildExecutionPlan(Seq(leaf("A"), child("B", "A"), child("C", "B")))
    plan.rootNode shouldBe "C"
  }

  it should "reject multiple root nodes" in {
    val builder = new DAGGraphBuilder(parallelNodes = false)
    // A and B are both roots (no one depends on them via joins)
    // C depends on A, D depends on B — but A and B are independent roots
    val ex = intercept[IllegalStateException] {
      builder.buildExecutionPlan(Seq(leaf("A"), leaf("B"), child("C", "A"), child("D", "B")))
    }
    ex.getMessage should include("Multiple root nodes")
  }

  it should "detect circular dependency" in {
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
    // A, B are independent leaves; C joins both → single root C, group 1 has A,B in parallel
    val nodeC = DAGNode(
      id = "C",
      sourceFlow = "flow_C",
      joins = Seq(
        JoinConfig(
          `type` = JoinType.LeftOuter,
          `with` = "A",
          conditions = Seq(JoinCondition("id", "id")),
          strategy = JoinStrategy.Nest
        ),
        JoinConfig(
          `type` = JoinType.LeftOuter,
          `with` = "B",
          conditions = Seq(JoinCondition("id", "id")),
          strategy = JoinStrategy.Flatten
        )
      )
    )
    val plan = builder.buildExecutionPlan(Seq(leaf("A"), leaf("B"), nodeC))

    plan.groups.head.parallel shouldBe true
    plan.groups.head.nodes.map(_.id).toSet shouldBe Set("A", "B")
    plan.rootNode shouldBe "C"
  }

  it should "disable parallel when parallelNodes is false" in {
    val builder = new DAGGraphBuilder(parallelNodes = false)
    val plan = builder.buildExecutionPlan(Seq(leaf("A"), child("B", "A")))
    plan.groups.head.parallel shouldBe false
    plan.rootNode shouldBe "B"
  }

  it should "handle single node DAG" in {
    val builder = new DAGGraphBuilder(parallelNodes = false)
    val plan = builder.buildExecutionPlan(Seq(leaf("A")))
    plan.groups.size shouldBe 1
    plan.rootNode shouldBe "A"
  }
}
