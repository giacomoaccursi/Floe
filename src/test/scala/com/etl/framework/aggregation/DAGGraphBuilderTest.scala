package com.etl.framework.aggregation

import com.etl.framework.config._
import com.etl.framework.exceptions.CircularDependencyException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DAGGraphBuilderTest extends AnyFlatSpec with Matchers {

  def createGlobalConfig(parallelNodes: Boolean = true): GlobalConfig = {
    GlobalConfig(
      paths = PathsConfig("/output", "/rejected", "/metadata"),
      processing = ProcessingConfig("yyyyMMdd", failOnValidationError = false, maxRejectionRate = 0.1),
      performance = PerformanceConfig(parallelFlows = true, parallelNodes = parallelNodes)
    )
  }

  def createNode(id: String, dependencies: Seq[String] = Seq.empty): DAGNode = {
    DAGNode(
      id = id,
      description = s"Node $id",
      sourceFlow = s"flow_$id",
      sourcePath = s"/path/$id",
      dependencies = dependencies
    )
  }

  def createNodeWithJoin(
      id: String,
      joinParent: String,
      dependencies: Seq[String] = Seq.empty
  ): DAGNode = {
    DAGNode(
      id = id,
      description = s"Node $id",
      sourceFlow = s"flow_$id",
      sourcePath = s"/path/$id",
      dependencies = dependencies,
      join = Some(
        JoinConfig(
          `type` = JoinType.LeftOuter,
          parent = joinParent,
          conditions = Seq(JoinCondition("id", "id")),
          strategy = JoinStrategy.Nest
        )
      )
    )
  }

  "DAGGraphBuilder" should "build dependency graph with no dependencies" in {
    val builder = new DAGGraphBuilder(createGlobalConfig())
    val nodes = Seq(createNode("A"), createNode("B"), createNode("C"))

    val graph = builder.buildDependencyGraph(nodes)

    graph.size shouldBe 3
    graph("A") shouldBe empty
    graph("B") shouldBe empty
    graph("C") shouldBe empty
  }

  it should "build dependency graph with linear dependencies" in {
    val builder = new DAGGraphBuilder(createGlobalConfig())
    val nodes = Seq(
      createNode("A"),
      createNode("B", Seq("A")),
      createNode("C", Seq("B"))
    )

    val graph = builder.buildDependencyGraph(nodes)

    graph("A") shouldBe empty
    graph("B") shouldBe Set("A")
    graph("C") shouldBe Set("B")
  }

  it should "build dependency graph with multiple dependencies" in {
    val builder = new DAGGraphBuilder(createGlobalConfig())
    val nodes = Seq(
      createNode("A"),
      createNode("B"),
      createNode("C", Seq("A", "B"))
    )

    val graph = builder.buildDependencyGraph(nodes)

    graph("C") shouldBe Set("A", "B")
  }

  it should "build execution plan with correct topological order" in {
    val builder = new DAGGraphBuilder(createGlobalConfig())
    val nodes = Seq(
      createNode("A"),
      createNode("B", Seq("A")),
      createNode("C", Seq("A")),
      createNode("D", Seq("B", "C"))
    )

    val plan = builder.buildExecutionPlan(nodes)

    // Should have 3 groups: [A], [B,C], [D]
    plan.groups.size shouldBe 3

    plan.groups(0).nodes.map(_.id) should contain("A")

    plan.groups(1).nodes.size shouldBe 2
    plan.groups(1).nodes.map(_.id) should contain allOf ("B", "C")
    plan.groups(1).parallel shouldBe true

    plan.groups(2).nodes.map(_.id) should contain("D")
  }

  it should "identify root node correctly" in {
    val builder = new DAGGraphBuilder(createGlobalConfig())
    val nodes = Seq(
      createNode("A"),
      createNode("B", Seq("A")),
      createNode("C", Seq("B"))
    )

    val plan = builder.buildExecutionPlan(nodes)

    // C is the root (no other node depends on it)
    plan.rootNode shouldBe "C"
  }

  it should "detect circular dependencies" in {
    val builder = new DAGGraphBuilder(createGlobalConfig())
    val nodes = Seq(
      createNode("A", Seq("C")),
      createNode("B", Seq("A")),
      createNode("C", Seq("B"))
    )

    val exception = intercept[CircularDependencyException] {
      builder.buildExecutionPlan(nodes)
    }

    exception.errorCode shouldBe "CONFIG_CIRCULAR_DEPENDENCY"
    exception.cycle should not be empty
  }

  it should "detect self-referencing dependency" in {
    val builder = new DAGGraphBuilder(createGlobalConfig())
    val nodes = Seq(createNode("A", Seq("A")))

    intercept[CircularDependencyException] {
      builder.buildExecutionPlan(nodes)
    }
  }

  it should "disable parallel execution when parallelNodes is false" in {
    val builder = new DAGGraphBuilder(createGlobalConfig(parallelNodes = false))
    val nodes = Seq(
      createNode("A"),
      createNode("B"),
      createNode("C", Seq("A", "B"))
    )

    val plan = builder.buildExecutionPlan(nodes)

    // First group has A and B but parallel should be false
    plan.groups(0).nodes.size shouldBe 2
    plan.groups(0).parallel shouldBe false
  }

  it should "handle single node DAG" in {
    val builder = new DAGGraphBuilder(createGlobalConfig())
    val nodes = Seq(createNode("A"))

    val plan = builder.buildExecutionPlan(nodes)

    plan.groups.size shouldBe 1
    plan.groups.head.nodes.size shouldBe 1
    plan.groups.head.nodes.head.id shouldBe "A"
    plan.rootNode shouldBe "A"
  }

  it should "handle complex diamond dependency" in {
    val builder = new DAGGraphBuilder(createGlobalConfig())
    // Diamond: A -> B, A -> C, B -> D, C -> D
    val nodes = Seq(
      createNode("A"),
      createNode("B", Seq("A")),
      createNode("C", Seq("A")),
      createNode("D", Seq("B", "C"))
    )

    val plan = builder.buildExecutionPlan(nodes)

    // Verify ordering: A before B,C before D
    val allNodeIds = plan.groups.flatMap(_.nodes.map(_.id))
    allNodeIds.indexOf("A") should be < allNodeIds.indexOf("B")
    allNodeIds.indexOf("A") should be < allNodeIds.indexOf("C")
    allNodeIds.indexOf("B") should be < allNodeIds.indexOf("D")
    allNodeIds.indexOf("C") should be < allNodeIds.indexOf("D")

    // Root node should be D (nothing depends on D)
    plan.rootNode shouldBe "D"
  }

  it should "group independent nodes for parallel execution" in {
    val builder = new DAGGraphBuilder(createGlobalConfig(parallelNodes = true))
    val nodes = Seq(
      createNode("A"),
      createNode("B"),
      createNode("C")
    )

    val plan = builder.buildExecutionPlan(nodes)

    // All nodes are independent, should be in one parallel group
    plan.groups.size shouldBe 1
    plan.groups.head.nodes.size shouldBe 3
    plan.groups.head.parallel shouldBe true
  }

  it should "handle wide dependency tree" in {
    val builder = new DAGGraphBuilder(createGlobalConfig())
    // A is root, B/C/D/E all depend on A, F depends on all of them
    val nodes = Seq(
      createNode("A"),
      createNode("B", Seq("A")),
      createNode("C", Seq("A")),
      createNode("D", Seq("A")),
      createNode("E", Seq("A")),
      createNode("F", Seq("B", "C", "D", "E"))
    )

    val plan = builder.buildExecutionPlan(nodes)

    // 3 groups: [A], [B,C,D,E], [F]
    plan.groups.size shouldBe 3
    plan.groups(0).nodes.map(_.id) should contain("A")
    plan.groups(1).nodes.size shouldBe 4
    plan.groups(1).parallel shouldBe true
    plan.groups(2).nodes.map(_.id) should contain("F")
    plan.rootNode shouldBe "F"
  }

  // --- Gap #3: join parent must be treated as implicit dependency ---

  it should "include join parent in the dependency graph" in {
    val builder = new DAGGraphBuilder(createGlobalConfig())
    // B declares a join on A but has NO explicit dependency on A
    val nodes = Seq(
      createNode("A"),
      createNodeWithJoin("B", joinParent = "A")
    )

    val graph = builder.buildDependencyGraph(nodes)

    graph("B") should contain("A")
  }

  it should "order join-only node after its join parent" in {
    val builder = new DAGGraphBuilder(createGlobalConfig())
    // B joins A but has no explicit dep — B must still execute after A
    val nodes = Seq(
      createNode("A"),
      createNodeWithJoin("B", joinParent = "A")
    )

    val plan = builder.buildExecutionPlan(nodes)

    val allNodeIds = plan.groups.flatMap(_.nodes.map(_.id))
    allNodeIds.indexOf("A") should be < allNodeIds.indexOf("B")
  }

  it should "detect a cycle induced by a join parent dependency" in {
    val builder = new DAGGraphBuilder(createGlobalConfig())
    // A joins B, B has explicit dep on A → cycle: A → B → A
    val nodes = Seq(
      createNodeWithJoin("A", joinParent = "B"),
      createNode("B", Seq("A"))
    )

    intercept[CircularDependencyException] {
      builder.buildExecutionPlan(nodes)
    }
  }

  // --- Gap #6: self-referential join ---

  it should "reject a node whose join parent is itself" in {
    val builder = new DAGGraphBuilder(createGlobalConfig())
    val nodes = Seq(createNodeWithJoin("A", joinParent = "A"))

    intercept[Exception] {
      builder.buildExecutionPlan(nodes)
    }
  }
}
