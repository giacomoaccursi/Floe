package com.etl.framework.orchestration.planning

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import com.etl.framework.exceptions.CircularDependencyException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DependencyGraphBuilderTest extends AnyFlatSpec with Matchers {

  def createFlowConfig(
      name: String,
      foreignKeys: Seq[ForeignKeyConfig] = Seq.empty
  ): FlowConfig =
    TestFixtures.flowConfig(
      name = name,
      primaryKey = Seq.empty,
      foreignKeys = foreignKeys,
      enforceSchema = true
    )

  def createForeignKey(
      column: String,
      refFlow: String,
      refColumn: String
  ): ForeignKeyConfig = {
    ForeignKeyConfig(Seq(column), ReferenceConfig(refFlow, Seq(refColumn)))
  }

  "DependencyGraphBuilder" should "build empty graph with no dependencies" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig("flow_b")
    val flowC = createFlowConfig("flow_c")

    val builder = new DependencyGraphBuilder(Seq(flowA, flowB, flowC))
    val graph = builder.buildGraph()

    graph.size shouldBe 3
    graph("flow_a") shouldBe empty
    graph("flow_b") shouldBe empty
    graph("flow_c") shouldBe empty
  }

  it should "build graph with simple linear dependencies" in {
    // flow_c depends on flow_b, flow_b depends on flow_a
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig(
      "flow_b",
      foreignKeys = Seq(createForeignKey("col1", "flow_a", "id"))
    )
    val flowC = createFlowConfig(
      "flow_c",
      foreignKeys = Seq(createForeignKey("col2", "flow_b", "id"))
    )

    val builder = new DependencyGraphBuilder(Seq(flowA, flowB, flowC))
    val graph = builder.buildGraph()

    graph("flow_a") shouldBe empty
    graph("flow_b") shouldBe Set("flow_a")
    graph("flow_c") shouldBe Set("flow_b")
  }

  it should "build graph with multiple dependencies" in {
    // flow_d depends on both flow_b and flow_c
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig("flow_b")
    val flowC = createFlowConfig("flow_c")
    val flowD = createFlowConfig(
      "flow_d",
      foreignKeys = Seq(
        createForeignKey("col1", "flow_b", "id"),
        createForeignKey("col2", "flow_c", "id")
      )
    )

    val builder = new DependencyGraphBuilder(Seq(flowA, flowB, flowC, flowD))
    val graph = builder.buildGraph()

    graph("flow_a") shouldBe empty
    graph("flow_b") shouldBe empty
    graph("flow_c") shouldBe empty
    graph("flow_d") shouldBe Set("flow_b", "flow_c")
  }

  it should "perform topological sort with no dependencies" in {
    val graph = Map(
      "flow_a" -> Set.empty[String],
      "flow_b" -> Set.empty[String],
      "flow_c" -> Set.empty[String]
    )

    val builder = new DependencyGraphBuilder(Seq.empty)
    val sorted = builder.topologicalSort(graph)

    sorted should contain allOf ("flow_a", "flow_b", "flow_c")
    sorted.size shouldBe 3
  }

  it should "perform topological sort with linear dependencies" in {
    // flow_a -> flow_b -> flow_c (flow_b depends on flow_a, flow_c depends on flow_b)
    val graph = Map(
      "flow_a" -> Set.empty[String],
      "flow_b" -> Set("flow_a"),
      "flow_c" -> Set("flow_b")
    )

    val builder = new DependencyGraphBuilder(Seq.empty)
    val sorted = builder.topologicalSort(graph)

    sorted shouldBe Seq("flow_a", "flow_b", "flow_c")
  }

  it should "perform topological sort with diamond dependencies" in {
    // flow_a is independent, flow_b and flow_c depend on flow_a, flow_d depends on both flow_b and flow_c
    val graph = Map(
      "flow_a" -> Set.empty[String],
      "flow_b" -> Set("flow_a"),
      "flow_c" -> Set("flow_a"),
      "flow_d" -> Set("flow_b", "flow_c")
    )

    val builder = new DependencyGraphBuilder(Seq.empty)
    val sorted = builder.topologicalSort(graph)

    sorted.size shouldBe 4
    sorted.indexOf("flow_a") should be < sorted.indexOf("flow_b")
    sorted.indexOf("flow_a") should be < sorted.indexOf("flow_c")
    sorted.indexOf("flow_b") should be < sorted.indexOf("flow_d")
    sorted.indexOf("flow_c") should be < sorted.indexOf("flow_d")
  }

  it should "detect circular dependency and throw exception" in {
    // flow_a -> flow_b -> flow_c -> flow_a (circular)
    val graph = Map(
      "flow_a" -> Set("flow_c"),
      "flow_b" -> Set("flow_a"),
      "flow_c" -> Set("flow_b")
    )

    val builder = new DependencyGraphBuilder(Seq.empty)

    val exception = intercept[CircularDependencyException] {
      builder.topologicalSort(graph)
    }

    exception.errorCode shouldBe "CONFIG_CIRCULAR_DEPENDENCY"
    exception.cycle should not be empty
    exception.getMessage should include("Circular dependency detected")
  }

  it should "detect self-referencing circular dependency" in {
    // flow_a depends on itself
    val graph = Map(
      "flow_a" -> Set("flow_a")
    )

    val builder = new DependencyGraphBuilder(Seq.empty)

    val exception = intercept[CircularDependencyException] {
      builder.topologicalSort(graph)
    }

    exception.cycle should contain("flow_a")
  }

  it should "group flows for parallel execution with no dependencies" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig("flow_b")
    val flowC = createFlowConfig("flow_c")

    val flows = Seq(flowA, flowB, flowC)
    val builder = new DependencyGraphBuilder(flows)
    val graph = builder.buildGraph()
    val sorted = builder.topologicalSort(graph)

    val groups =
      builder.groupForParallelExecution(sorted, graph, parallelEnabled = true)

    groups.size shouldBe 1
    groups.head.flows.size shouldBe 3
    groups.head.parallel shouldBe true
  }

  it should "group flows for parallel execution with linear dependencies" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig(
      "flow_b",
      foreignKeys = Seq(createForeignKey("col1", "flow_a", "id"))
    )
    val flowC = createFlowConfig(
      "flow_c",
      foreignKeys = Seq(createForeignKey("col2", "flow_b", "id"))
    )

    val flows = Seq(flowA, flowB, flowC)
    val builder = new DependencyGraphBuilder(flows)
    val graph = builder.buildGraph()
    val sorted = builder.topologicalSort(graph)

    val groups =
      builder.groupForParallelExecution(sorted, graph, parallelEnabled = true)

    // Should create 3 groups (one for each flow, executed sequentially)
    groups.size shouldBe 3
    groups.head.flows.map(_.name) should contain("flow_a")
    groups(1).flows.map(_.name) should contain("flow_b")
    groups(2).flows.map(_.name) should contain("flow_c")
  }

  it should "group flows for parallel execution with diamond dependencies" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig(
      "flow_b",
      foreignKeys = Seq(createForeignKey("col1", "flow_a", "id"))
    )
    val flowC = createFlowConfig(
      "flow_c",
      foreignKeys = Seq(createForeignKey("col2", "flow_a", "id"))
    )
    val flowD = createFlowConfig(
      "flow_d",
      foreignKeys = Seq(
        createForeignKey("col3", "flow_b", "id"),
        createForeignKey("col4", "flow_c", "id")
      )
    )

    val flows = Seq(flowA, flowB, flowC, flowD)
    val builder = new DependencyGraphBuilder(flows)
    val graph = builder.buildGraph()
    val sorted = builder.topologicalSort(graph)

    val groups =
      builder.groupForParallelExecution(sorted, graph, parallelEnabled = true)

    // Should create 3 groups: [flow_a], [flow_b, flow_c], [flow_d]
    groups.size shouldBe 3

    // First group: flow_a
    groups(0).flows.map(_.name) should contain("flow_a")
    groups(0).parallel shouldBe false // only one flow

    // Second group: flow_b and flow_c can run in parallel
    groups(1).flows.size shouldBe 2
    groups(1).flows.map(_.name) should contain allOf ("flow_b", "flow_c")
    groups(1).parallel shouldBe true // multiple flows

    // Third group: flow_d
    groups(2).flows.map(_.name) should contain("flow_d")
    groups(2).parallel shouldBe false // only one flow
  }

  it should "disable parallel execution when parallelEnabled is false" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig("flow_b")
    val flowC = createFlowConfig("flow_c")

    val flows = Seq(flowA, flowB, flowC)
    val builder = new DependencyGraphBuilder(flows)
    val graph = builder.buildGraph()
    val sorted = builder.topologicalSort(graph)

    val groups =
      builder.groupForParallelExecution(sorted, graph, parallelEnabled = false)

    groups.size shouldBe 1
    groups.head.flows.size shouldBe 3
    groups.head.parallel shouldBe false // disabled by flag
  }

  it should "handle complex mixed dependencies" in {
    // Complex scenario:
    // flow_a, flow_b are independent
    // flow_c depends on flow_a
    // flow_d depends on flow_b
    // flow_e depends on both flow_c and flow_d
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig("flow_b")
    val flowC = createFlowConfig(
      "flow_c",
      foreignKeys = Seq(createForeignKey("col1", "flow_a", "id"))
    )
    val flowD = createFlowConfig(
      "flow_d",
      foreignKeys = Seq(createForeignKey("col2", "flow_b", "id"))
    )
    val flowE = createFlowConfig(
      "flow_e",
      foreignKeys = Seq(
        createForeignKey("col3", "flow_c", "id"),
        createForeignKey("col4", "flow_d", "id")
      )
    )

    val flows = Seq(flowA, flowB, flowC, flowD, flowE)
    val builder = new DependencyGraphBuilder(flows)
    val graph = builder.buildGraph()
    val sorted = builder.topologicalSort(graph)

    val groups =
      builder.groupForParallelExecution(sorted, graph, parallelEnabled = true)

    // Expected groups:
    // Group 1: [flow_a, flow_b] - parallel
    // Group 2: [flow_c, flow_d] - parallel
    // Group 3: [flow_e] - single
    groups.size shouldBe 3

    // First group should contain flow_a and flow_b
    groups(0).flows.size shouldBe 2
    groups(0).flows.map(_.name) should contain allOf ("flow_a", "flow_b")
    groups(0).parallel shouldBe true

    // Second group should contain flow_c and flow_d
    groups(1).flows.size shouldBe 2
    groups(1).flows.map(_.name) should contain allOf ("flow_c", "flow_d")
    groups(1).parallel shouldBe true

    // Third group should contain flow_e
    groups(2).flows.map(_.name) should contain("flow_e")
    groups(2).parallel shouldBe false
  }

  "dependsOn" should "add explicit dependency edges to the graph" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig("flow_b").copy(dependsOn = Seq("flow_a"))

    val builder = new DependencyGraphBuilder(Seq(flowA, flowB))
    val graph = builder.buildGraph()

    graph("flow_a") shouldBe empty
    graph("flow_b") shouldBe Set("flow_a")
  }

  it should "combine with FK dependencies" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig("flow_b")
    val flowC = createFlowConfig(
      "flow_c",
      foreignKeys = Seq(createForeignKey("col1", "flow_a", "id"))
    ).copy(dependsOn = Seq("flow_b"))

    val builder = new DependencyGraphBuilder(Seq(flowA, flowB, flowC))
    val graph = builder.buildGraph()

    graph("flow_c") shouldBe Set("flow_a", "flow_b")
  }

  it should "enforce execution order in topological sort" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig("flow_b")
    val flowC = createFlowConfig("flow_c").copy(dependsOn = Seq("flow_a", "flow_b"))

    val builder = new DependencyGraphBuilder(Seq(flowA, flowB, flowC))
    val graph = builder.buildGraph()
    val sorted = builder.topologicalSort(graph)

    sorted.indexOf("flow_a") should be < sorted.indexOf("flow_c")
    sorted.indexOf("flow_b") should be < sorted.indexOf("flow_c")
  }

  it should "prevent parallel execution of dependent flows" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig("flow_b").copy(dependsOn = Seq("flow_a"))

    val builder = new DependencyGraphBuilder(Seq(flowA, flowB))
    val graph = builder.buildGraph()
    val sorted = builder.topologicalSort(graph)
    val groups = builder.groupForParallelExecution(sorted, graph, parallelEnabled = true)

    groups.size shouldBe 2
    groups(0).flows.map(_.name) should contain("flow_a")
    groups(1).flows.map(_.name) should contain("flow_b")
  }

  it should "detect circular dependency via dependsOn" in {
    val flowA = createFlowConfig("flow_a").copy(dependsOn = Seq("flow_b"))
    val flowB = createFlowConfig("flow_b").copy(dependsOn = Seq("flow_a"))

    val builder = new DependencyGraphBuilder(Seq(flowA, flowB))
    val graph = builder.buildGraph()

    intercept[CircularDependencyException] {
      builder.topologicalSort(graph)
    }
  }
}
