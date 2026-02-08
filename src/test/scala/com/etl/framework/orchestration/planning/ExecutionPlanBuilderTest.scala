package com.etl.framework.orchestration.planning

import com.etl.framework.config._
import com.etl.framework.exceptions.CircularDependencyException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExecutionPlanBuilderTest extends AnyFlatSpec with Matchers {

  def createGlobalConfig(parallelFlows: Boolean = true): GlobalConfig = {
    GlobalConfig(
      paths = PathsConfig("/full", "/delta", "/input", "/rejected", "/metadata"),
      processing = ProcessingConfig(
        "yyyyMMdd",
        failOnValidationError = false,
        maxRejectionRate = 0.1
      ),
      performance = PerformanceConfig(parallelFlows, parallelNodes = true)
    )
  }

  def createFlowConfig(
      name: String,
      foreignKeys: Seq[ForeignKeyConfig] = Seq.empty
  ): FlowConfig = {
    FlowConfig(
      name = name,
      description = "Test flow",
      version = "1.0",
      owner = "test",
      source =
        SourceConfig(SourceType.File, "/path", FileFormat.CSV, Map.empty, None),
      schema = SchemaConfig(true, true, Seq.empty),
      loadMode = LoadModeConfig(LoadMode.Full),
      validation = ValidationConfig(Seq.empty, foreignKeys, Seq.empty),
      output = OutputConfig()
    )
  }

  def createForeignKey(
      name: String,
      column: String,
      refFlow: String,
      refColumn: String
  ): ForeignKeyConfig = {
    ForeignKeyConfig(name, column, ReferenceConfig(refFlow, refColumn))
  }

  "ExecutionPlanBuilder" should "build plan with no dependencies" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig("flow_b")
    val flowC = createFlowConfig("flow_c")

    val flows = Seq(flowA, flowB, flowC)
    val globalConfig = createGlobalConfig(parallelFlows = true)
    val builder = new ExecutionPlanBuilder(flows, globalConfig)

    val plan = builder.build()

    // All flows should be in a single group since they have no dependencies
    plan.groups.size shouldBe 1
    plan.groups.head.flows.size shouldBe 3
    plan.groups.head.parallel shouldBe true
  }

  it should "build plan with linear dependencies" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig(
      "flow_b",
      foreignKeys = Seq(createForeignKey("fk1", "col1", "flow_a", "id"))
    )
    val flowC = createFlowConfig(
      "flow_c",
      foreignKeys = Seq(createForeignKey("fk2", "col2", "flow_b", "id"))
    )

    val flows = Seq(flowA, flowB, flowC)
    val globalConfig = createGlobalConfig(parallelFlows = true)
    val builder = new ExecutionPlanBuilder(flows, globalConfig)

    val plan = builder.build()

    // Should create 3 groups (one for each flow, executed sequentially)
    plan.groups.size shouldBe 3
    plan.groups(0).flows.map(_.name) should contain("flow_a")
    plan.groups(1).flows.map(_.name) should contain("flow_b")
    plan.groups(2).flows.map(_.name) should contain("flow_c")
  }

  it should "build plan with diamond dependencies" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig(
      "flow_b",
      foreignKeys = Seq(createForeignKey("fk1", "col1", "flow_a", "id"))
    )
    val flowC = createFlowConfig(
      "flow_c",
      foreignKeys = Seq(createForeignKey("fk2", "col2", "flow_a", "id"))
    )
    val flowD = createFlowConfig(
      "flow_d",
      foreignKeys = Seq(
        createForeignKey("fk3", "col3", "flow_b", "id"),
        createForeignKey("fk4", "col4", "flow_c", "id")
      )
    )

    val flows = Seq(flowA, flowB, flowC, flowD)
    val globalConfig = createGlobalConfig(parallelFlows = true)
    val builder = new ExecutionPlanBuilder(flows, globalConfig)

    val plan = builder.build()

    // Should create 3 groups: [flow_a], [flow_b, flow_c], [flow_d]
    plan.groups.size shouldBe 3

    // First group: flow_a
    plan.groups(0).flows.map(_.name) should contain("flow_a")

    // Second group: flow_b and flow_c (can run in parallel)
    plan.groups(1).flows.size shouldBe 2
    plan.groups(1).flows.map(_.name) should contain allOf ("flow_b", "flow_c")
    plan.groups(1).parallel shouldBe true

    // Third group: flow_d
    plan.groups(2).flows.map(_.name) should contain("flow_d")
  }

  it should "respect parallelFlows configuration when enabled" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig("flow_b")
    val flowC = createFlowConfig("flow_c")

    val flows = Seq(flowA, flowB, flowC)
    val globalConfig = createGlobalConfig(parallelFlows = true)
    val builder = new ExecutionPlanBuilder(flows, globalConfig)

    val plan = builder.build()

    plan.groups.size shouldBe 1
    plan.groups.head.parallel shouldBe true
  }

  it should "respect parallelFlows configuration when disabled" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig("flow_b")
    val flowC = createFlowConfig("flow_c")

    val flows = Seq(flowA, flowB, flowC)
    val globalConfig = createGlobalConfig(parallelFlows = false)
    val builder = new ExecutionPlanBuilder(flows, globalConfig)

    val plan = builder.build()

    plan.groups.size shouldBe 1
    plan.groups.head.parallel shouldBe false
  }

  it should "build plan with complex mixed dependencies" in {
    // Complex scenario:
    // flow_a, flow_b are independent
    // flow_c depends on flow_a
    // flow_d depends on flow_b
    // flow_e depends on both flow_c and flow_d
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig("flow_b")
    val flowC = createFlowConfig(
      "flow_c",
      foreignKeys = Seq(createForeignKey("fk1", "col1", "flow_a", "id"))
    )
    val flowD = createFlowConfig(
      "flow_d",
      foreignKeys = Seq(createForeignKey("fk2", "col2", "flow_b", "id"))
    )
    val flowE = createFlowConfig(
      "flow_e",
      foreignKeys = Seq(
        createForeignKey("fk3", "col3", "flow_c", "id"),
        createForeignKey("fk4", "col4", "flow_d", "id")
      )
    )

    val flows = Seq(flowA, flowB, flowC, flowD, flowE)
    val globalConfig = createGlobalConfig(parallelFlows = true)
    val builder = new ExecutionPlanBuilder(flows, globalConfig)

    val plan = builder.build()

    // Expected groups:
    // Group 1: [flow_a, flow_b] - parallel
    // Group 2: [flow_c, flow_d] - parallel
    // Group 3: [flow_e] - single
    plan.groups.size shouldBe 3

    // First group should contain flow_a and flow_b
    plan.groups(0).flows.size shouldBe 2
    plan.groups(0).flows.map(_.name) should contain allOf ("flow_a", "flow_b")
    plan.groups(0).parallel shouldBe true

    // Second group should contain flow_c and flow_d
    plan.groups(1).flows.size shouldBe 2
    plan.groups(1).flows.map(_.name) should contain allOf ("flow_c", "flow_d")
    plan.groups(1).parallel shouldBe true

    // Third group should contain flow_e
    plan.groups(2).flows.map(_.name) should contain("flow_e")
    plan.groups(2).parallel shouldBe false
  }

  it should "propagate CircularDependencyException from DependencyGraphBuilder" in {
    // Create circular dependency: flow_a -> flow_b -> flow_c -> flow_a
    val flowA = createFlowConfig(
      "flow_a",
      foreignKeys = Seq(createForeignKey("fk1", "col1", "flow_c", "id"))
    )
    val flowB = createFlowConfig(
      "flow_b",
      foreignKeys = Seq(createForeignKey("fk2", "col2", "flow_a", "id"))
    )
    val flowC = createFlowConfig(
      "flow_c",
      foreignKeys = Seq(createForeignKey("fk3", "col3", "flow_b", "id"))
    )

    val flows = Seq(flowA, flowB, flowC)
    val globalConfig = createGlobalConfig()
    val builder = new ExecutionPlanBuilder(flows, globalConfig)

    val exception = intercept[CircularDependencyException] {
      builder.build()
    }

    exception.errorCode shouldBe "CONFIG_CIRCULAR_DEPENDENCY"
    exception.cycle should not be empty
  }

  it should "handle single flow with no dependencies" in {
    val flowA = createFlowConfig("flow_a")

    val flows = Seq(flowA)
    val globalConfig = createGlobalConfig(parallelFlows = true)
    val builder = new ExecutionPlanBuilder(flows, globalConfig)

    val plan = builder.build()

    plan.groups.size shouldBe 1
    plan.groups.head.flows.size shouldBe 1
    plan.groups.head.flows.head.name shouldBe "flow_a"
    plan.groups.head.parallel shouldBe false // single flow, no parallelism needed
  }

  it should "create execution plan with correct number of flows" in {
    val flowA = createFlowConfig("flow_a")
    val flowB = createFlowConfig(
      "flow_b",
      foreignKeys = Seq(createForeignKey("fk1", "col1", "flow_a", "id"))
    )
    val flowC = createFlowConfig(
      "flow_c",
      foreignKeys = Seq(createForeignKey("fk2", "col2", "flow_a", "id"))
    )

    val flows = Seq(flowA, flowB, flowC)
    val globalConfig = createGlobalConfig(parallelFlows = true)
    val builder = new ExecutionPlanBuilder(flows, globalConfig)

    val plan = builder.build()

    // Count total flows across all groups
    val totalFlows = plan.groups.flatMap(_.flows).size
    totalFlows shouldBe 3

    // Verify all flows are present
    val flowNames = plan.groups.flatMap(_.flows.map(_.name))
    flowNames should contain allOf ("flow_a", "flow_b", "flow_c")
  }

  it should "use factory method from companion object" in {
    val flowA = createFlowConfig("flow_a")
    val flows = Seq(flowA)
    val globalConfig = createGlobalConfig()

    val builder = ExecutionPlanBuilder(flows, globalConfig)
    val plan = builder.build()

    plan.groups should not be empty
  }
}
