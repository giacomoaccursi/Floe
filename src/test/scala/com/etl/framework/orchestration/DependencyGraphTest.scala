package com.etl.framework.orchestration

import com.etl.framework.config._
import com.etl.framework.exceptions.CircularDependencyException
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DependencyGraphTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DependencyGraphTest")
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

  private def createFlow(
      name: String,
      foreignKeys: Seq[ForeignKeyConfig] = Seq.empty,
      fkColumns: Seq[ColumnConfig] = Seq.empty
  ): FlowConfig = FlowConfig(
    name = name,
    description = s"Flow $name",
    version = "1.0",
    owner = "test",
    source = SourceConfig(
      SourceType.File,
      s"/data/input/$name",
      FileFormat.CSV,
      Map.empty,
      None
    ),
    schema = SchemaConfig(
      true,
      false,
      ColumnConfig("id", "string", false, "PK") +: fkColumns
    ),
    loadMode = LoadModeConfig(LoadMode.Full),
    validation = ValidationConfig(
      primaryKey = Seq("id"),
      foreignKeys = foreignKeys,
      rules = Seq.empty
    ),
    output = OutputConfig()
  )

  "Dependency graph" should "contain all FK edges" in {
    val flowA = createFlow("flow_a")
    val flowB = createFlow(
      "flow_b",
      foreignKeys = Seq(
        ForeignKeyConfig("fk_a", "a_id", ReferenceConfig("flow_a", "id"))
      ),
      fkColumns = Seq(ColumnConfig("a_id", "string", false, "FK to A"))
    )
    val flowC = createFlow(
      "flow_c",
      foreignKeys = Seq(
        ForeignKeyConfig("fk_a", "a_id", ReferenceConfig("flow_a", "id")),
        ForeignKeyConfig("fk_b", "b_id", ReferenceConfig("flow_b", "id"))
      ),
      fkColumns = Seq(
        ColumnConfig("a_id", "string", false, "FK to A"),
        ColumnConfig("b_id", "string", false, "FK to B")
      )
    )

    val builder =
      new com.etl.framework.orchestration.planning.DependencyGraphBuilder(
        Seq(flowA, flowB, flowC)
      )
    val graph = builder.buildGraph()

    graph should contain key "flow_a"
    graph should contain key "flow_b"
    graph should contain key "flow_c"

    graph("flow_a") shouldBe empty
    graph("flow_b") should contain("flow_a")
    graph("flow_c") should contain allOf ("flow_a", "flow_b")
  }

  it should "produce correct topological sort" in {
    val flowA = createFlow("flow_a")
    val flowB = createFlow(
      "flow_b",
      foreignKeys = Seq(
        ForeignKeyConfig("fk_a", "a_id", ReferenceConfig("flow_a", "id"))
      ),
      fkColumns = Seq(ColumnConfig("a_id", "string", false, "FK to A"))
    )
    val flowC = createFlow(
      "flow_c",
      foreignKeys = Seq(
        ForeignKeyConfig("fk_b", "b_id", ReferenceConfig("flow_b", "id"))
      ),
      fkColumns = Seq(ColumnConfig("b_id", "string", false, "FK to B"))
    )

    val orchestrator =
      FlowOrchestrator(globalConfig, Seq(flowA, flowB, flowC))
    val plan = orchestrator.buildExecutionPlan()

    val executionOrder = plan.groups.flatMap(_.flows.map(_.name))
    val positions = executionOrder.zipWithIndex.toMap

    positions("flow_a") should be < positions("flow_b")
    positions("flow_b") should be < positions("flow_c")
    executionOrder should have size 3
  }

  it should "detect circular dependencies" in {
    val flowA = createFlow(
      "flow_a",
      foreignKeys = Seq(
        ForeignKeyConfig("fk_c", "c_id", ReferenceConfig("flow_c", "id"))
      ),
      fkColumns = Seq(ColumnConfig("c_id", "string", false, "FK to C"))
    )
    val flowB = createFlow(
      "flow_b",
      foreignKeys = Seq(
        ForeignKeyConfig("fk_a", "a_id", ReferenceConfig("flow_a", "id"))
      ),
      fkColumns = Seq(ColumnConfig("a_id", "string", false, "FK to A"))
    )
    val flowC = createFlow(
      "flow_c",
      foreignKeys = Seq(
        ForeignKeyConfig("fk_b", "b_id", ReferenceConfig("flow_b", "id"))
      ),
      fkColumns = Seq(ColumnConfig("b_id", "string", false, "FK to B"))
    )

    val orchestrator =
      FlowOrchestrator(globalConfig, Seq(flowA, flowB, flowC))

    intercept[CircularDependencyException] {
      orchestrator.buildExecutionPlan()
    }
  }

  it should "produce empty plan for empty flow list" in {
    val orchestrator = FlowOrchestrator(globalConfig, Seq.empty)
    val plan = orchestrator.buildExecutionPlan()

    plan.groups shouldBe empty
  }

  it should "produce single group for single flow" in {
    val flow = createFlow("single_flow")

    val orchestrator = FlowOrchestrator(globalConfig, Seq(flow))
    val plan = orchestrator.buildExecutionPlan()

    plan.groups should have size 1
    plan.groups.head.flows should have size 1
    plan.groups.head.flows.head.name shouldBe "single_flow"
  }
}
