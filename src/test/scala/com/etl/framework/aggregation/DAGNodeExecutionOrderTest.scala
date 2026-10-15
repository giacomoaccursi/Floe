package com.etl.framework.aggregation

import com.etl.framework.config._
import com.etl.framework.exceptions.{CircularDependencyException, ValidationConfigException}
import com.etl.framework.util.SqlIdentifier
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DAGNodeExecutionOrderTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DAGNodeExecutionOrderTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  private val globalConfig = GlobalConfig(
    paths = PathsConfig("/data/output", "/data/rejected", "/data/metadata"),
    performance = PerformanceConfig(false),
    iceberg = IcebergConfig(warehouse = "/tmp/test-warehouse")
  )

  private def leaf(id: String): DAGNode =
    DAGNode(id = id, sourceFlow = s"flow_$id")

  private def child(id: String, withNode: String): DAGNode =
    DAGNode(
      id = id,
      sourceFlow = s"flow_$id",
      joins = Seq(
        JoinConfig(
          `type` = JoinType.LeftOuter,
          `with` = withNode,
          conditions = Seq(JoinCondition("id", "id")),
          strategy = JoinStrategy.Nest,
          orderBy = Seq("id")
        )
      )
    )

  private def createDagConfig(nodes: Seq[DAGNode]): AggregationConfig =
    AggregationConfig(name = "test_aggregation", nodes = nodes)

  "DAG execution order" should "respect join-based dependencies" in {
    val dagConfig = createDagConfig(Seq(leaf("node_a"), child("node_b", "node_a"), child("node_c", "node_b")))
    val plan = new DAGOrchestrator(dagConfig, globalConfig).buildExecutionPlan()

    val order = plan.groups.flatMap(_.nodes.map(_.id))
    order.indexOf("node_a") should be < order.indexOf("node_b")
    order.indexOf("node_b") should be < order.indexOf("node_c")
    order should have size 3
  }

  it should "detect circular dependencies via join parents" in {
    val dagConfig = createDagConfig(Seq(child("node_a", "node_b"), child("node_b", "node_a")))
    intercept[CircularDependencyException] {
      new DAGOrchestrator(dagConfig, globalConfig).buildExecutionPlan()
    }
  }

  it should "handle empty node list" in {
    val dagConfig = createDagConfig(Seq.empty)
    intercept[ValidationConfigException] {
      new DAGOrchestrator(dagConfig, globalConfig).buildExecutionPlan()
    }
  }

  it should "produce single group for single node" in {
    val dagConfig = createDagConfig(Seq(leaf("single_node")))
    val plan = new DAGOrchestrator(dagConfig, globalConfig).buildExecutionPlan()

    plan.groups should have size 1
    plan.groups.head.nodes.head.id shouldBe "single_node"
  }

  it should "reject aggregate joins without aggregation expressions during planning" in {
    val aggregateRoot = DAGNode(
      id = "root",
      sourceFlow = "flow_root",
      joins = Seq(
        JoinConfig(
          `type` = JoinType.LeftOuter,
          `with` = "child",
          conditions = Seq(JoinCondition("id", "id")),
          strategy = JoinStrategy.Aggregate
        )
      )
    )
    val dagConfig = createDagConfig(Seq(leaf("child"), aggregateRoot))

    intercept[ValidationConfigException] {
      new DAGOrchestrator(dagConfig, globalConfig).buildExecutionPlan()
    }.getMessage should include("requires at least one aggregation")
  }

  it should "build a lazy root DataFrame that the caller can materialize" in {
    import spark.implicits._
    Seq((1, "Alice")).toDF("id", "name").createOrReplaceTempView("dag_parent")
    Seq((1, "Rome")).toDF("parent_id", "city").createOrReplaceTempView("dag_child")
    try {
      val childNode = DAGNode(id = "child", sourceTable = Some("dag_child"))
      val rootNode = DAGNode(
        id = "root",
        sourceTable = Some("dag_parent"),
        joins = Seq(
          JoinConfig(
            `type` = JoinType.Inner,
            `with` = "child",
            conditions = Seq(JoinCondition("id", "parent_id")),
            strategy = JoinStrategy.Flatten
          )
        )
      )

      val orchestrator = new DAGOrchestrator(createDagConfig(Seq(childNode, rootNode)), globalConfig)
      val processor = new DAGNodeProcessor(
        new JoinStrategyExecutor(),
        globalConfig.iceberg.catalogName,
        globalConfig.iceberg.namespace,
        Some(tableName => spark.table(SqlIdentifier.quoteMultipart(tableName)))
      )
      val result = new DAGExecutor(processor).execute(orchestrator.buildExecutionPlan())

      result.select("id", "name", "city").as[(Int, String, String)].head() shouldBe ((1, "Alice", "Rome"))
    } finally {
      Seq("dag_parent", "dag_child").foreach { viewName =>
        val _ = spark.catalog.dropTempView(viewName)
      }
    }
  }
}
