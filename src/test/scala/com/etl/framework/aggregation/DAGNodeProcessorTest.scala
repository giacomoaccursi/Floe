package com.etl.framework.aggregation

import com.etl.framework.config._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class DAGNodeProcessorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DAGNodeProcessorTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  import spark.implicits._

  private val joinExecutor = new JoinStrategyExecutor()
  private val processor = new DAGNodeProcessor(joinExecutor)

  private var parquetDir: java.nio.file.Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    parquetDir = Files.createTempDirectory("dag_node_test")
    val df = Seq((1, "Alice", 100), (2, "Bob", 200)).toDF("id", "name", "amount")
    df.write.parquet(parquetDir.resolve("customers").toString)

    // Register a temp view to simulate a catalog table
    df.createOrReplaceTempView("test_customers_table")
  }

  override def afterAll(): Unit = {
    spark.sql("DROP VIEW IF EXISTS test_customers_table")
    // clean up temp dir
    import scala.reflect.io.Directory
    new Directory(parquetDir.toFile).deleteRecursively()
    super.afterAll()
  }

  "DAGNodeProcessor" should "load source data from parquet path" in {
    val node = DAGNode(
      id = "customers",
      description = "Customers from parquet",
      sourceFlow = "customers",
      sourcePath = parquetDir.resolve("customers").toString,
      dependencies = Seq.empty
    )

    val result = processor.executeNode(node, Map.empty)
    result.count() shouldBe 2
    result.columns should contain allOf("id", "name", "amount")
  }

  it should "load source data from sourceTable when provided" in {
    val node = DAGNode(
      id = "customers",
      description = "Customers from table",
      sourceFlow = "customers",
      sourcePath = "",
      dependencies = Seq.empty,
      sourceTable = Some("test_customers_table")
    )

    val result = processor.executeNode(node, Map.empty)
    result.count() shouldBe 2
    result.columns should contain allOf("id", "name", "amount")
  }

  it should "apply filters correctly" in {
    val node = DAGNode(
      id = "customers",
      description = "Filtered customers",
      sourceFlow = "customers",
      sourcePath = parquetDir.resolve("customers").toString,
      dependencies = Seq.empty,
      filters = Seq("amount > 150")
    )

    val result = processor.executeNode(node, Map.empty)
    result.count() shouldBe 1
    result.first().getAs[String]("name") shouldBe "Bob"
  }

  it should "apply column selection correctly" in {
    val node = DAGNode(
      id = "customers",
      description = "Selected columns",
      sourceFlow = "customers",
      sourcePath = parquetDir.resolve("customers").toString,
      dependencies = Seq.empty,
      select = Seq("id", "name")
    )

    val result = processor.executeNode(node, Map.empty)
    result.columns should contain allOf("id", "name")
    result.columns should not contain "amount"
  }

  it should "prefer sourceTable over sourcePath when both are provided" in {
    // sourceTable points to a valid table, sourcePath points to a nonexistent path
    val node = DAGNode(
      id = "customers",
      description = "Table takes priority",
      sourceFlow = "customers",
      sourcePath = "/nonexistent/path",
      dependencies = Seq.empty,
      sourceTable = Some("test_customers_table")
    )

    val result = processor.executeNode(node, Map.empty)
    result.count() shouldBe 2
  }

  it should "throw an exception when join parent is not found in node results" in {
    val node = DAGNode(
      id = "child_node",
      description = "Node requiring missing parent",
      sourceFlow = "child",
      sourcePath = parquetDir.resolve("customers").toString,
      dependencies = Seq("missing_parent"),
      join = Some(JoinConfig(
        `type` = JoinType.LeftOuter,
        parent = "missing_parent",
        conditions = Seq(JoinCondition("id", "id")),
        strategy = JoinStrategy.Flatten
      ))
    )

    an[IllegalStateException] should be thrownBy {
      processor.executeNode(node, Map.empty)
    }
  }
}
