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
      .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  import spark.implicits._

  private val joinExecutor = new JoinStrategyExecutor()
  private val processor = new DAGNodeProcessor(joinExecutor, "test_catalog")

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

  "DAGNodeProcessor" should "load source data from Iceberg table" in {
    val node = DAGNode(
      id = "customers",
      sourceFlow = "customers",
      sourceTable = Some("test_customers_table")
    )

    val result = processor.executeNode(node, Map.empty)
    result.count() shouldBe 2
    result.columns should contain allOf ("id", "name", "amount")
  }

  it should "load source data from sourceTable when provided" in {
    val node = DAGNode(
      id = "customers",
      description = "Customers from table",
      sourceFlow = "customers",
      sourceTable = Some("test_customers_table")
    )

    val result = processor.executeNode(node, Map.empty)
    result.count() shouldBe 2
    result.columns should contain allOf ("id", "name", "amount")
  }

  it should "apply filters correctly" in {
    val node = DAGNode(
      id = "customers",
      description = "Filtered customers",
      sourceFlow = "customers",
      sourceTable = Some("test_customers_table"),
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
      sourceTable = Some("test_customers_table"),
      select = Seq("id", "name")
    )

    val result = processor.executeNode(node, Map.empty)
    result.columns should contain allOf ("id", "name")
    result.columns should not contain "amount"
  }

  it should "use sourceTable when explicitly provided" in {
    val node = DAGNode(
      id = "customers",
      description = "Explicit table name",
      sourceFlow = "customers",
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
      sourceTable = Some("test_customers_table"),
      joins = Seq(
        JoinConfig(
          `type` = JoinType.LeftOuter,
          `with` = "missing_parent",
          conditions = Seq(JoinCondition("id", "id")),
          strategy = JoinStrategy.Flatten
        )
      )
    )

    an[IllegalStateException] should be thrownBy {
      processor.executeNode(node, Map.empty)
    }
  }
}
