package com.etl.framework.pipeline

import com.etl.framework.config.IcebergConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class DerivedTableExecutorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DerivedTableExecutorTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
    .config("spark.sql.catalog.spark_catalog.warehouse", {
      Files.createTempDirectory("derived_table_test_warehouse").toString
    })
    .getOrCreate()

  import spark.implicits._

  private var tempWarehouse: String = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempWarehouse = Files.createTempDirectory("derived_exec_test").toString
  }

  private def icebergConfig: IcebergConfig =
    IcebergConfig(warehouse = tempWarehouse)

  private def seedIcebergTable(tableName: String, df: DataFrame): Unit = {
    val fullName = s"spark_catalog.default.$tableName"
    val cols = df.schema.fields.map(f => s"${f.name} ${f.dataType.sql}").mkString(", ")
    try { spark.sql(s"DROP TABLE IF EXISTS $fullName") } catch { case _: Exception => }
    spark.sql(s"CREATE TABLE $fullName ($cols) USING iceberg")
    df.writeTo(fullName).append()
  }

  private def dropTable(name: String): Unit =
    try { spark.sql(s"DROP TABLE IF EXISTS spark_catalog.default.$name") } catch { case _: Exception => }

  override def afterEach(): Unit = {
    Seq("orders", "order_summary", "orders_domestic", "orders_intl", "empty_derived", "failing_derived")
      .foreach(dropTable)
    super.afterEach()
  }

  "DerivedTableExecutor" should "write a derived table to Iceberg from a source table" in {
    val orders = Seq(
      (1, "electronics", 100.0),
      (2, "electronics", 200.0),
      (3, "books", 50.0)
    ).toDF("id", "category", "amount")
    seedIcebergTable("orders", orders)

    val executor = new DerivedTableExecutor(icebergConfig)
    val derivedTables: Seq[(String, DerivedTableContext => DataFrame)] = Seq(
      "order_summary" -> { ctx: DerivedTableContext =>
        ctx.table("orders")
          .groupBy("category")
          .agg(sum("amount").as("total"))
      }
    )

    val results = executor.execute(derivedTables, "batch_001")

    results should have size 1
    results.head.success shouldBe true
    results.head.tableName shouldBe "order_summary"
    results.head.recordsWritten shouldBe 2L

    val written = spark.table("spark_catalog.default.order_summary")
    written.count() shouldBe 2L
    written.filter(col("category") === "electronics").select("total").first().getDouble(0) shouldBe 300.0
  }

  it should "support multiple derived tables in a single execution" in {
    val orders = Seq(
      (1, "IT", 100.0),
      (2, "US", 200.0),
      (3, "IT", 50.0)
    ).toDF("id", "country", "amount")
    seedIcebergTable("orders", orders)

    val executor = new DerivedTableExecutor(icebergConfig)
    val derivedTables: Seq[(String, DerivedTableContext => DataFrame)] = Seq(
      "orders_domestic" -> { ctx: DerivedTableContext =>
        ctx.table("orders").filter(col("country") === "IT")
      },
      "orders_intl" -> { ctx: DerivedTableContext =>
        ctx.table("orders").filter(col("country") =!= "IT")
      }
    )

    val results = executor.execute(derivedTables, "batch_002")

    results should have size 2
    results.foreach(_.success shouldBe true)

    spark.table("spark_catalog.default.orders_domestic").count() shouldBe 2L
    spark.table("spark_catalog.default.orders_intl").count() shouldBe 1L
  }

  it should "overwrite existing data on re-execution (full load)" in {
    val orders1 = Seq((1, "A", 10.0)).toDF("id", "category", "amount")
    seedIcebergTable("orders", orders1)

    val executor = new DerivedTableExecutor(icebergConfig)
    val derivedTables: Seq[(String, DerivedTableContext => DataFrame)] = Seq(
      "order_summary" -> { ctx: DerivedTableContext =>
        ctx.table("orders").groupBy("category").agg(sum("amount").as("total"))
      }
    )

    executor.execute(derivedTables, "batch_001")
    spark.table("spark_catalog.default.order_summary").count() shouldBe 1L

    // Seed new data and re-execute
    dropTable("orders")
    val orders2 = Seq((1, "A", 10.0), (2, "B", 20.0)).toDF("id", "category", "amount")
    seedIcebergTable("orders", orders2)

    executor.execute(derivedTables, "batch_002")
    spark.table("spark_catalog.default.order_summary").count() shouldBe 2L
  }

  it should "handle empty source table" in {
    val emptyOrders = spark.createDataFrame(
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      org.apache.spark.sql.types.StructType(Seq(
        org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType),
        org.apache.spark.sql.types.StructField("category", org.apache.spark.sql.types.StringType)
      ))
    )
    seedIcebergTable("orders", emptyOrders)

    val executor = new DerivedTableExecutor(icebergConfig)
    val derivedTables: Seq[(String, DerivedTableContext => DataFrame)] = Seq(
      "empty_derived" -> { ctx: DerivedTableContext => ctx.table("orders") }
    )

    val results = executor.execute(derivedTables, "batch_empty")

    results.head.success shouldBe true
    results.head.recordsWritten shouldBe 0L
  }

  it should "report failure without stopping other derived tables" in {
    val orders = Seq((1, "A", 10.0)).toDF("id", "category", "amount")
    seedIcebergTable("orders", orders)

    val executor = new DerivedTableExecutor(icebergConfig)
    val derivedTables: Seq[(String, DerivedTableContext => DataFrame)] = Seq(
      "failing_derived" -> { _: DerivedTableContext =>
        throw new RuntimeException("intentional failure")
      },
      "order_summary" -> { ctx: DerivedTableContext =>
        ctx.table("orders").groupBy("category").agg(sum("amount").as("total"))
      }
    )

    val results = executor.execute(derivedTables, "batch_fail")

    results should have size 2
    results.head.success shouldBe false
    results.head.error shouldBe Some("intentional failure")
    results(1).success shouldBe true
    results(1).recordsWritten shouldBe 1L
  }

  it should "provide batchId in context" in {
    val orders = Seq((1, "A")).toDF("id", "category")
    seedIcebergTable("orders", orders)

    val executor = new DerivedTableExecutor(icebergConfig)
    var capturedBatchId: String = ""

    val derivedTables: Seq[(String, DerivedTableContext => DataFrame)] = Seq(
      "order_summary" -> { ctx: DerivedTableContext =>
        capturedBatchId = ctx.batchId
        ctx.table("orders")
      }
    )

    executor.execute(derivedTables, "batch_ctx_test")
    capturedBatchId shouldBe "batch_ctx_test"
  }

  it should "handle schema evolution by adding new columns" in {
    val orders = Seq((1, "A", 10.0)).toDF("id", "category", "amount")
    seedIcebergTable("orders", orders)

    val executor = new DerivedTableExecutor(icebergConfig)

    // First execution: 2 columns
    val v1: Seq[(String, DerivedTableContext => DataFrame)] = Seq(
      "order_summary" -> { ctx: DerivedTableContext =>
        ctx.table("orders").groupBy("category").agg(sum("amount").as("total"))
      }
    )
    executor.execute(v1, "batch_v1")

    // Second execution: 3 columns (added count)
    val v2: Seq[(String, DerivedTableContext => DataFrame)] = Seq(
      "order_summary" -> { ctx: DerivedTableContext =>
        ctx.table("orders").groupBy("category").agg(
          sum("amount").as("total"),
          count("*").as("cnt")
        )
      }
    )
    val results = executor.execute(v2, "batch_v2")

    results.head.success shouldBe true
    val written = spark.table("spark_catalog.default.order_summary")
    written.columns should contain("cnt")
  }

  it should "tag snapshots with batch ID after write" in {
    val orders = Seq((1, "A", 10.0)).toDF("id", "category", "amount")
    seedIcebergTable("orders", orders)

    val executor = new DerivedTableExecutor(icebergConfig)
    val derivedTables: Seq[(String, DerivedTableContext => DataFrame)] = Seq(
      "order_summary" -> { ctx: DerivedTableContext =>
        ctx.table("orders").groupBy("category").agg(sum("amount").as("total"))
      }
    )

    executor.execute(derivedTables, "batch_tag_test")

    val refs = spark.sql("SELECT * FROM spark_catalog.default.order_summary.refs")
    val tags = refs.filter(col("type") === "TAG").select("name").collect().map(_.getString(0))
    tags should contain("batch_batch_tag_test")
  }

  it should "not tag snapshots when tagging is disabled" in {
    val orders = Seq((1, "A", 10.0)).toDF("id", "category", "amount")
    seedIcebergTable("orders", orders)

    val noTagConfig = icebergConfig.copy(enableSnapshotTagging = false)
    val executor = new DerivedTableExecutor(noTagConfig)
    val derivedTables: Seq[(String, DerivedTableContext => DataFrame)] = Seq(
      "order_summary" -> { ctx: DerivedTableContext =>
        ctx.table("orders").groupBy("category").agg(sum("amount").as("total"))
      }
    )

    executor.execute(derivedTables, "batch_no_tag")

    val refs = spark.sql("SELECT * FROM spark_catalog.default.order_summary.refs")
    val tags = refs.filter(col("type") === "TAG").select("name").collect().map(_.getString(0))
    tags should not contain "batch_batch_no_tag"
  }

  "IngestionPipelineBuilder" should "reject duplicate derived table names" in {
    val fn: DerivedTableContext => DataFrame = _ => spark.emptyDataFrame
    val builder = IngestionPipeline.builder()
      .withDerivedTable("my_table", fn)

    val ex = intercept[IllegalArgumentException] {
      builder.withDerivedTable("my_table", fn)
    }
    ex.getMessage should include("my_table")
    ex.getMessage should include("already registered")
  }
}
