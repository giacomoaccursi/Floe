package com.etl.framework.pipeline

import com.etl.framework.TestFixtures
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
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.floe", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.floe.type", "hadoop")
    .config(
      "spark.sql.catalog.floe.warehouse", {
        Files.createTempDirectory("derived_table_test_warehouse").toString
      }
    )
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
    val fullName = s"floe.default.$tableName"
    val cols = df.schema.fields.map(f => s"${f.name} ${f.dataType.sql}").mkString(", ")
    try { spark.sql(s"DROP TABLE IF EXISTS $fullName") }
    catch { case _: Exception => }
    spark.sql(s"CREATE TABLE $fullName ($cols) USING iceberg")
    df.writeTo(fullName).append()
  }

  private def dropTable(name: String): Unit =
    try { val _ = spark.sql(s"DROP TABLE IF EXISTS floe.default.`${name.replace("`", "``")}`") }
    catch { case _: Exception => }

  private def derived(
      name: String,
      dependencies: Seq[String] = Seq.empty
  )(fn: DerivedTableContext => DataFrame): DerivedTableDefinition =
    DerivedTableDefinition(name, dependencies, fn)

  private def inputs(names: String*): Map[String, DataFrame] =
    names.map(name => name -> spark.table(s"floe.default.`${name.replace("`", "``")}`")).toMap

  override def afterEach(): Unit = {
    Seq(
      "orders",
      "order_summary",
      "orders_domestic",
      "orders_intl",
      "empty_derived",
      "failing_derived",
      "base_derived",
      "second_derived",
      "undeclared_reader",
      "daily orders"
    )
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
    val derivedTables = Seq(
      derived("order_summary", Seq("orders")) { ctx =>
        ctx
          .table("orders")
          .groupBy("category")
          .agg(sum("amount").as("total"))
      }
    )

    val results = executor.execute(derivedTables, "batch_001", inputs("orders"))

    results should have size 1
    results.head.success shouldBe true
    results.head.tableName shouldBe "order_summary"
    results.head.recordsWritten shouldBe 2L

    val written = spark.table("floe.default.order_summary")
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
    val derivedTables = Seq(
      derived("orders_domestic", Seq("orders")) { ctx =>
        ctx.table("orders").filter(col("country") === "IT")
      },
      derived("orders_intl", Seq("orders")) { ctx =>
        ctx.table("orders").filter(col("country") =!= "IT")
      }
    )

    val results = executor.execute(derivedTables, "batch_002", inputs("orders"))

    results should have size 2
    results.foreach(_.success shouldBe true)

    spark.table("floe.default.orders_domestic").count() shouldBe 2L
    spark.table("floe.default.orders_intl").count() shouldBe 1L
  }

  it should "overwrite existing data on re-execution (full load)" in {
    val orders1 = Seq((1, "A", 10.0)).toDF("id", "category", "amount")
    seedIcebergTable("orders", orders1)

    val executor = new DerivedTableExecutor(icebergConfig)
    val derivedTables = Seq(
      derived("order_summary", Seq("orders")) { ctx =>
        ctx.table("orders").groupBy("category").agg(sum("amount").as("total"))
      }
    )

    executor.execute(derivedTables, "batch_001", inputs("orders"))
    spark.table("floe.default.order_summary").count() shouldBe 1L

    // Seed new data and re-execute
    dropTable("orders")
    val orders2 = Seq((1, "A", 10.0), (2, "B", 20.0)).toDF("id", "category", "amount")
    seedIcebergTable("orders", orders2)

    executor.execute(derivedTables, "batch_002", inputs("orders"))
    spark.table("floe.default.order_summary").count() shouldBe 2L
  }

  it should "handle empty source table" in {
    val emptyOrders = spark.createDataFrame(
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      org.apache.spark.sql.types.StructType(
        Seq(
          org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType),
          org.apache.spark.sql.types.StructField("category", org.apache.spark.sql.types.StringType)
        )
      )
    )
    seedIcebergTable("orders", emptyOrders)

    val executor = new DerivedTableExecutor(icebergConfig)
    val derivedTables = Seq(
      derived("empty_derived", Seq("orders")) { ctx => ctx.table("orders") }
    )

    val results = executor.execute(derivedTables, "batch_empty", inputs("orders"))

    results.head.success shouldBe true
    results.head.recordsWritten shouldBe 0L
  }

  it should "quote a spaced table name and reserved derived column" in {
    val executor = new DerivedTableExecutor(icebergConfig)
    val derivedTables = Seq(
      derived("daily orders") { _ =>
        Seq((1, "A"), (2, "B")).toDF("select", "customer name")
      }
    )

    val result = executor.execute(derivedTables, "batch_quoted_identifiers", Map.empty[String, DataFrame]).head

    result.success shouldBe true
    val written = spark.table("floe.default.`daily orders`")
    written.columns should contain allOf ("select", "customer name")
    written.count() shouldBe 2L
  }

  it should "report failure without stopping other derived tables" in {
    val orders = Seq((1, "A", 10.0)).toDF("id", "category", "amount")
    seedIcebergTable("orders", orders)

    val executor = new DerivedTableExecutor(icebergConfig)
    val derivedTables = Seq(
      derived("failing_derived") { _ =>
        throw new RuntimeException("intentional failure")
      },
      derived("order_summary", Seq("orders")) { ctx =>
        ctx.table("orders").groupBy("category").agg(sum("amount").as("total"))
      }
    )

    val results = executor.execute(derivedTables, "batch_fail", inputs("orders"))

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

    val derivedTables = Seq(
      derived("order_summary", Seq("orders")) { ctx =>
        capturedBatchId = ctx.batchId
        ctx.table("orders")
      }
    )

    executor.execute(derivedTables, "batch_ctx_test", inputs("orders"))
    capturedBatchId shouldBe "batch_ctx_test"
  }

  it should "handle schema evolution by adding new columns" in {
    val orders = Seq((1, "A", 10.0)).toDF("id", "category", "amount")
    seedIcebergTable("orders", orders)

    val executor = new DerivedTableExecutor(icebergConfig)

    // First execution: 2 columns
    val v1 = Seq(
      derived("order_summary", Seq("orders")) { ctx =>
        ctx.table("orders").groupBy("category").agg(sum("amount").as("total"))
      }
    )
    executor.execute(v1, "batch_v1", inputs("orders"))

    // Second execution: 3 columns (added count)
    val v2 = Seq(
      derived("order_summary", Seq("orders")) { ctx =>
        ctx
          .table("orders")
          .groupBy("category")
          .agg(
            sum("amount").as("total"),
            count("*").as("cnt")
          )
      }
    )
    val results = executor.execute(v2, "batch_v2", inputs("orders"))

    results.head.success shouldBe true
    val written = spark.table("floe.default.order_summary")
    written.columns should contain("cnt")
  }

  it should "tag snapshots with batch ID after write" in {
    val orders = Seq((1, "A", 10.0)).toDF("id", "category", "amount")
    seedIcebergTable("orders", orders)

    val executor = new DerivedTableExecutor(icebergConfig)
    val derivedTables = Seq(
      derived("order_summary", Seq("orders")) { ctx =>
        ctx.table("orders").groupBy("category").agg(sum("amount").as("total"))
      }
    )

    executor.execute(derivedTables, "batch_tag_test", inputs("orders"))

    val refs = spark.sql("SELECT * FROM floe.default.order_summary.refs")
    val tags = refs.filter(col("type") === "TAG").select("name").collect().map(_.getString(0))
    tags should contain("batch_batch_tag_test")
  }

  it should "not tag snapshots when tagging is disabled" in {
    val orders = Seq((1, "A", 10.0)).toDF("id", "category", "amount")
    seedIcebergTable("orders", orders)

    val noTagConfig = icebergConfig.copy(enableSnapshotTagging = false)
    val executor = new DerivedTableExecutor(noTagConfig)
    val derivedTables = Seq(
      derived("order_summary", Seq("orders")) { ctx =>
        ctx.table("orders").groupBy("category").agg(sum("amount").as("total"))
      }
    )

    executor.execute(derivedTables, "batch_no_tag", inputs("orders"))

    val refs = spark.sql("SELECT * FROM floe.default.order_summary.refs")
    val tags = refs.filter(col("type") === "TAG").select("name").collect().map(_.getString(0))
    tags should not contain "batch_batch_no_tag"
  }

  it should "order derived tables by declared dependencies rather than registration order" in {
    seedIcebergTable("orders", Seq((1, 10), (2, 20)).toDF("id", "amount"))
    val definitions = Seq(
      derived("second_derived", Seq("base_derived")) { ctx =>
        ctx.table("base_derived").withColumn("doubled", col("amount") * 2)
      },
      derived("base_derived", Seq("orders"))(_.table("orders"))
    )

    val results = new DerivedTableExecutor(icebergConfig).execute(definitions, "batch_order", inputs("orders"))

    results.map(_.tableName) shouldBe Seq("base_derived", "second_derived")
    results.forall(_.success) shouldBe true
    spark.table("floe.default.second_derived").select(sum("doubled")).first().getLong(0) shouldBe 60L
  }

  it should "read the supplied snapshot even when catalog HEAD advances" in {
    seedIcebergTable("orders", Seq((1, "original")).toDF("id", "value"))
    val snapshotId =
      spark.sql("SELECT snapshot_id FROM floe.default.orders.refs WHERE name = 'main'").first().getLong(0)
    val pinned = spark.read.option("snapshot-id", snapshotId).table("floe.default.orders")
    Seq((2, "later")).toDF("id", "value").writeTo("floe.default.orders").append()

    val result = new DerivedTableExecutor(icebergConfig).execute(
      Seq(derived("base_derived", Seq("orders"))(_.table("orders"))),
      "batch_pinned",
      Map("orders" -> pinned)
    )

    result.head.success shouldBe true
    spark.table("floe.default.base_derived").select("id").as[Int].collect().toSeq shouldBe Seq(1)
  }

  it should "reject undeclared reads and block their dependents" in {
    seedIcebergTable("orders", Seq((1, "A")).toDF("id", "value"))
    val definitions = Seq(
      derived("base_derived", Seq("orders"))(_.table("customers")),
      derived("second_derived", Seq("base_derived"))(_.table("base_derived"))
    )

    val results = new DerivedTableExecutor(icebergConfig).execute(definitions, "batch_blocked", inputs("orders"))

    results.head.success shouldBe false
    results.head.error.get should include("not declared")
    results(1).success shouldBe false
    results(1).error.get should include("Blocked by failed derived dependencies")
  }

  it should "reject unknown and cyclic derived dependencies before execution" in {
    val unknown = Seq(derived("base_derived", Seq("missing"))(_ => spark.emptyDataFrame))
    DerivedTableExecutor.validateDefinitions(unknown, Set("orders")).mkString(" ") should include(
      "unknown dependency 'missing'"
    )

    val cyclic = Seq(
      derived("base_derived", Seq("second_derived"))(_ => spark.emptyDataFrame),
      derived("second_derived", Seq("base_derived"))(_ => spark.emptyDataFrame)
    )
    DerivedTableExecutor.validateDefinitions(cyclic, Set("orders")).mkString(" ") should include(
      "dependency cycle"
    )
  }

  "IngestionPipelineBuilder" should "reject duplicate derived table names" in {
    val fn: DerivedTableContext => DataFrame = _ => spark.emptyDataFrame
    val builder = IngestionPipeline
      .builder()
      .withDerivedTable("my_table", Seq.empty, fn)

    val ex = intercept[IllegalArgumentException] {
      builder.withDerivedTable("my_table", Seq.empty, fn)
    }
    ex.getMessage should include("my_table")
    ex.getMessage should include("already registered")
  }

  it should "reject derived table names that differ only by case" in {
    val builder = IngestionPipeline
      .builder()
      .withDerivedTable("Daily_Orders", Seq.empty, _ => spark.emptyDataFrame)

    val ex = intercept[IllegalArgumentException] {
      builder.withDerivedTable("daily_orders", Seq.empty, _ => spark.emptyDataFrame)
    }
    ex.getMessage should include("already registered")
  }

  it should "reject blank or qualified derived identifiers at registration" in {
    an[IllegalArgumentException] should be thrownBy IngestionPipeline
      .builder()
      .withDerivedTable(" ", Seq.empty, _ => spark.emptyDataFrame)
    an[IllegalArgumentException] should be thrownBy IngestionPipeline
      .builder()
      .withDerivedTable("analytics.daily_orders", Seq.empty, _ => spark.emptyDataFrame)
    an[IllegalArgumentException] should be thrownBy IngestionPipeline
      .builder()
      .withDerivedTable("daily_orders", Seq("analytics.orders"), _ => spark.emptyDataFrame)
  }

  it should "reject a derived table that collides with a primary flow table" in {
    val builder = IngestionPipeline
      .builder()
      .withGlobalConfig(TestFixtures.globalConfig(iceberg = icebergConfig))
      .withFlowConfigs(Seq(TestFixtures.flowConfig(name = "orders")))
      .withDerivedTable("orders", Seq.empty, (_: DerivedTableContext) => spark.emptyDataFrame)

    val ex = intercept[IllegalArgumentException] {
      builder.build()
    }
    ex.getMessage should include("orders")
    builder.validate().mkString(" ") should include("orders")
  }

  it should "reject a derived table that collides with the quality metrics table" in {
    val baseConfig = TestFixtures.globalConfig(iceberg = icebergConfig)
    val globalConfig = baseConfig.copy(
      processing = baseConfig.processing.copy(qualityMetricsTable = Some("quality_metrics"))
    )
    val builder = IngestionPipeline
      .builder()
      .withGlobalConfig(globalConfig)
      .withFlowConfigs(Seq(TestFixtures.flowConfig(name = "orders")))
      .withDerivedTable("QUALITY_METRICS", Seq.empty, (_: DerivedTableContext) => spark.emptyDataFrame)

    intercept[IllegalArgumentException] {
      builder.build()
    }
    builder.validate().mkString(" ") should include("QUALITY_METRICS")
  }

  it should "reject unknown derived dependencies during pipeline validation and build" in {
    val builder = IngestionPipeline
      .builder()
      .withGlobalConfig(TestFixtures.globalConfig(iceberg = icebergConfig))
      .withFlowConfigs(Seq(TestFixtures.flowConfig(name = "orders")))
      .withDerivedTable("order_summary", Seq("missing"), _ => spark.emptyDataFrame)

    builder.validate().mkString(" ") should include("unknown dependency 'missing'")
    intercept[IllegalArgumentException](builder.build()).getMessage should include("unknown dependency 'missing'")
  }

  it should "reject derived dependency cycles during pipeline validation and build" in {
    val builder = IngestionPipeline
      .builder()
      .withGlobalConfig(TestFixtures.globalConfig(iceberg = icebergConfig))
      .withFlowConfigs(Seq(TestFixtures.flowConfig(name = "orders")))
      .withDerivedTable("base_derived", Seq("second_derived"), _ => spark.emptyDataFrame)
      .withDerivedTable("second_derived", Seq("base_derived"), _ => spark.emptyDataFrame)

    builder.validate().mkString(" ") should include("dependency cycle")
    intercept[IllegalArgumentException](builder.build()).getMessage should include("dependency cycle")
  }
}
