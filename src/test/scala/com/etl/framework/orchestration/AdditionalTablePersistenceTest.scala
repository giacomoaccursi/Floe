package com.etl.framework.orchestration

import com.etl.framework.core.{AdditionalTableMetadata, TransformationContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{functions => f}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AdditionalTablePersistenceTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("AdditionalTablePersistenceTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  private val testData = Seq(
    ("id_1", 100, "A", 1500.0),
    ("id_2", 200, "B", 2500.0),
    ("id_3", 300, "A", 3500.0),
    ("id_4", 400, "C", 500.0),
    ("id_5", 500, "B", 7500.0)
  )

  private def createContext(
      df: DataFrame
  ): TransformationContext = TransformationContext(
    currentFlow = "test_flow",
    currentData = df,
    validatedFlows = Map.empty,
    batchId = "test_batch",
    spark = spark
  )

  "Additional table" should "be stored in context via addTable" in {
    val df = testData.toDF("id", "count", "category", "amount")
    val ctx = createContext(df)

    val additionalTable = df
      .groupBy("category")
      .agg(
        f.count("*").as("record_count"),
        f.sum("amount").as("total_amount")
      )

    val updatedCtx = ctx.addTable(
      tableName = "summary_table",
      data = additionalTable
    )

    updatedCtx.getAdditionalTables should have size 1
    updatedCtx.getAdditionalTables should contain key "summary_table"

    val tableInfo = updatedCtx.getAdditionalTables("summary_table")
    tableInfo.tableName shouldBe "summary_table"
    tableInfo.data.count() should be > 0L
    tableInfo.outputPath shouldBe None
    tableInfo.dagMetadata shouldBe None
  }

  it should "support custom output path" in {
    val df = testData.toDF("id", "count", "category", "amount")
    val ctx = createContext(df)

    val additionalTable = df
      .groupBy("category")
      .agg(f.count("*").as("record_count"))

    val customPath = "/tmp/custom_output/summary_custom"
    val updatedCtx = ctx.addTable(
      tableName = "summary_custom",
      data = additionalTable,
      outputPath = Some(customPath)
    )

    val tableInfo = updatedCtx.getAdditionalTables("summary_custom")
    tableInfo.outputPath shouldBe Some(customPath)
  }

  it should "include DAG metadata with join configuration" in {
    val df = testData.toDF("id", "count", "category", "amount")
    val ctx = createContext(df)

    val additionalTable = df
      .groupBy("category")
      .agg(f.count("*").as("record_count"))

    val dagMetadata = Some(
      AdditionalTableMetadata(
        primaryKey = Seq("category"),
        joinKeys = Map(
          "customers" -> Seq("customer_id"),
          "orders" -> Seq("order_id")
        ),
        description = Some("Test aggregation table"),
        partitionBy = Seq("category")
      )
    )

    val updatedCtx = ctx.addTable(
      tableName = "summary_dag",
      data = additionalTable,
      dagMetadata = dagMetadata
    )

    val tableInfo = updatedCtx.getAdditionalTables("summary_dag")
    tableInfo.dagMetadata shouldBe defined
    tableInfo.dagMetadata.get.primaryKey shouldBe Seq("category")
    tableInfo.dagMetadata.get.joinKeys should contain key "customers"
    tableInfo.dagMetadata.get.joinKeys should contain key "orders"
    tableInfo.dagMetadata.get.description shouldBe Some(
      "Test aggregation table"
    )
    tableInfo.dagMetadata.get.partitionBy shouldBe Seq("category")
  }

  it should "support multiple tables in single context" in {
    val df = testData.toDF("id", "count", "category", "amount")
    val ctx = createContext(df)

    val table1 = df
      .groupBy("category")
      .agg(f.count("*").as("record_count"))

    val table2 = df
      .withColumn(
        "amount_range",
        f.when(f.col("amount") < 1000, "low")
          .when(f.col("amount") < 5000, "medium")
          .otherwise("high")
      )
      .groupBy("amount_range")
      .agg(f.count("*").as("record_count"))

    val ctx2 = ctx.addTable(
      tableName = "summary_by_category",
      data = table1
    )
    val ctx3 = ctx2.addTable(
      tableName = "summary_by_amount_range",
      data = table2
    )

    ctx3.getAdditionalTables should have size 2
    ctx3.getAdditionalTables should contain key "summary_by_category"
    ctx3.getAdditionalTables should contain key "summary_by_amount_range"

    ctx3.getAdditionalTables("summary_by_category").data.count() should be > 0L
    ctx3
      .getAdditionalTables("summary_by_amount_range")
      .data
      .count() should be > 0L
  }

  it should "preserve original context immutability" in {
    val df = testData.toDF("id", "count", "category", "amount")
    val ctx = createContext(df)

    val additionalTable = df
      .groupBy("category")
      .agg(f.count("*").as("record_count"))

    val updatedCtx = ctx.addTable(
      tableName = "summary",
      data = additionalTable
    )

    ctx.getAdditionalTables shouldBe empty
    updatedCtx.getAdditionalTables should have size 1
  }
}
