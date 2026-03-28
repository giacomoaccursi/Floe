package com.etl.framework.orchestration

import com.etl.framework.core.TransformationContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{functions => f}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PostValidationTransformationTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("PostValidationTransformationTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  private val currentData = Seq(
    ("id_1", 100, "active"),
    ("id_2", 200, "inactive"),
    ("id_3", 300, "pending")
  )

  private val customersData = Seq(
    ("c1", 10, "active"),
    ("c2", 20, "inactive")
  )

  private val ordersData = Seq(
    ("o1", 50, "pending"),
    ("o2", 60, "active"),
    ("o3", 70, "inactive")
  )

  private def createContext(
      df: DataFrame,
      validatedFlows: Map[String, DataFrame] = Map.empty,
      flowName: String = "test_flow",
      batchId: String = "test_batch"
  ): TransformationContext = TransformationContext(
    currentFlow = flowName,
    currentData = df,
    validatedFlows = validatedFlows,
    batchId = batchId,
    spark = spark
  )

  "Post-validation transformation" should "have access to validated flows" in {
    var receivedFlows: Map[String, DataFrame] = Map.empty

    val transformation: TransformationContext => TransformationContext = { ctx =>
      receivedFlows = ctx.validatedFlows
      ctx
    }

    val df = currentData.toDF("id", "value", "status")
    val customersDf = customersData.toDF("id", "value", "status")
    val ordersDf = ordersData.toDF("id", "value", "status")
    val flows = Map("customers" -> customersDf, "orders" -> ordersDf)

    transformation(createContext(df, flows))

    receivedFlows.keySet shouldBe Set("customers", "orders")
    receivedFlows("customers").count() shouldBe 2L
    receivedFlows("orders").count() shouldBe 3L
  }

  it should "access specific flow via getFlow" in {
    var accessedFlow: Option[DataFrame] = None

    val transformation: TransformationContext => TransformationContext = { ctx =>
      accessedFlow = ctx.getFlow("customers")
      ctx
    }

    val df = currentData.toDF("id", "value", "status")
    val customersDf = customersData.toDF("id", "value", "status")
    transformation(createContext(df, Map("customers" -> customersDf)))

    accessedFlow shouldBe defined
    accessedFlow.get.count() shouldBe 2L
  }

  it should "return None for non-existent flow" in {
    var accessedFlow: Option[DataFrame] = None

    val transformation: TransformationContext => TransformationContext = { ctx =>
      accessedFlow = ctx.getFlow("non_existent")
      ctx
    }

    val df = currentData.toDF("id", "value", "status")
    transformation(createContext(df))

    accessedFlow shouldBe None
  }

  it should "join with other flows" in {
    val transformation: TransformationContext => TransformationContext = { ctx =>
      ctx.getFlow("other_flow") match {
        case Some(otherDf) =>
          ctx.withData(ctx.currentData.join(otherDf, Seq("id"), "left"))
        case None =>
          ctx
      }
    }

    val df = currentData.toDF("id", "value", "status")
    val otherDf = Seq(
      ("id_1", 999, "extra"),
      ("id_3", 888, "extra2")
    ).toDF("id", "value", "status")

    val result =
      transformation(createContext(df, Map("other_flow" -> otherDf)))

    result.currentData.count() shouldBe 3L
    result.currentData.columns.length should be >= df.columns.length
  }

  it should "preserve current data when accessing other flows" in {
    val transformation: TransformationContext => TransformationContext = { ctx =>
      ctx.validatedFlows.foreach { case (_, vdf) => vdf.count() }
      ctx
    }

    val df = currentData.toDF("id", "value", "status")
    val customersDf = customersData.toDF("id", "value", "status")
    val originalCount = df.count()
    val originalSchema = df.schema

    val result =
      transformation(createContext(df, Map("customers" -> customersDf)))

    result.currentData.count() shouldBe originalCount
    result.currentData.schema shouldBe originalSchema
  }

  it should "enrich data from other flows" in {
    val transformation: TransformationContext => TransformationContext = { ctx =>
      ctx.getFlow("enrichment_flow") match {
        case Some(enrichmentDf) =>
          val enriched = enrichmentDf
            .groupBy("status")
            .agg(f.avg("value").as("avg_value_by_status"))
          ctx.withData(ctx.currentData.join(enriched, Seq("status"), "left"))
        case None =>
          ctx
      }
    }

    val df = currentData.toDF("id", "value", "status")
    val enrichmentDf = Seq(
      ("e1", 100, "active"),
      ("e2", 200, "active"),
      ("e3", 300, "inactive")
    ).toDF("id", "value", "status")

    val result = transformation(
      createContext(df, Map("enrichment_flow" -> enrichmentDf))
    )

    result.currentData.columns should contain("avg_value_by_status")
    result.currentData.columns.length should be > df.columns.length
  }
}
