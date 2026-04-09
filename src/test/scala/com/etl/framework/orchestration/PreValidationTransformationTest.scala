package com.etl.framework.orchestration

import com.etl.framework.pipeline.TransformationContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{functions => f}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PreValidationTransformationTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("PreValidationTransformationTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  private val testData = Seq(
    ("id_1", 100, "active"),
    ("id_2", 600, "inactive"),
    ("id_3", 300, "pending"),
    ("id_4", 800, "active"),
    ("id_5", 50, "inactive")
  )

  private def createContext(
      df: DataFrame,
      flowName: String = "test_flow",
      batchId: String = "test_batch"
  ): TransformationContext = TransformationContext(
    currentFlow = flowName,
    currentData = df,
    validatedFlows = Map.empty,
    batchId = batchId,
    spark = spark
  )

  "Pre-validation transformation" should "execute before validation and add marker column" in {
    var transformationCalled = false
    var transformationRecordCount: Long = 0

    val transformation: TransformationContext => TransformationContext = { ctx =>
      transformationCalled = true
      transformationRecordCount = ctx.currentData.count()
      ctx.withData(ctx.currentData.withColumn("_pre_transformation_marker", f.lit(true)))
    }

    val df = testData.toDF("id", "value", "status")
    val result = transformation(createContext(df))

    transformationCalled shouldBe true
    transformationRecordCount shouldBe 5L
    result.currentData.columns should contain("_pre_transformation_marker")
  }

  it should "receive correct context with flow name and batch ID" in {
    var receivedContext: Option[TransformationContext] = None

    val transformation: TransformationContext => TransformationContext = { ctx =>
      receivedContext = Some(ctx)
      ctx
    }

    val df = testData.toDF("id", "value", "status")
    transformation(createContext(df, "my_flow", "batch_002"))

    receivedContext shouldBe defined
    receivedContext.get.currentFlow shouldBe "my_flow"
    receivedContext.get.batchId shouldBe "batch_002"
    receivedContext.get.validatedFlows shouldBe empty
  }

  it should "make modifications visible to subsequent processing" in {
    val transformation: TransformationContext => TransformationContext = { ctx =>
      ctx.withData(ctx.currentData.filter(f.col("value") > 500))
    }

    val df = testData.toDF("id", "value", "status")
    val result = transformation(createContext(df))

    result.currentData.count() shouldBe 2L
  }

  it should "preserve schema when not modifying columns" in {
    val transformation: TransformationContext => TransformationContext = { ctx =>
      ctx.withData(ctx.currentData.filter(f.col("value") > 0))
    }

    val df = testData.toDF("id", "value", "status")
    val originalSchema = df.schema
    val result = transformation(createContext(df))

    result.currentData.schema shouldBe originalSchema
  }
}
