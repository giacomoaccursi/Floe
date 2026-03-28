package com.etl.framework.orchestration.flow

import com.etl.framework.config._
import com.etl.framework.core.{AdditionalTableInfo, AdditionalTableMetadata}
import org.apache.spark.sql.{SparkSession, functions => f}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class FlowTransformerAddTableTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("FlowTransformerAddTableTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  import spark.implicits._

  private val flowConfig = FlowConfig(
    name = "orders",
    description = "test",
    version = "1.0",
    owner = "test",
    source = SourceConfig(SourceType.File, "data/orders.csv", FileFormat.CSV, Map.empty),
    schema = SchemaConfig(enforceSchema = false, allowExtraColumns = true, Seq.empty),
    loadMode = LoadModeConfig(LoadMode.Full),
    validation = ValidationConfig(Seq("id"), Seq.empty, Seq.empty),
    output = OutputConfig(),
    preValidationTransformation = None,
    postValidationTransformation = None
  )

  "FlowTransformer" should "collect additional tables registered via addTable in post-validation" in {
    val additionalTables = mutable.Map[String, AdditionalTableInfo]()

    val config = flowConfig.copy(
      postValidationTransformation = Some { ctx =>
        val summary = ctx.currentData.groupBy("category").agg(f.count("*").as("cnt"))
        ctx
          .withData(ctx.currentData)
          .addTable(
            tableName = "order_summary",
            data = summary,
            dagMetadata = Some(AdditionalTableMetadata(primaryKey = Seq("category")))
          )
      }
    )

    val transformer = new FlowTransformer(config, additionalTables)
    val df = Seq(("1", "A"), ("2", "B"), ("3", "A")).toDF("id", "category")

    transformer.applyPostValidationTransformation(df, "batch_001", Map.empty)

    additionalTables should contain key "order_summary"
    additionalTables("order_summary").data.count() shouldBe 2
  }

  it should "collect additional tables registered via addTable in pre-validation" in {
    val additionalTables = mutable.Map[String, AdditionalTableInfo]()

    val config = flowConfig.copy(
      preValidationTransformation = Some { ctx =>
        val summary = ctx.currentData.groupBy("category").agg(f.count("*").as("cnt"))
        ctx.addTable("pre_summary", summary)
      }
    )

    val transformer = new FlowTransformer(config, additionalTables)
    val df = Seq(("1", "X"), ("2", "Y")).toDF("id", "category")

    transformer.applyPreValidationTransformation(df, "batch_001")

    additionalTables should contain key "pre_summary"
  }
}
