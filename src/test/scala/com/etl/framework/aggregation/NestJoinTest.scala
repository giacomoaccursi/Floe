package com.etl.framework.aggregation

import com.etl.framework.config._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class NestJoinTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("NestJoinTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  private val parentRecords = Seq(
    ("p1", "alice", 100),
    ("p2", "bob", 200),
    ("p3", "charlie", 300)
  )

  private val childRecords = Seq(
    ("p1", "child_a", 10),
    ("p1", "child_b", 20),
    ("p2", "child_c", 30)
  )

  private def createNestJoinConfig(
      nestAs: String = "children",
      joinType: JoinType = JoinType.LeftOuter
  ): JoinConfig = JoinConfig(
    `type` = joinType,
    `with` = "parent_node",
    conditions = Seq(JoinCondition("id", "parent_id")),
    strategy = JoinStrategy.Nest,
    nestAs = Some(nestAs),
    aggregations = Seq.empty
  )

  "Nest join" should "create nested array field" in {
    val parentDF = parentRecords.toDF("id", "name", "value")
    val childDF = childRecords.toDF("parent_id", "child_name", "child_value")

    val executor = new JoinStrategyExecutor()
    val result =
      executor.applyJoin(parentDF, childDF, createNestJoinConfig())

    result.columns should contain("children")
    result.schema.fields
      .find(_.name == "children")
      .map(_.dataType) shouldBe a[Some[_]]
  }

  it should "preserve all parent records with left outer join" in {
    val parentDF = parentRecords.toDF("id", "name", "value")
    val childDF = childRecords.toDF("parent_id", "child_name", "child_value")

    val executor = new JoinStrategyExecutor()
    val result =
      executor.applyJoin(parentDF, childDF, createNestJoinConfig())

    result.count() shouldBe parentRecords.size.toLong
  }

  it should "group child records by parent" in {
    val parentDF = parentRecords.toDF("id", "name", "value")
    val childDF = childRecords.toDF("parent_id", "child_name", "child_value")

    val executor = new JoinStrategyExecutor()
    val result =
      executor.applyJoin(parentDF, childDF, createNestJoinConfig())

    val resultData = result.collect()

    resultData.foreach { row =>
      val parentId = row.getAs[String]("id")
      val children = row.getAs[Seq[Row]]("children")

      parentId match {
        case "p1" => children should have size 2
        case "p2" => children should have size 1
        case "p3" => children should have size 0
        case _    => fail(s"Unexpected parent id: $parentId")
      }
    }
  }

  it should "preserve all parent columns" in {
    val parentDF = parentRecords.toDF("id", "name", "value")
    val childDF = childRecords.toDF("parent_id", "child_name", "child_value")

    val executor = new JoinStrategyExecutor()
    val result =
      executor.applyJoin(parentDF, childDF, createNestJoinConfig())

    val parentColumns = parentDF.columns.toSet
    parentColumns.subsetOf(result.columns.toSet) shouldBe true
  }

  it should "handle join key with same name in both tables without ambiguity" in {
    val parentDF = Seq((1, "Alice"), (2, "Bob")).toDF("customer_id", "name")
    val childDF = Seq((10, 1, "shipped"), (11, 1, "pending"), (12, 2, "shipped"))
      .toDF("order_id", "customer_id", "status")

    val joinConfig = JoinConfig(
      `type` = JoinType.LeftOuter,
      `with` = "customers",
      conditions = Seq(JoinCondition("customer_id", "customer_id")),
      strategy = JoinStrategy.Nest,
      nestAs = Some("orders")
    )

    val executor = new JoinStrategyExecutor()
    val result = executor.applyJoin(parentDF, childDF, joinConfig)

    result.columns should contain allOf ("customer_id", "name", "orders")
    result.count() shouldBe 2L
  }

  it should "produce empty arrays for parents without children" in {
    val parentDF = parentRecords.toDF("id", "name", "value")
    val emptyChildDF =
      Seq
        .empty[(String, String, Int)]
        .toDF("parent_id", "child_name", "child_value")

    val executor = new JoinStrategyExecutor()
    val result =
      executor.applyJoin(parentDF, emptyChildDF, createNestJoinConfig())

    val resultData = result.collect()

    resultData.foreach { row =>
      val children = row.getAs[Seq[Row]]("children")
      children should not be null
      children shouldBe empty
    }
  }

  it should "produce empty arrays for parent records when child DataFrame is empty" in {
    val parentDF = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val emptyChildDF = spark.createDataFrame(
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      org.apache.spark.sql.types.StructType(
        Seq(
          org.apache.spark.sql.types.StructField("parent_id", org.apache.spark.sql.types.IntegerType),
          org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType)
        )
      )
    )

    val joinConfig = JoinConfig(
      `type` = JoinType.LeftOuter,
      `with` = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = JoinStrategy.Nest,
      nestAs = Some("children")
    )

    val executor = new JoinStrategyExecutor()
    val result = executor.applyJoin(parentDF, emptyChildDF, joinConfig)

    result.count() shouldBe 2L
    result.filter(org.apache.spark.sql.functions.size(result("children")) === 0).count() shouldBe 2L
  }
}
