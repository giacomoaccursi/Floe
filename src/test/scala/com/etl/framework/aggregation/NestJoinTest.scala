package com.etl.framework.aggregation

import com.etl.framework.config._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.ArrayType
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
    parent = "parent_node",
    on = Seq(JoinCondition("id", "parent_id")),
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
      .map(_.dataType) shouldBe a[Some[ArrayType]]
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

  it should "produce empty arrays for parents without children" in {
    val parentDF = parentRecords.toDF("id", "name", "value")
    val emptyChildDF =
      Seq.empty[(String, String, Int)]
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
}
