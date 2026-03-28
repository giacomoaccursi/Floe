package com.etl.framework.aggregation

import com.etl.framework.config._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FlattenJoinTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("FlattenJoinTest")
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

  private def createFlattenJoinConfig(
      joinType: JoinType = JoinType.LeftOuter
  ): JoinConfig = JoinConfig(
    `type` = joinType,
    parent = "parent_node",
    conditions = Seq(JoinCondition("id", "parent_id")),
    strategy = JoinStrategy.Flatten,
    nestAs = None,
    aggregations = Seq.empty
  )

  "Flatten join" should "preserve all parent columns" in {
    val parentDF = parentRecords.toDF("id", "parent_name", "parent_value")
    val childDF = childRecords.toDF("parent_id", "child_name", "child_value")

    val executor = new JoinStrategyExecutor()
    val result =
      executor.applyJoin(parentDF, childDF, createFlattenJoinConfig())

    val parentColumns = parentDF.columns.toSet
    parentColumns.subsetOf(result.columns.toSet) shouldBe true
  }

  it should "include child columns in result" in {
    val parentDF = parentRecords.toDF("id", "parent_name", "parent_value")
    val childDF = childRecords.toDF("parent_id", "child_name", "child_value")

    val executor = new JoinStrategyExecutor()
    val result =
      executor.applyJoin(parentDF, childDF, createFlattenJoinConfig())

    val childJoinKeys = Set("parent_id")
    val childDataColumns = childDF.columns.toSet -- childJoinKeys
    val resultColumns = result.columns.toSet

    childDataColumns.foreach { childCol =>
      (resultColumns.contains(childCol) || resultColumns.contains(
        s"child_$childCol"
      )) shouldBe true
    }
  }

  it should "handle column name conflicts with prefix" in {
    val parentDF = parentRecords.toDF("id", "parent_name", "parent_value")
    val conflictChildDF =
      Seq(("p1", "conflict_value", 42), ("p2", "conflict_value2", 43))
        .toDF("parent_id", "parent_name", "child_specific")

    val joinConfig = createFlattenJoinConfig(JoinType.Inner)
    val executor = new JoinStrategyExecutor()
    val result = executor.applyJoin(parentDF, conflictChildDF, joinConfig)

    val resultColumns = result.columns.toSet
    resultColumns should contain("parent_name")
    resultColumns should contain("child_parent_name")
  }

  it should "produce only matching records with inner join" in {
    val parentDF = parentRecords.toDF("id", "parent_name", "parent_value")
    // Only p1 has children
    val singleChildDF =
      Seq(("p1", "child_a", 10)).toDF("parent_id", "child_name", "child_value")

    val joinConfig = createFlattenJoinConfig(JoinType.Inner)
    val executor = new JoinStrategyExecutor()
    val result = executor.applyJoin(parentDF, singleChildDF, joinConfig)

    result.count() shouldBe 1L
  }

  it should "preserve all parent records with left outer join" in {
    val parentDF = parentRecords.toDF("id", "parent_name", "parent_value")
    // Only p1 has children
    val singleChildDF =
      Seq(("p1", "child_a", 10)).toDF("parent_id", "child_name", "child_value")

    val joinConfig = createFlattenJoinConfig(JoinType.LeftOuter)
    val executor = new JoinStrategyExecutor()
    val result = executor.applyJoin(parentDF, singleChildDF, joinConfig)

    result.count() shouldBe parentRecords.size.toLong
  }

  it should "handle join key with same name in both tables without ambiguity" in {
    // Both parent and child share the same join key column name: "product_id"
    val parentDF = Seq((1, "Widget"), (2, "Gadget")).toDF("product_id", "product_name")
    val childDF  = Seq((10, 1, 5), (11, 1, 3), (12, 2, 7)).toDF("order_id", "product_id", "quantity")

    val joinConfig = JoinConfig(
      `type` = JoinType.Inner,
      parent = "products",
      conditions = Seq(JoinCondition("product_id", "product_id")),
      strategy = JoinStrategy.Flatten
    )

    val executor = new JoinStrategyExecutor()
    val result = executor.applyJoin(parentDF, childDF, joinConfig)

    result.count() shouldBe 3L
    result.columns should contain allOf("product_id", "product_name", "order_id", "quantity")
  }
}
