package com.etl.framework.aggregation

import com.etl.framework.config.JoinStrategy.{Aggregate, Flatten, Nest}
import com.etl.framework.config.{AggregationSpec, JoinCondition, JoinConfig}
import com.etl.framework.exceptions.ValidationConfigException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession

class JoinStrategyExecutorTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("JoinStrategyExecutorTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  val executor = new JoinStrategyExecutor()

  // --- Nest strategy ---

  "JoinStrategyExecutor (nest)" should "nest child records as array of structs" in {
    val parent = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val child = Seq((1, "order1"), (1, "order2"), (2, "order3")).toDF("parent_id", "order_name")

    import com.etl.framework.config.JoinType._

    val joinConfig = JoinConfig(
      `type` = LeftOuter,
      parent = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Nest,
      nestAs = Some("orders")
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain allOf ("id", "name", "orders")
    result.count() shouldBe 2

    val aliceOrders = result.filter($"id" === 1).select("orders").head().getSeq(0)
    aliceOrders should have size 2
  }

  it should "use default nest field name when not specified" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, "item1")).toDF("parent_id", "item")

    import com.etl.framework.config.JoinType._

    val joinConfig = JoinConfig(
      `type` = LeftOuter,
      parent = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Nest,
      nestAs = None
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain("nested_records")
  }

  it should "produce empty array for parent without children in nest join" in {
    val parent = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val child = Seq((1, "order1")).toDF("parent_id", "order_name")

    import com.etl.framework.config.JoinType._

    val joinConfig = JoinConfig(
      `type` = LeftOuter,
      parent = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Nest,
      nestAs = Some("orders")
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    val bobOrders = result.filter($"id" === 2).select("orders").head().getSeq(0)
    bobOrders should have size 0
  }

  // --- Flatten strategy ---

  "JoinStrategyExecutor (flatten)" should "flatten child fields into parent record" in {
    val parent = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val child = Seq((1, "Rome", 30), (2, "Milan", 25)).toDF("parent_id", "city", "age")

    import com.etl.framework.config.JoinType._

    val joinConfig = JoinConfig(
      `type` = Inner,
      parent = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Flatten
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain allOf ("id", "name", "city", "age")
    result.count() shouldBe 2
  }

  it should "rename conflicting columns with child_ prefix" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, "AliceChild")).toDF("parent_id", "name")

    import com.etl.framework.config.JoinType._

    val joinConfig = JoinConfig(
      `type` = Inner,
      parent = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Flatten
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain("child_name")
  }

  it should "exclude child join keys from flattened result" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, "detail")).toDF("parent_id", "detail")

    import com.etl.framework.config.JoinType._

    val joinConfig = JoinConfig(
      `type` = Inner,
      parent = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Flatten
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain("detail")
    // parent_id is a join key and should not appear as a separate column
  }

  // --- Aggregate strategy ---

  "JoinStrategyExecutor (aggregate)" should "apply sum aggregation" in {
    val parent = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val child = Seq((1, 100.0), (1, 200.0), (2, 50.0)).toDF("parent_id", "amount")

    import com.etl.framework.config.JoinType._
    import com.etl.framework.config.AggregationFunction._

    val joinConfig = JoinConfig(
      `type` = LeftOuter,
      parent = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Aggregate,
      aggregations = Seq(AggregationSpec("amount", Sum, "total_amount"))
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain allOf ("id", "name", "total_amount")
    result.filter($"id" === 1).select("total_amount").head().getDouble(0) shouldBe 300.0
    result.filter($"id" === 2).select("total_amount").head().getDouble(0) shouldBe 50.0
  }

  it should "apply count aggregation" in {
    val parent = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val child = Seq((1, "a"), (1, "b"), (1, "c"), (2, "d")).toDF("parent_id", "item")

    import com.etl.framework.config.JoinType._
    import com.etl.framework.config.AggregationFunction._

    val joinConfig = JoinConfig(
      `type` = LeftOuter,
      parent = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Aggregate,
      aggregations = Seq(AggregationSpec("item", Count, "item_count"))
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.filter($"id" === 1).select("item_count").head().getLong(0) shouldBe 3
    result.filter($"id" === 2).select("item_count").head().getLong(0) shouldBe 1
  }

  it should "apply multiple aggregations" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, 10.0), (1, 20.0), (1, 30.0)).toDF("parent_id", "score")

    import com.etl.framework.config.JoinType._
    import com.etl.framework.config.AggregationFunction._

    val joinConfig = JoinConfig(
      `type` = LeftOuter,
      parent = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Aggregate,
      aggregations = Seq(
        AggregationSpec("score", Sum, "total_score"),
        AggregationSpec("score", Avg, "avg_score"),
        AggregationSpec("score", Min, "min_score"),
        AggregationSpec("score", Max, "max_score")
      )
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain allOf ("total_score", "avg_score", "min_score", "max_score")
    result.select("total_score").head().getDouble(0) shouldBe 60.0
    result.select("avg_score").head().getDouble(0) shouldBe 20.0
    result.select("min_score").head().getDouble(0) shouldBe 10.0
    result.select("max_score").head().getDouble(0) shouldBe 30.0
  }

  it should "handle aggregate join key with same name in both tables without ambiguity" in {
    val parent = Seq((1, "Alice"), (2, "Bob")).toDF("customer_id", "name")
    val child = Seq((1, 50.0), (1, 30.0), (2, 20.0)).toDF("customer_id", "amount")

    import com.etl.framework.config.JoinType._
    import com.etl.framework.config.AggregationFunction._

    val joinConfig = JoinConfig(
      `type` = LeftOuter,
      parent = "customers",
      conditions = Seq(JoinCondition("customer_id", "customer_id")),
      strategy = Aggregate,
      aggregations = Seq(AggregationSpec("amount", Sum, "total_spent"))
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain allOf ("customer_id", "name", "total_spent")
    result.filter($"customer_id" === 1).select("total_spent").head().getDouble(0) shouldBe 80.0
  }

  it should "throw exception for empty aggregations" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, "a")).toDF("parent_id", "item")

    import com.etl.framework.config.JoinType._

    val joinConfig = JoinConfig(
      `type` = LeftOuter,
      parent = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Aggregate,
      aggregations = Seq.empty
    )

    intercept[ValidationConfigException] {
      executor.applyJoin(parent, child, joinConfig)
    }
  }

}
