package com.etl.framework.aggregation

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

    val joinConfig = JoinConfig(
      `type` = "left",
      parent = "parent",
      on = Seq(JoinCondition("id", "parent_id")),
      strategy = "nest",
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

    val joinConfig = JoinConfig(
      `type` = "left",
      parent = "parent",
      on = Seq(JoinCondition("id", "parent_id")),
      strategy = "nest",
      nestAs = None
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain("nested_records")
  }

  it should "produce empty array for parent without children in nest join" in {
    val parent = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val child = Seq((1, "order1")).toDF("parent_id", "order_name")

    val joinConfig = JoinConfig(
      `type` = "left",
      parent = "parent",
      on = Seq(JoinCondition("id", "parent_id")),
      strategy = "nest",
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

    val joinConfig = JoinConfig(
      `type` = "inner",
      parent = "parent",
      on = Seq(JoinCondition("id", "parent_id")),
      strategy = "flatten"
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain allOf ("id", "name", "city", "age")
    result.count() shouldBe 2
  }

  it should "rename conflicting columns with child_ prefix" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, "AliceChild")).toDF("parent_id", "name")

    val joinConfig = JoinConfig(
      `type` = "inner",
      parent = "parent",
      on = Seq(JoinCondition("id", "parent_id")),
      strategy = "flatten"
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain("child_name")
  }

  it should "exclude child join keys from flattened result" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, "detail")).toDF("parent_id", "detail")

    val joinConfig = JoinConfig(
      `type` = "inner",
      parent = "parent",
      on = Seq(JoinCondition("id", "parent_id")),
      strategy = "flatten"
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain("detail")
    // parent_id is a join key and should not appear as a separate column
  }

  // --- Aggregate strategy ---

  "JoinStrategyExecutor (aggregate)" should "apply sum aggregation" in {
    val parent = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val child = Seq((1, 100.0), (1, 200.0), (2, 50.0)).toDF("parent_id", "amount")

    val joinConfig = JoinConfig(
      `type` = "left",
      parent = "parent",
      on = Seq(JoinCondition("id", "parent_id")),
      strategy = "aggregate",
      aggregations = Seq(AggregationSpec("amount", "sum", "total_amount"))
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain allOf ("id", "name", "total_amount")
    result.filter($"id" === 1).select("total_amount").head().getDouble(0) shouldBe 300.0
    result.filter($"id" === 2).select("total_amount").head().getDouble(0) shouldBe 50.0
  }

  it should "apply count aggregation" in {
    val parent = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val child = Seq((1, "a"), (1, "b"), (1, "c"), (2, "d")).toDF("parent_id", "item")

    val joinConfig = JoinConfig(
      `type` = "left",
      parent = "parent",
      on = Seq(JoinCondition("id", "parent_id")),
      strategy = "aggregate",
      aggregations = Seq(AggregationSpec("item", "count", "item_count"))
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.filter($"id" === 1).select("item_count").head().getLong(0) shouldBe 3
    result.filter($"id" === 2).select("item_count").head().getLong(0) shouldBe 1
  }

  it should "apply multiple aggregations" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, 10.0), (1, 20.0), (1, 30.0)).toDF("parent_id", "score")

    val joinConfig = JoinConfig(
      `type` = "left",
      parent = "parent",
      on = Seq(JoinCondition("id", "parent_id")),
      strategy = "aggregate",
      aggregations = Seq(
        AggregationSpec("score", "sum", "total_score"),
        AggregationSpec("score", "avg", "avg_score"),
        AggregationSpec("score", "min", "min_score"),
        AggregationSpec("score", "max", "max_score")
      )
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain allOf ("total_score", "avg_score", "min_score", "max_score")
    result.select("total_score").head().getDouble(0) shouldBe 60.0
    result.select("avg_score").head().getDouble(0) shouldBe 20.0
    result.select("min_score").head().getDouble(0) shouldBe 10.0
    result.select("max_score").head().getDouble(0) shouldBe 30.0
  }

  it should "throw exception for empty aggregations" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, "a")).toDF("parent_id", "item")

    val joinConfig = JoinConfig(
      `type` = "left",
      parent = "parent",
      on = Seq(JoinCondition("id", "parent_id")),
      strategy = "aggregate",
      aggregations = Seq.empty
    )

    intercept[ValidationConfigException] {
      executor.applyJoin(parent, child, joinConfig)
    }
  }

  it should "throw exception for unsupported aggregation function" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, "a")).toDF("parent_id", "item")

    val joinConfig = JoinConfig(
      `type` = "left",
      parent = "parent",
      on = Seq(JoinCondition("id", "parent_id")),
      strategy = "aggregate",
      aggregations = Seq(AggregationSpec("item", "median", "item_median"))
    )

    intercept[UnsupportedOperationException] {
      executor.applyJoin(parent, child, joinConfig)
    }
  }

  // --- General ---

  "JoinStrategyExecutor" should "throw exception for unsupported strategy" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, "a")).toDF("parent_id", "item")

    val joinConfig = JoinConfig(
      `type` = "inner",
      parent = "parent",
      on = Seq(JoinCondition("id", "parent_id")),
      strategy = "unknown_strategy"
    )

    intercept[UnsupportedOperationException] {
      executor.applyJoin(parent, child, joinConfig)
    }
  }

  it should "support multiple join conditions" in {
    val parent = Seq((1, "A", "data1"), (2, "B", "data2")).toDF("id", "code", "info")
    val child = Seq((1, "A", 100), (2, "B", 200)).toDF("p_id", "p_code", "value")

    val joinConfig = JoinConfig(
      `type` = "inner",
      parent = "parent",
      on = Seq(
        JoinCondition("id", "p_id"),
        JoinCondition("code", "p_code")
      ),
      strategy = "flatten"
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.count() shouldBe 2
    result.columns should contain("value")
  }
}
