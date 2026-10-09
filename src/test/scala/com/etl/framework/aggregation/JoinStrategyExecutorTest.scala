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
    .config("spark.driver.bindAddress", "127.0.0.1")
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
      `with` = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Nest,
      nestAs = Some("orders"),
      orderBy = Seq("order_name")
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
      `with` = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Nest,
      nestAs = None,
      orderBy = Seq("item")
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
      `with` = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Nest,
      nestAs = Some("orders"),
      orderBy = Seq("order_name")
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    val bobOrders = result.filter($"id" === 2).select("orders").head().getSeq(0)
    bobOrders should have size 0
  }

  it should "reject a nest alias that collides with a parent column" in {
    val parent = Seq((1, "existing")).toDF("id", "orders")
    val child = Seq((1, "order1")).toDF("parent_id", "order_name")
    import com.etl.framework.config.JoinType._

    val error = intercept[ValidationConfigException] {
      executor.applyJoin(
        parent,
        child,
        JoinConfig(
          LeftOuter,
          "child",
          Seq(JoinCondition("id", "parent_id")),
          Nest,
          nestAs = Some("orders"),
          orderBy = Seq("order_name")
        )
      )
    }

    error.getMessage should include("orders")
  }

  it should "preserve child-only keys in full outer nest joins" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((2, "order2")).toDF("parent_id", "order_name")
    import com.etl.framework.config.JoinType._

    val result = executor.applyJoin(
      parent,
      child,
      JoinConfig(
        FullOuter,
        "child",
        Seq(JoinCondition("id", "parent_id")),
        Nest,
        nestAs = Some("orders"),
        orderBy = Seq("order_name")
      )
    )

    result.filter($"id".isNull).count() shouldBe 0
    result.select("id").as[Int].collect().toSet shouldBe Set(1, 2)
  }

  it should "nest map payloads without requiring the complete child struct to be orderable" in {
    val parent = Seq(1).toDF("id")
    val child = Seq(
      (1, 2, Map("status" -> "new")),
      (1, 1, Map("status" -> "old"))
    ).toDF("parent_id", "sequence", "attributes")
    import com.etl.framework.config.JoinType._

    val result = executor.applyJoin(
      parent,
      child,
      JoinConfig(
        LeftOuter,
        "child",
        Seq(JoinCondition("id", "parent_id")),
        Nest,
        nestAs = Some("events"),
        orderBy = Seq("sequence")
      )
    )

    result
      .selectExpr("transform(events, event -> event.sequence) AS sequences")
      .as[Seq[Int]]
      .head() shouldBe Seq(1, 2)
  }

  it should "order nested records lexicographically with nulls first" in {
    val parent = Seq(1).toDF("id")
    val child = Seq(
      (1, Some(1), 2, "first-b"),
      (1, None, 9, "unknown"),
      (1, Some(1), 1, "first-a"),
      (1, Some(2), 1, "second")
    ).toDF("parent_id", "sequence", "tie_breaker", "label")
    import com.etl.framework.config.JoinType._

    val result = executor.applyJoin(
      parent,
      child,
      JoinConfig(
        LeftOuter,
        "child",
        Seq(JoinCondition("id", "parent_id")),
        Nest,
        orderBy = Seq("sequence", "tie_breaker")
      )
    )

    result
      .selectExpr("transform(nested_records, event -> event.label) AS labels")
      .as[Seq[String]]
      .head() shouldBe Seq("unknown", "first-a", "first-b", "second")
  }

  it should "reject missing, duplicate, and non-orderable nest ordering columns" in {
    val parent = Seq(1).toDF("id")
    val child = Seq((1, 1, Map("status" -> "new"))).toDF("parent_id", "sequence", "attributes")
    import com.etl.framework.config.JoinType._

    val missingAndDuplicate = intercept[ValidationConfigException] {
      executor.applyJoin(
        parent,
        child,
        JoinConfig(
          LeftOuter,
          "child",
          Seq(JoinCondition("id", "parent_id")),
          Nest,
          orderBy = Seq("missing", "sequence", "SEQUENCE")
        )
      )
    }
    missingAndDuplicate.getMessage should include("must be unique")
    missingAndDuplicate.getMessage should include("'missing' was not found")

    val nonOrderable = intercept[ValidationConfigException] {
      executor.applyJoin(
        parent,
        child,
        JoinConfig(
          LeftOuter,
          "child",
          Seq(JoinCondition("id", "parent_id")),
          Nest,
          orderBy = Seq("attributes")
        )
      )
    }
    nonOrderable.getMessage should include("non-orderable type map<string,string>")
  }

  it should "require nest ordering and reject join-level ordering for other strategies" in {
    val parent = Seq(1).toDF("id")
    val child = Seq((1, 1)).toDF("parent_id", "sequence")
    import com.etl.framework.config.JoinType._

    intercept[ValidationConfigException] {
      executor.applyJoin(
        parent,
        child,
        JoinConfig(LeftOuter, "child", Seq(JoinCondition("id", "parent_id")), Nest)
      )
    }.getMessage should include("nest join requires orderBy")

    intercept[ValidationConfigException] {
      executor.applyJoin(
        parent,
        child,
        JoinConfig(
          LeftOuter,
          "child",
          Seq(JoinCondition("id", "parent_id")),
          Flatten,
          orderBy = Seq("sequence")
        )
      )
    }.getMessage should include("flatten join must not define orderBy")
  }

  // --- Flatten strategy ---

  "JoinStrategyExecutor (flatten)" should "flatten child fields into parent record" in {
    val parent = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val child = Seq((1, "Rome", 30), (2, "Milan", 25)).toDF("parent_id", "city", "age")

    import com.etl.framework.config.JoinType._

    val joinConfig = JoinConfig(
      `type` = Inner,
      `with` = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Flatten
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain allOf ("id", "name", "city", "age")
    result.count() shouldBe 2
  }

  it should "preserve the declared order of child data columns" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, "z", "a", "m", "o")).toDF(
      "parent_id",
      "zeta",
      "alpha",
      "middle",
      "omega"
    )

    import com.etl.framework.config.JoinType._

    val result = executor.applyJoin(
      parent,
      child,
      JoinConfig(
        `type` = Inner,
        `with` = "child",
        conditions = Seq(JoinCondition("id", "parent_id")),
        strategy = Flatten
      )
    )

    result.columns shouldBe Array("id", "name", "zeta", "alpha", "middle", "omega")
  }

  it should "rename conflicting columns with node name prefix" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, "AliceChild")).toDF("parent_id", "name")

    import com.etl.framework.config.JoinType._

    val joinConfig = JoinConfig(
      `type` = Inner,
      `with` = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Flatten
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain("parent_name")
  }

  it should "exclude child join keys from flattened result" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, "detail")).toDF("parent_id", "detail")

    import com.etl.framework.config.JoinType._

    val joinConfig = JoinConfig(
      `type` = Inner,
      `with` = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Flatten
    )

    val result = executor.applyJoin(parent, child, joinConfig)

    result.columns should contain("detail")
    // parent_id is a join key and should not appear as a separate column
  }

  it should "reject joins without conditions before building a Spark expression" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, "Rome")).toDF("parent_id", "city")
    import com.etl.framework.config.JoinType._

    val error = intercept[ValidationConfigException] {
      executor.applyJoin(parent, child, JoinConfig(Inner, "child", Seq.empty, Flatten))
    }

    error.getMessage should include("at least one condition")
  }

  it should "report missing join columns before Spark analysis" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, "Rome")).toDF("parent_id", "city")
    import com.etl.framework.config.JoinType._

    val error = intercept[ValidationConfigException] {
      executor.applyJoin(
        parent,
        child,
        JoinConfig(Inner, "child", Seq(JoinCondition("missing_id", "missing_parent_id")), Flatten)
      )
    }

    error.getMessage should include("left column 'missing_id' was not found")
    error.getMessage should include("right column 'missing_parent_id' was not found")
  }

  it should "reject join keys with different data types" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1L, "Rome")).toDF("parent_id", "city")
    import com.etl.framework.config.JoinType._

    val error = intercept[ValidationConfigException] {
      executor.applyJoin(
        parent,
        child,
        JoinConfig(Inner, "child", Seq(JoinCondition("id", "parent_id")), Flatten)
      )
    }

    error.getMessage should include("join key types must match")
    error.getMessage should include("int != bigint")
  }

  it should "rename case-insensitive child column conflicts" in {
    val parent = Seq((1, "Alice")).toDF("id", "Name")
    val child = Seq((1, "child-name")).toDF("parent_id", "name")
    import com.etl.framework.config.JoinType._

    val result = executor.applyJoin(
      parent,
      child,
      JoinConfig(Inner, "child", Seq(JoinCondition("id", "parent_id")), Flatten)
    )

    result.columns shouldBe Array("id", "Name", "child_name")
  }

  it should "preserve child-only keys in full outer flatten joins" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((2, "Rome")).toDF("parent_id", "city")
    import com.etl.framework.config.JoinType._

    val result = executor.applyJoin(
      parent,
      child,
      JoinConfig(FullOuter, "child", Seq(JoinCondition("id", "parent_id")), Flatten)
    )

    result.filter($"id".isNull).count() shouldBe 0
    result.select("id").as[Int].collect().toSet shouldBe Set(1, 2)
  }

  // --- Aggregate strategy ---

  "JoinStrategyExecutor (aggregate)" should "apply sum aggregation" in {
    val parent = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val child = Seq((1, 100.0), (1, 200.0), (2, 50.0)).toDF("parent_id", "amount")

    import com.etl.framework.config.JoinType._
    import com.etl.framework.config.AggregationFunction._

    val joinConfig = JoinConfig(
      `type` = LeftOuter,
      `with` = "parent",
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
      `with` = "parent",
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
      `with` = "parent",
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
      `with` = "customers",
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
      `with` = "parent",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = Aggregate,
      aggregations = Seq.empty
    )

    intercept[ValidationConfigException] {
      executor.applyJoin(parent, child, joinConfig)
    }
  }

  it should "reject aggregate aliases that collide with parent columns" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((1, 10.0)).toDF("parent_id", "amount")
    import com.etl.framework.config.AggregationFunction._
    import com.etl.framework.config.JoinType._

    val error = intercept[ValidationConfigException] {
      executor.applyJoin(
        parent,
        child,
        JoinConfig(
          LeftOuter,
          "child",
          Seq(JoinCondition("id", "parent_id")),
          Aggregate,
          aggregations = Seq(AggregationSpec("amount", Sum, "name"))
        )
      )
    }

    error.getMessage should include("name")
  }

  it should "reject case-insensitive aggregate alias collisions" in {
    val parent = Seq((1, 10.0)).toDF("id", "Total")
    val child = Seq((1, 5.0)).toDF("parent_id", "amount")
    import com.etl.framework.config.AggregationFunction._
    import com.etl.framework.config.JoinType._

    val error = intercept[ValidationConfigException] {
      executor.applyJoin(
        parent,
        child,
        JoinConfig(
          LeftOuter,
          "child",
          Seq(JoinCondition("id", "parent_id")),
          Aggregate,
          aggregations = Seq(AggregationSpec("amount", Sum, "total"))
        )
      )
    }

    error.getMessage should include("total")
  }

  it should "require orderBy for first and last aggregations" in {
    val parent = Seq(1).toDF("id")
    val child = Seq((1, "old", 1), (1, "new", 2)).toDF("parent_id", "value", "sequence")
    import com.etl.framework.config.AggregationFunction._
    import com.etl.framework.config.JoinType._

    intercept[ValidationConfigException] {
      executor.applyJoin(
        parent,
        child,
        JoinConfig(
          LeftOuter,
          "child",
          Seq(JoinCondition("id", "parent_id")),
          Aggregate,
          aggregations = Seq(AggregationSpec("value", First, "first_value"))
        )
      )
    }
  }

  it should "compute first and last deterministically from orderBy" in {
    val parent = Seq(1).toDF("id")
    val child = Seq((1, "middle", 2), (1, "last", 3), (1, "first", 1))
      .toDF("parent_id", "value", "sequence")
    import com.etl.framework.config.AggregationFunction._
    import com.etl.framework.config.JoinType._

    val result = executor.applyJoin(
      parent,
      child,
      JoinConfig(
        LeftOuter,
        "child",
        Seq(JoinCondition("id", "parent_id")),
        Aggregate,
        aggregations = Seq(
          AggregationSpec("value", First, "first_value", orderBy = Seq("sequence")),
          AggregationSpec("value", Last, "last_value", orderBy = Seq("sequence"))
        )
      )
    )

    result.select("first_value", "last_value").as[(String, String)].head() shouldBe (("first", "last"))
  }

  it should "sort collect_list output deterministically" in {
    val parent = Seq(1).toDF("id")
    val child = Seq((1, "c"), (1, "a"), (1, "b")).toDF("parent_id", "value").repartition(3)
    import com.etl.framework.config.AggregationFunction._
    import com.etl.framework.config.JoinType._

    val result = executor.applyJoin(
      parent,
      child,
      JoinConfig(
        LeftOuter,
        "child",
        Seq(JoinCondition("id", "parent_id")),
        Aggregate,
        aggregations = Seq(AggregationSpec("value", CollectList, "values"))
      )
    )

    result.select("values").as[Seq[String]].head() shouldBe Seq("a", "b", "c")
  }

  it should "sort collect_set output deterministically after removing duplicates" in {
    val parent = Seq(1).toDF("id")
    val child = Seq((1, "z"), (1, "a"), (1, "m"), (1, "a"), (1, "b"))
      .toDF("parent_id", "value")
      .repartition(4)
    import com.etl.framework.config.AggregationFunction._
    import com.etl.framework.config.JoinType._

    val result = executor.applyJoin(
      parent,
      child,
      JoinConfig(
        LeftOuter,
        "child",
        Seq(JoinCondition("id", "parent_id")),
        Aggregate,
        aggregations = Seq(AggregationSpec("value", CollectSet, "values"))
      )
    )

    result.select("values").as[Seq[String]].head() shouldBe Seq("a", "b", "m", "z")
  }

  it should "use SQL aggregation identities for parents without children" in {
    val parent = Seq(1, 2).toDF("id")
    val child = Seq((1, 10.0, "paid")).toDF("parent_id", "amount", "status")
    import com.etl.framework.config.AggregationFunction._
    import com.etl.framework.config.JoinType._

    val result = executor.applyJoin(
      parent,
      child,
      JoinConfig(
        LeftOuter,
        "child",
        Seq(JoinCondition("id", "parent_id")),
        Aggregate,
        aggregations = Seq(
          AggregationSpec("amount", Count, "item_count"),
          AggregationSpec("status", CollectList, "statuses"),
          AggregationSpec("status", CollectSet, "unique_statuses"),
          AggregationSpec("amount", Sum, "total_amount")
        )
      )
    )

    val unmatched = result.filter($"id" === 2).head()
    unmatched.isNullAt(unmatched.fieldIndex("item_count")) shouldBe false
    unmatched.getAs[Long]("item_count") shouldBe 0L
    unmatched.getAs[Seq[String]]("statuses") shouldBe empty
    unmatched.getAs[Seq[String]]("unique_statuses") shouldBe empty
    unmatched.isNullAt(unmatched.fieldIndex("total_amount")) shouldBe true
  }

  it should "reject non-orderable collection inputs before Spark analysis" in {
    val parent = Seq(1).toDF("id")
    val child = Seq((1, Map("status" -> "paid"))).toDF("parent_id", "attributes")
    import com.etl.framework.config.AggregationFunction._
    import com.etl.framework.config.JoinType._

    val error = intercept[ValidationConfigException] {
      executor.applyJoin(
        parent,
        child,
        JoinConfig(
          LeftOuter,
          "child",
          Seq(JoinCondition("id", "parent_id")),
          Aggregate,
          aggregations = Seq(AggregationSpec("attributes", CollectList, "attribute_history"))
        )
      )
    }

    error.getMessage should include("non-orderable type map<string,string>")
  }

  it should "reject non-orderable or ambiguous aggregate comparison keys" in {
    val parent = Seq(1).toDF("id")
    val child = Seq((1, "paid", 1, Map("priority" -> "high")))
      .toDF("parent_id", "status", "sequence", "attributes")
    import com.etl.framework.config.AggregationFunction._
    import com.etl.framework.config.JoinType._

    val nonOrderableValue = intercept[ValidationConfigException] {
      executor.applyJoin(
        parent,
        child,
        JoinConfig(
          LeftOuter,
          "child",
          Seq(JoinCondition("id", "parent_id")),
          Aggregate,
          aggregations = Seq(AggregationSpec("attributes", Min, "minimum_attributes"))
        )
      )
    }
    nonOrderableValue.getMessage should include("non-orderable type map<string,string>")

    val invalidOrder = intercept[ValidationConfigException] {
      executor.applyJoin(
        parent,
        child,
        JoinConfig(
          LeftOuter,
          "child",
          Seq(JoinCondition("id", "parent_id")),
          Aggregate,
          aggregations = Seq(
            AggregationSpec(
              "status",
              First,
              "first_status",
              orderBy = Seq("attributes", "sequence", "SEQUENCE")
            )
          )
        )
      )
    }
    invalidOrder.getMessage should include("orderBy columns must be unique")
    invalidOrder.getMessage should include("non-orderable type map<string,string>")
  }

  it should "reject implicit string coercion for numeric aggregations" in {
    val parent = Seq(1).toDF("id")
    val child = Seq((1, "10.50"), (1, "not-a-number")).toDF("parent_id", "amount")
    import com.etl.framework.config.AggregationFunction._
    import com.etl.framework.config.JoinType._

    val error = intercept[ValidationConfigException] {
      executor.applyJoin(
        parent,
        child,
        JoinConfig(
          LeftOuter,
          "child",
          Seq(JoinCondition("id", "parent_id")),
          Aggregate,
          aggregations = Seq(
            AggregationSpec("amount", Sum, "total_amount"),
            AggregationSpec("amount", Avg, "average_amount")
          )
        )
      )
    }

    error.getMessage should include("sum requires a numeric or ANSI interval column")
    error.getMessage should include("avg requires a numeric or ANSI interval column")
  }

  it should "accept decimal and ANSI interval additive aggregations" in {
    val parent = Seq(1).toDF("id")
    val child = spark.sql(
      """
        |SELECT 1 AS parent_id, CAST(10.50 AS DECIMAL(10, 2)) AS amount, INTERVAL '1' DAY AS duration
        |UNION ALL
        |SELECT 1 AS parent_id, CAST(4.50 AS DECIMAL(10, 2)) AS amount, INTERVAL '3' DAY AS duration
        |""".stripMargin
    )
    import com.etl.framework.config.AggregationFunction._
    import com.etl.framework.config.JoinType._

    val result = executor.applyJoin(
      parent,
      child,
      JoinConfig(
        LeftOuter,
        "child",
        Seq(JoinCondition("id", "parent_id")),
        Aggregate,
        aggregations = Seq(
          AggregationSpec("amount", Sum, "total_amount"),
          AggregationSpec("duration", Avg, "average_duration")
        )
      )
    )

    val row = result.head()
    row.getDecimal(row.fieldIndex("total_amount")) shouldBe new java.math.BigDecimal("15.00")
    row.isNullAt(row.fieldIndex("average_duration")) shouldBe false
  }

  it should "preserve child-only keys in right outer aggregate joins" in {
    val parent = Seq((1, "Alice")).toDF("id", "name")
    val child = Seq((2, 5.0)).toDF("parent_id", "amount")
    import com.etl.framework.config.AggregationFunction._
    import com.etl.framework.config.JoinType._

    val result = executor.applyJoin(
      parent,
      child,
      JoinConfig(
        RightOuter,
        "child",
        Seq(JoinCondition("id", "parent_id")),
        Aggregate,
        aggregations = Seq(AggregationSpec("amount", Sum, "total"))
      )
    )

    result.filter($"id".isNull).count() shouldBe 0
    result.select("id", "total").as[(Int, Double)].head() shouldBe ((2, 5.0))
  }

}
