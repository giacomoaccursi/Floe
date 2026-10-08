package com.etl.framework.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AggregationConfigValidatorTest extends AnyFlatSpec with Matchers {

  private def leaf(id: String): DAGNode = DAGNode(id = id, sourceFlow = s"flow_$id")

  private def join(strategy: JoinStrategy, aggregations: Seq[AggregationSpec] = Seq.empty): JoinConfig =
    JoinConfig(
      `type` = JoinType.LeftOuter,
      `with` = "child",
      conditions = Seq(JoinCondition("id", "parent_id")),
      strategy = strategy,
      aggregations = aggregations
    )

  "AggregationConfigValidator" should "accept a complete valid DAG" in {
    val total = AggregationSpec("amount", AggregationFunction.Sum, "total_amount")
    val root = DAGNode(id = "root", sourceFlow = "root", joins = Seq(join(JoinStrategy.Aggregate, Seq(total))))

    AggregationConfigValidator.validate(AggregationConfig("valid", nodes = Seq(leaf("child"), root))) shouldBe empty
  }

  it should "reject ambiguous sources and case-insensitive node ID collisions" in {
    val config = AggregationConfig(
      "invalid",
      nodes = Seq(
        DAGNode("Customer", sourceFlow = "customer", sourceTable = Some("catalog.ns.customer")),
        DAGNode("customer", sourceFlow = "duplicate")
      )
    )

    val errors = AggregationConfigValidator.validate(config)
    errors.mkString("; ") should include("exactly one of sourceFlow or sourceTable")
    errors.mkString("; ") should include("unique case-insensitively")
  }

  it should "reject blank and duplicate join conditions" in {
    val invalidJoin = join(JoinStrategy.Flatten).copy(
      conditions = Seq(JoinCondition("", "parent_id"), JoinCondition("", "parent_id"))
    )
    val root = DAGNode(id = "root", sourceFlow = "root", joins = Seq(invalidJoin))

    val errors = AggregationConfigValidator.validate(AggregationConfig("invalid", nodes = Seq(leaf("child"), root)))
    errors.mkString("; ") should include("left column must not be blank")
    errors.mkString("; ") should include("duplicate conditions")
  }

  it should "reject surrounding whitespace in column identifiers" in {
    val invalidJoin = join(
      JoinStrategy.Aggregate,
      Seq(AggregationSpec(" amount", AggregationFunction.First, "chosen ", orderBy = Seq(" sequence")))
    ).copy(conditions = Seq(JoinCondition("id ", " parent_id")))
    val root = DAGNode(id = "root", sourceFlow = "root", select = Seq(" id"), joins = Seq(invalidJoin))

    val errors = AggregationConfigValidator.validate(AggregationConfig("invalid", nodes = Seq(leaf("child"), root)))
    errors.count(_.contains("surrounding whitespace")) should be >= 6
  }

  it should "reject fields that the selected join strategy would ignore" in {
    val invalidFlatten = join(JoinStrategy.Flatten, Seq(AggregationSpec("amount", AggregationFunction.Sum, "total")))
      .copy(nestAs = Some("nested"))
    val root = DAGNode(id = "root", sourceFlow = "root", joins = Seq(invalidFlatten))

    val errors = AggregationConfigValidator.validate(AggregationConfig("invalid", nodes = Seq(leaf("child"), root)))
    errors.mkString("; ") should include("flatten strategy must not define nestAs")
    errors.mkString("; ") should include("flatten strategy must not define aggregations")
  }

  it should "enforce ordering semantics and unique aggregate aliases" in {
    val specs = Seq(
      AggregationSpec("value", AggregationFunction.First, "chosen"),
      AggregationSpec("value", AggregationFunction.Sum, "CHOSEN", orderBy = Seq("sequence"))
    )
    val root = DAGNode(id = "root", sourceFlow = "root", joins = Seq(join(JoinStrategy.Aggregate, specs)))

    val errors = AggregationConfigValidator.validate(AggregationConfig("invalid", nodes = Seq(leaf("child"), root)))
    errors.mkString("; ") should include("with first requires orderBy")
    errors.mkString("; ") should include("orderBy is only supported by first and last")
    errors.mkString("; ") should include("aliases must be unique case-insensitively")
  }
}
