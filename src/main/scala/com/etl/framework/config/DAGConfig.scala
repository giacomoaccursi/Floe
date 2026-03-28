package com.etl.framework.config

/** Configuration for DAG-based aggregation
  */
case class AggregationConfig(
    name: String,
    description: String = "",
    version: String = "",
    nodes: Seq[DAGNode]
)

/** DAG node configuration
  */
case class DAGNode(
    id: String,
    description: String = "",
    sourceFlow: String,
    dependencies: Seq[String],
    join: Option[JoinConfig] = None,
    select: Seq[String] = Seq.empty,
    filters: Seq[String] = Seq.empty,
    sourceTable: Option[String] = None
)

/** Join configuration
  */
case class JoinConfig(
    `type`: JoinType,
    parent: String,
    conditions: Seq[JoinCondition],
    strategy: JoinStrategy,
    nestAs: Option[String] = None,
    aggregations: Seq[AggregationSpec] = Seq.empty
)

/** Join condition
  */
case class JoinCondition(
    left: String,
    right: String
)

/** Aggregation specification
  */
case class AggregationSpec(
    column: String,
    function: AggregationFunction,
    alias: String
)
