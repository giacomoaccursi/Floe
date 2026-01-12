package com.etl.framework.config

/**
 * Configuration for DAG-based aggregation
 */
case class AggregationConfig(
  name: String,
  description: String,
  version: String,
  batchModel: ModelConfig,
  finalModel: ModelConfig,
  output: DAGOutputConfig,
  nodes: Seq[DAGNode]
)

/**
 * Model configuration
 */
case class ModelConfig(
  `class`: String,
  mappingFile: Option[String] = None,
  mapperClass: Option[String] = None
)

/**
 * DAG output configuration
 */
case class DAGOutputConfig(
  batch: OutputConfig,
  `final`: OutputConfig
)

/**
 * DAG node configuration
 */
case class DAGNode(
  id: String,
  description: String,
  sourceFlow: String,
  sourcePath: String,
  dependencies: Seq[String],
  join: Option[JoinConfig] = None,
  select: Seq[String] = Seq.empty,
  filters: Seq[String] = Seq.empty
)

/**
 * Join configuration
 */
case class JoinConfig(
  `type`: String,  // "inner" | "left_outer" | "right_outer" | "full_outer"
  parent: String,
  on: Seq[JoinCondition],
  strategy: String,  // "nest" | "flatten" | "aggregate"
  nestAs: Option[String] = None,
  aggregations: Seq[AggregationSpec] = Seq.empty
)

/**
 * Join condition
 */
case class JoinCondition(
  left: String,
  right: String
)

/**
 * Aggregation specification
 */
case class AggregationSpec(
  column: String,
  function: String,  // "sum" | "count" | "avg" | "min" | "max"
  alias: String
)
