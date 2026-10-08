package com.etl.framework.aggregation

import com.etl.framework.config.DAGNode

/** DAG execution plan
  */
case class DAGExecutionPlan(
    groups: Seq[DAGExecutionGroup],
    rootNode: String
)

/** Dependency-level group of DAG nodes
  */
case class DAGExecutionGroup(nodes: Seq[DAGNode])
