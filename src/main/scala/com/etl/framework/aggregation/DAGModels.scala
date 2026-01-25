package com.etl.framework.aggregation

import com.etl.framework.config.DAGNode

/**
 * DAG execution plan
 */
case class DAGExecutionPlan(
  groups: Seq[DAGExecutionGroup],
  rootNode: String
)

/**
 * Group of DAG nodes that can be executed together
 */
case class DAGExecutionGroup(
  nodes: Seq[DAGNode],
  parallel: Boolean
)

/**
 * Exception thrown when circular dependency is detected in DAG
 */
class CircularDependencyException(message: String) extends Exception(message)
