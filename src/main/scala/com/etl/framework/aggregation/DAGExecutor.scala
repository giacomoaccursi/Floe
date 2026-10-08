package com.etl.framework.aggregation

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import scala.collection.mutable

/** Builds the lazy root DataFrame according to the execution plan. This class does not trigger Spark actions; physical
  * execution starts when the caller materializes the returned DataFrame.
  */
class DAGExecutor(nodeProcessor: DAGNodeProcessor) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Executes DAG nodes according to the execution plan
    */
  def execute(plan: DAGExecutionPlan): DataFrame = {
    logger.info("Preparing DAG logical plan")
    val nodeResults = mutable.Map[String, DataFrame]()

    plan.groups.foreach { group =>
      logger.info(s"Preparing DAG dependency group with ${group.nodes.size} nodes")

      val groupResults = executeGroupSequential(group, nodeResults.toMap)

      groupResults.foreach { case (nodeId, df) =>
        nodeResults(nodeId) = df
      }
    }

    val rootResult = nodeResults.getOrElse(
      plan.rootNode,
      throw new IllegalStateException(
        s"DAG execution error: root node '${plan.rootNode}' was not produced. " +
          s"Available nodes: ${nodeResults.keys.mkString(", ")}"
      )
    )
    logger.info(s"DAG logical plan prepared, returning root node: ${plan.rootNode}")
    rootResult
  }

  /** Executes a group of nodes sequentially
    */
  private def executeGroupSequential(
      group: DAGExecutionGroup,
      nodeResults: Map[String, DataFrame]
  ): Map[String, DataFrame] = {
    val results = mutable.Map[String, DataFrame]()

    group.nodes.foreach { node =>
      val result = nodeProcessor.executeNode(node, nodeResults ++ results)
      results(node.id) = result
    }

    results.toMap
  }
}
