package com.etl.framework.aggregation

import com.etl.framework.config.{AggregationConfigValidator, DAGNode}
import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.util.TopologicalSorter
import org.slf4j.LoggerFactory

/** Builds the DAG execution plan: validates nodes, resolves dependencies via topological sort, groups independent nodes
  * for parallel execution, and identifies the root node.
  */
class DAGGraphBuilder(parallelNodes: Boolean) {

  private val logger = LoggerFactory.getLogger(getClass)

  def buildExecutionPlan(nodes: Seq[DAGNode]): DAGExecutionPlan = {
    logger.info(s"Building DAG execution plan for ${nodes.size} nodes")
    validateNodes(nodes)

    val nodeMap = nodes.map(n => n.id -> n).toMap
    val graph = TopologicalSorter.buildGraph[DAGNode](nodes, _.id, _.joins.map(_.`with`).toSet)
    val sorted = TopologicalSorter.sort(graph, "DAG")
    val groups = TopologicalSorter.group(sorted, graph, parallelNodes).map { case (ids, parallel) =>
      DAGExecutionGroup(ids.flatMap(nodeMap.get), parallel)
    }
    val rootNode = findRootNode(nodes, graph)

    DAGExecutionPlan(groups, rootNode)
  }

  private def validateNodes(nodes: Seq[DAGNode]): Unit = {
    val structuralErrors = AggregationConfigValidator.validateNodes(nodes)
    if (structuralErrors.nonEmpty)
      throw ValidationConfigException(structuralErrors.mkString("; "))
  }

  private def findRootNode(nodes: Seq[DAGNode], graph: Map[String, Set[String]]): String = {
    val allDependencies = graph.values.flatten.toSet
    val rootNodes = nodes.map(_.id).filterNot(allDependencies.contains)
    rootNodes match {
      case Seq()       => throw new IllegalStateException("No root node found in DAG")
      case Seq(single) => single
      case multiple =>
        throw new IllegalStateException(
          s"Multiple root nodes found in DAG: ${multiple.mkString(", ")}. " +
            s"A DAG must have exactly one root node. Restructure the DAG so that a single node " +
            s"joins all independent branches, or split into separate DAGs."
        )
    }
  }
}
