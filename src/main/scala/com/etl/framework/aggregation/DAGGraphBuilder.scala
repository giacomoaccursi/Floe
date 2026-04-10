package com.etl.framework.aggregation

import com.etl.framework.config.DAGNode
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
    if (nodes.isEmpty)
      throw new IllegalStateException("DAG must contain at least one node")

    val nodeIds = nodes.map(_.id).toSet
    val duplicates = nodes.map(_.id).groupBy(identity).collect { case (id, ids) if ids.size > 1 => id }
    if (duplicates.nonEmpty)
      throw new IllegalStateException(s"DAG contains duplicate node IDs: ${duplicates.mkString(", ")}")

    nodes.foreach { node =>
      node.joins.foreach { joinConfig =>
        if (joinConfig.`with` == node.id)
          throw new IllegalStateException(s"Node '${node.id}' has a self-referential join")
        if (!nodeIds.contains(joinConfig.`with`))
          throw new IllegalStateException(
            s"Node '${node.id}' references join target '${joinConfig.`with`}' which does not exist"
          )
        if (joinConfig.conditions.isEmpty)
          throw new IllegalStateException(s"Node '${node.id}' has a join with no conditions")
      }
    }
  }

  private def findRootNode(nodes: Seq[DAGNode], graph: Map[String, Set[String]]): String = {
    val allDependencies = graph.values.flatten.toSet
    val rootNodes = nodes.map(_.id).filterNot(allDependencies.contains)
    rootNodes match {
      case Seq()       => throw new IllegalStateException("No root node found in DAG")
      case Seq(single) => single
      case multiple =>
        logger.warn(s"Multiple root nodes found: ${multiple.mkString(", ")}. Using first: ${multiple.head}")
        multiple.head
    }
  }
}
