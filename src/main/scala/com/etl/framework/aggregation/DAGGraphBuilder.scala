package com.etl.framework.aggregation

import com.etl.framework.config.{DAGNode, GlobalConfig}
import com.etl.framework.exceptions.CircularDependencyException
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Builds and analyzes DAG dependency graphs
 */
class DAGGraphBuilder(globalConfig: GlobalConfig) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Builds execution plan from DAG nodes
   */
  def buildExecutionPlan(nodes: Seq[DAGNode]): DAGExecutionPlan = {
    logger.info(s"Building DAG execution plan for ${nodes.size} nodes")
    
    val dependencyGraph = buildDependencyGraph(nodes)
    val sortedNodeIds = topologicalSort(dependencyGraph, nodes)
    val groups = groupNodesForParallelExecution(sortedNodeIds, dependencyGraph, nodes)
    val rootNode = findRootNode(nodes, dependencyGraph)
    
    DAGExecutionPlan(groups, rootNode)
  }
  
  /**
   * Builds dependency graph from DAG nodes (package-private for testing)
   */
  private[aggregation] def buildDependencyGraph(nodes: Seq[DAGNode]): Map[String, Set[String]] = {
    val graph = mutable.Map[String, mutable.Set[String]]()
    
    nodes.foreach { node =>
      graph.getOrElseUpdate(node.id, mutable.Set.empty)
    }
    
    nodes.foreach { node =>
      node.dependencies.foreach { dep =>
        graph.getOrElseUpdate(node.id, mutable.Set.empty).add(dep)
      }
    }
    
    graph.map { case (k, v) => k -> v.toSet }.toMap
  }
  
  /**
   * Performs topological sort on the dependency graph
   */
  private def topologicalSort(
    dependencyGraph: Map[String, Set[String]],
    nodes: Seq[DAGNode]
  ): Seq[String] = {
    val sorted = mutable.ArrayBuffer[String]()
    val visited = mutable.Set[String]()
    val visiting = mutable.Set[String]()
    val path = mutable.ArrayBuffer[String]()
    
    def visit(nodeId: String): Unit = {
      if (visiting.contains(nodeId)) {
        // Found a cycle - construct the cycle path
        val cycleStart = path.indexOf(nodeId)
        val cycle = path.slice(cycleStart, path.length).toSeq :+ nodeId
        throw CircularDependencyException(
          graphType = "DAG",
          cycle = cycle
        )
      }
      
      if (!visited.contains(nodeId)) {
        visiting.add(nodeId)
        path.append(nodeId)
        
        dependencyGraph.getOrElse(nodeId, Set.empty).foreach { dependency =>
          visit(dependency)
        }
        
        path.remove(path.length - 1)
        visiting.remove(nodeId)
        visited.add(nodeId)
        sorted.append(nodeId)
      }
    }
    
    dependencyGraph.keys.foreach(visit)
    
    logger.info(s"Topological sort completed: ${sorted.mkString(" -> ")}")
    sorted.toSeq
  }
  
  /**
   * Groups nodes into execution groups for parallel execution
   */
  private def groupNodesForParallelExecution(
    sortedNodeIds: Seq[String],
    dependencyGraph: Map[String, Set[String]],
    nodes: Seq[DAGNode]
  ): Seq[DAGExecutionGroup] = {
    val groups = mutable.ArrayBuffer[DAGExecutionGroup]()
    val completed = mutable.Set[String]()
    val remaining = mutable.Queue(sortedNodeIds: _*)
    
    while (remaining.nonEmpty) {
      val currentGroup = mutable.ArrayBuffer[String]()
      val nextRemaining = mutable.Queue[String]()
      
      while (remaining.nonEmpty) {
        val nodeId = remaining.dequeue()
        val dependencies = dependencyGraph.getOrElse(nodeId, Set.empty)
        
        if (dependencies.subsetOf(completed)) {
          currentGroup.append(nodeId)
        } else {
          nextRemaining.enqueue(nodeId)
        }
      }
      
      if (currentGroup.nonEmpty) {
        val nodesInGroup = currentGroup.flatMap { nodeId =>
          nodes.find(_.id == nodeId)
        }
        
        val parallel = globalConfig.performance.parallelNodes && currentGroup.size > 1
        
        groups.append(DAGExecutionGroup(nodesInGroup.toSeq, parallel))
        logger.info(s"Created execution group with ${nodesInGroup.size} nodes (parallel=$parallel)")
        
        currentGroup.foreach(completed.add)
      }
      
      remaining.clear()
      remaining ++= nextRemaining
    }
    
    groups.toSeq
  }
  
  /**
   * Finds the root node (node with no dependents)
   */
  private def findRootNode(nodes: Seq[DAGNode], dependencyGraph: Map[String, Set[String]]): String = {
    val allDependencies = dependencyGraph.values.flatten.toSet
    val rootNodes = nodes.map(_.id).filterNot(allDependencies.contains)
    
    if (rootNodes.isEmpty) {
      throw new IllegalStateException("No root node found in DAG - all nodes are dependencies of others")
    }
    
    if (rootNodes.size > 1) {
      logger.warn(s"Multiple root nodes found: ${rootNodes.mkString(", ")}. Using first one: ${rootNodes.head}")
    }
    
    rootNodes.head
  }
}
