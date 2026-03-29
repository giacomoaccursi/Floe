package com.etl.framework.orchestration.planning

import com.etl.framework.config.FlowConfig
import com.etl.framework.exceptions.CircularDependencyException
import com.etl.framework.orchestration.ExecutionGroup

import scala.collection.mutable

/** Builds and analyzes dependency graphs from flow configurations
  */
class DependencyGraphBuilder(flowConfigs: Seq[FlowConfig]) {

  /** Builds dependency graph from Foreign Key relationships Returns a map: flow_name -> Set(dependent_flow_names)
    */
  def buildGraph(): Map[String, Set[String]] = {
    val graph = mutable.Map[String, mutable.Set[String]]()

    // Initialize all flows in the graph
    flowConfigs.foreach { flow =>
      graph.getOrElseUpdate(flow.name, mutable.Set.empty)
    }

    // Add edges based on Foreign Key dependencies
    flowConfigs.foreach { flow =>
      flow.validation.foreignKeys.foreach { fk =>
        val referencedFlow = fk.references.flow
        graph.getOrElseUpdate(flow.name, mutable.Set.empty).add(referencedFlow)
      }

      // Add edges based on explicit dependsOn
      flow.dependsOn.foreach { dep =>
        graph.getOrElseUpdate(flow.name, mutable.Set.empty).add(dep)
      }
    }

    // Convert to immutable map
    graph.map { case (k, v) => k -> v.toSet }.toMap
  }

  /** Performs topological sort on the dependency graph Returns flows in execution order Throws exception if circular
    * dependency detected
    */
  def topologicalSort(dependencyGraph: Map[String, Set[String]]): Seq[String] = {
    val sorted = mutable.ArrayBuffer[String]()
    val visited = mutable.Set[String]()
    val visiting = mutable.Set[String]()
    val path = mutable.ArrayBuffer[String]()

    def visit(flowName: String): Unit = {
      if (visiting.contains(flowName)) {
        // Found a cycle - construct the cycle path
        val cycleStart = path.indexOf(flowName)
        val cycle = path.slice(cycleStart, path.length).toSeq :+ flowName
        throw CircularDependencyException(
          graphType = "flow dependency graph",
          cycle = cycle
        )
      }

      if (!visited.contains(flowName)) {
        visiting.add(flowName)
        path.append(flowName)

        // Visit all dependencies first
        dependencyGraph.getOrElse(flowName, Set.empty).foreach { dependency =>
          visit(dependency)
        }

        path.remove(path.length - 1)
        visiting.remove(flowName)
        visited.add(flowName)
        sorted.append(flowName)
      }
    }

    // Visit all flows
    dependencyGraph.keys.foreach(visit)

    sorted.toSeq
  }

  /** Groups flows into execution groups for parallel execution Flows in the same group have no dependencies on each
    * other
    */
  def groupForParallelExecution(
      sortedFlows: Seq[String],
      dependencyGraph: Map[String, Set[String]],
      parallelEnabled: Boolean
  ): Seq[ExecutionGroup] = {
    val groups = mutable.ArrayBuffer[ExecutionGroup]()
    val completed = mutable.Set[String]()
    val remaining = mutable.Queue(sortedFlows: _*)

    while (remaining.nonEmpty) {
      val currentGroup = mutable.ArrayBuffer[String]()
      val nextRemaining = mutable.Queue[String]()

      // Find all flows whose dependencies are satisfied
      while (remaining.nonEmpty) {
        val flow = remaining.dequeue()
        val dependencies = dependencyGraph.getOrElse(flow, Set.empty)

        if (dependencies.subsetOf(completed)) {
          // All dependencies satisfied, can execute in this group
          currentGroup.append(flow)
        } else {
          // Dependencies not satisfied, defer to next iteration
          nextRemaining.enqueue(flow)
        }
      }

      if (currentGroup.nonEmpty) {
        // Get FlowConfig objects for this group
        val flowConfigsInGroup = currentGroup.flatMap { flowName =>
          flowConfigs.find(_.name == flowName)
        }

        // Determine if parallel execution is enabled
        val parallel = parallelEnabled && currentGroup.size > 1

        groups.append(ExecutionGroup(flowConfigsInGroup.toSeq, parallel))

        // Mark flows in this group as completed AFTER the group is formed
        currentGroup.foreach(completed.add)
      }

      remaining.clear()
      remaining ++= nextRemaining
    }

    groups
  }
}
