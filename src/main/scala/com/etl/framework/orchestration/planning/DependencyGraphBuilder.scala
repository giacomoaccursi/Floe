package com.etl.framework.orchestration.planning

import com.etl.framework.config.FlowConfig
import com.etl.framework.orchestration.ExecutionGroup
import com.etl.framework.util.TopologicalSorter

/** Builds a dependency graph from flow FK references and dependsOn declarations. Delegates to TopologicalSorter for
  * sorting and grouping.
  */
class DependencyGraphBuilder(flowConfigs: Seq[FlowConfig]) {

  private val flowConfigMap = flowConfigs.map(fc => fc.name -> fc).toMap

  /** Builds a dependency graph from FK references and explicit dependsOn declarations. */
  def buildGraph(): Map[String, Set[String]] =
    TopologicalSorter.buildGraph[FlowConfig](
      flowConfigs,
      _.name,
      fc => {
        val fkDeps = fc.validation.foreignKeys.map(_.references.flow).toSet
        val explicitDeps = fc.dependsOn.toSet
        fkDeps ++ explicitDeps
      }
    )

  /** Sorts flows in dependency order. Throws CircularDependencyException if a cycle is detected. */
  def topologicalSort(dependencyGraph: Map[String, Set[String]]): Seq[String] =
    TopologicalSorter.sort(dependencyGraph, "flow dependency graph")

  /** Groups sorted flows into execution groups. Independent flows in the same group can run in parallel. */
  def groupForParallelExecution(
      sortedFlows: Seq[String],
      dependencyGraph: Map[String, Set[String]],
      parallelEnabled: Boolean
  ): Seq[ExecutionGroup] =
    TopologicalSorter.group(sortedFlows, dependencyGraph, parallelEnabled).map { case (ids, parallel) =>
      ExecutionGroup(ids.flatMap(flowConfigMap.get), parallel)
    }
}
