package com.etl.framework.orchestration.planning

import com.etl.framework.config.FlowConfig
import com.etl.framework.orchestration.ExecutionGroup
import com.etl.framework.util.TopologicalSorter

class DependencyGraphBuilder(flowConfigs: Seq[FlowConfig]) {

  private val flowConfigMap = flowConfigs.map(fc => fc.name -> fc).toMap

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

  def topologicalSort(dependencyGraph: Map[String, Set[String]]): Seq[String] =
    TopologicalSorter.sort(dependencyGraph, "flow dependency graph")

  def groupForParallelExecution(
      sortedFlows: Seq[String],
      dependencyGraph: Map[String, Set[String]],
      parallelEnabled: Boolean
  ): Seq[ExecutionGroup] =
    TopologicalSorter.group(sortedFlows, dependencyGraph, parallelEnabled).map { case (ids, parallel) =>
      ExecutionGroup(ids.flatMap(flowConfigMap.get), parallel)
    }
}
