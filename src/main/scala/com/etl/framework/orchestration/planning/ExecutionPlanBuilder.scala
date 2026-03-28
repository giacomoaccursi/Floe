package com.etl.framework.orchestration.planning

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.orchestration.ExecutionPlan
import org.slf4j.LoggerFactory

/** Builds execution plans based on flow dependencies. Extracted from FlowOrchestrator to follow Single Responsibility
  * Principle.
  */
class ExecutionPlanBuilder(
    flowConfigs: Seq[FlowConfig],
    globalConfig: GlobalConfig
) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val dependencyBuilder = new DependencyGraphBuilder(flowConfigs)

  /** Builds execution plan based on FK dependencies. Creates ordered groups of flows that can be executed together.
    */
  def build(): ExecutionPlan = {
    logger.info(s"Building execution plan for ${flowConfigs.size} flows")

    validateForeignKeyReferences()

    val dependencyGraph = dependencyBuilder.buildGraph()
    val sortedFlows = dependencyBuilder.topologicalSort(dependencyGraph)
    val groups = dependencyBuilder.groupForParallelExecution(
      sortedFlows,
      dependencyGraph,
      globalConfig.performance.parallelFlows
    )

    logger.info(s"Execution plan created with ${groups.size} groups (${flowConfigs.size} flows)")

    ExecutionPlan(groups)
  }

  private def validateForeignKeyReferences(): Unit = {
    val flowNames = flowConfigs.map(_.name).toSet
    flowConfigs.foreach { fc =>
      fc.validation.foreignKeys.foreach { fk =>
        if (!flowNames.contains(fk.references.flow)) {
          throw new IllegalArgumentException(
            s"Flow '${fc.name}': FK '${fk.name}' references unknown flow '${fk.references.flow}'"
          )
        }
      }
    }
  }
}

object ExecutionPlanBuilder {
  def apply(
      flowConfigs: Seq[FlowConfig],
      globalConfig: GlobalConfig
  ): ExecutionPlanBuilder = new ExecutionPlanBuilder(flowConfigs, globalConfig)
}
