package com.etl.framework.pipeline

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.util.{SensitiveDataRedactor, TopologicalSorter}

import java.util.Locale

/** Validates pipeline-wide identities and derived dependencies without reading catalogs or source data. */
object PipelineStructureValidator {

  def validate(
      globalConfig: GlobalConfig,
      flowConfigs: Seq[FlowConfig],
      derivedTables: Seq[DerivedTableDefinition]
  ): Seq[String] = {
    val errors = Vector.newBuilder[String]
    errors ++= validateFlowNames(flowConfigs)
    errors ++= validateFlowDependencies(flowConfigs)

    globalConfig.processing.qualityMetricsTable.foreach { metricsTable =>
      flowConfigs.find(_.name.equalsIgnoreCase(metricsTable)).foreach { flow =>
        errors += s"qualityMetricsTable '$metricsTable' collides with flow '${flow.name}'"
      }
    }

    val managedNames =
      (flowConfigs.map(_.name) ++ globalConfig.processing.qualityMetricsTable.toSeq)
        .map(_.toLowerCase(Locale.ROOT))
        .toSet
    derivedTables.foreach { derivedTable =>
      if (managedNames.contains(derivedTable.name.toLowerCase(Locale.ROOT)))
        errors += s"Derived table '${derivedTable.name}' collides with a managed table"
    }

    errors ++= DerivedTableExecutor.validateDefinitions(derivedTables, flowConfigs.map(_.name).toSet)
    errors.result().distinct
  }

  def validateFlowNames(flowConfigs: Seq[FlowConfig]): Seq[String] =
    flowConfigs
      .groupBy(_.name.toLowerCase(Locale.ROOT))
      .values
      .filter(_.size > 1)
      .map(collisions => s"Flow names collide case-insensitively: ${collisions.map(_.name).sorted.mkString(", ")}")
      .toSeq
      .sorted

  def validateFlowDependencies(flowConfigs: Seq[FlowConfig]): Seq[String] = {
    val errors = Vector.newBuilder[String]
    errors ++= validateFlowReferences(flowConfigs)

    if (validateFlowNames(flowConfigs).isEmpty) {
      val graphValidation = scala.util.Try {
        val graph = TopologicalSorter.buildGraph[FlowConfig](
          flowConfigs,
          _.name,
          config => config.validation.foreignKeys.map(_.references.flow).toSet ++ config.dependsOn.toSet
        )
        TopologicalSorter.sort(graph, "flow dependency graph")
      }
      graphValidation.failed.foreach(error => errors += SensitiveDataRedactor.message(error))
    }

    errors.result().distinct
  }

  def validateFlowReferences(flowConfigs: Seq[FlowConfig]): Seq[String] = {
    val errors = Vector.newBuilder[String]
    val flowNames = flowConfigs.map(_.name).toSet
    val uniqueNames = validateFlowNames(flowConfigs).isEmpty
    val flowsByName =
      if (uniqueNames) flowConfigs.map(flow => flow.name -> flow).toMap else Map.empty[String, FlowConfig]

    flowConfigs.foreach { config =>
      config.validation.foreignKeys.foreach { foreignKey =>
        flowsByName.get(foreignKey.references.flow) match {
          case None if !flowNames.contains(foreignKey.references.flow) =>
            errors += s"Flow '${config.name}': FK references unknown flow '${foreignKey.references.flow}'"
          case Some(parent) =>
            val referencedColumns = parent.schema.columns.map(_.name).toSet
            if (referencedColumns.nonEmpty) {
              val missing = foreignKey.references.columns.filterNot(referencedColumns)
              if (missing.nonEmpty)
                errors +=
                  s"Flow '${config.name}': FK ${foreignKey.displayName} references columns not declared by flow '${foreignKey.references.flow}': ${missing.mkString(", ")}"
            }
          case None =>
        }
      }

      config.dependsOn.foreach { dependency =>
        if (!flowNames.contains(dependency))
          errors += s"Flow '${config.name}': dependsOn references unknown flow '$dependency'"
        else if (dependency == config.name)
          errors += s"Flow '${config.name}': dependsOn cannot reference itself"
      }
    }

    errors.result().distinct
  }

  def requireValidFlowNames(flowConfigs: Seq[FlowConfig]): Unit = {
    val errors = validateFlowNames(flowConfigs)
    require(errors.isEmpty, errors.mkString("; "))
  }

  def requireValidFlowReferences(flowConfigs: Seq[FlowConfig]): Unit = {
    val errors = validateFlowReferences(flowConfigs)
    require(errors.isEmpty, errors.mkString("; "))
  }
}
