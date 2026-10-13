package com.etl.framework.pipeline

import com.etl.framework.config.{FlowConfig, GlobalConfig}

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

  def requireValidFlowNames(flowConfigs: Seq[FlowConfig]): Unit = {
    val errors = validateFlowNames(flowConfigs)
    require(errors.isEmpty, errors.mkString("; "))
  }
}
