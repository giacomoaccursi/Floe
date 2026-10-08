package com.etl.framework.config

import java.util.Locale

/** Structural validation shared by YAML loading and programmatic DAG construction. */
object AggregationConfigValidator {

  def validate(config: AggregationConfig): Seq[String] = {
    val errors = Seq.newBuilder[String]

    if (config.name.trim.isEmpty) errors += "name must not be blank"
    else if (config.name != config.name.trim) errors += "name must not have leading or trailing whitespace"
    errors ++= validateNodes(config.nodes)

    errors.result()
  }

  def validateNodes(nodes: Seq[DAGNode]): Seq[String] = {
    val errors = Seq.newBuilder[String]

    if (nodes.isEmpty) {
      errors += "nodes list must not be empty"
      return errors.result()
    }

    val nodeIds = nodes.map(_.id)
    val normalizedNodeIds = nodeIds.map(normalize)
    if (nodeIds.exists(_.trim.isEmpty)) errors += "node IDs must not be blank"
    if (nodeIds.exists(id => id != id.trim)) errors += "node IDs must not have leading or trailing whitespace"
    duplicateValues(normalizedNodeIds).foreach { duplicate =>
      val spellings = nodeIds.filter(id => normalize(id) == duplicate).distinct.sorted
      errors += s"node IDs must be unique case-insensitively: ${spellings.mkString(", ")}"
    }

    val declaredIds = nodeIds.toSet
    nodes.foreach { node =>
      validateNode(node, declaredIds).foreach(errors += _)
    }

    errors.result()
  }

  private def validateNode(node: DAGNode, declaredIds: Set[String]): Seq[String] = {
    val errors = Seq.newBuilder[String]
    val prefix = s"node '${node.id}'"
    val hasSourceFlow = node.sourceFlow.trim.nonEmpty
    val hasSourceTable = node.sourceTable.exists(_.trim.nonEmpty)

    if (hasSourceFlow == hasSourceTable)
      errors += s"$prefix must configure exactly one of sourceFlow or sourceTable"
    if (hasSourceFlow && node.sourceFlow != node.sourceFlow.trim)
      errors += s"$prefix sourceFlow must not have leading or trailing whitespace"
    node.sourceTable.foreach { table =>
      if (table.trim.isEmpty) errors += s"$prefix sourceTable must not be blank"
      else if (table != table.trim)
        errors += s"$prefix sourceTable must not have leading or trailing whitespace"
    }

    validateNonBlankUnique(node.select, s"$prefix select columns").foreach(errors += _)
    if (node.filters.exists(_.trim.isEmpty)) errors += s"$prefix filters must not be blank"

    node.joins.zipWithIndex.foreach { case (join, index) =>
      val joinPrefix = s"$prefix join ${index + 1}"
      if (join.`with`.trim.isEmpty) errors += s"$joinPrefix target must not be blank"
      else if (join.`with` != join.`with`.trim)
        errors += s"$joinPrefix target must not have leading or trailing whitespace"
      else if (join.`with` == node.id) errors += s"$prefix has a self-referential join"
      else if (!declaredIds.contains(join.`with`))
        errors += s"$prefix references missing join target '${join.`with`}'"

      if (join.conditions.isEmpty) errors += s"$joinPrefix must contain at least one condition"
      join.conditions.zipWithIndex.foreach { case (condition, conditionIndex) =>
        if (condition.left.trim.isEmpty)
          errors += s"$joinPrefix condition ${conditionIndex + 1} left column must not be blank"
        else if (condition.left != condition.left.trim)
          errors += s"$joinPrefix condition ${conditionIndex + 1} left column must not have surrounding whitespace"
        if (condition.right.trim.isEmpty)
          errors += s"$joinPrefix condition ${conditionIndex + 1} right column must not be blank"
        else if (condition.right != condition.right.trim)
          errors += s"$joinPrefix condition ${conditionIndex + 1} right column must not have surrounding whitespace"
      }
      val conditionKeys = join.conditions.map(condition => normalize(condition.left) -> normalize(condition.right))
      if (conditionKeys.distinct.size != conditionKeys.size)
        errors += s"$joinPrefix contains duplicate conditions"

      validateStrategy(join, joinPrefix).foreach(errors += _)
    }

    val outputNames = node.joins.flatMap { join =>
      join.strategy match {
        case JoinStrategy.Nest      => Seq(join.nestAs.getOrElse("nested_records"))
        case JoinStrategy.Aggregate => join.aggregations.map(_.alias)
        case JoinStrategy.Flatten   => Seq.empty
      }
    }
    if (duplicateValues(outputNames.map(normalize)).nonEmpty)
      errors += s"$prefix declares duplicate join output names"

    errors.result()
  }

  private def validateStrategy(join: JoinConfig, prefix: String): Seq[String] = {
    val errors = Seq.newBuilder[String]

    join.strategy match {
      case JoinStrategy.Nest =>
        if (join.nestAs.exists(_.trim.isEmpty)) errors += s"$prefix nestAs must not be blank"
        if (join.nestAs.exists(value => value != value.trim))
          errors += s"$prefix nestAs must not have surrounding whitespace"
        if (join.aggregations.nonEmpty) errors += s"$prefix nest strategy must not define aggregations"

      case JoinStrategy.Flatten =>
        if (join.nestAs.nonEmpty) errors += s"$prefix flatten strategy must not define nestAs"
        if (join.aggregations.nonEmpty) errors += s"$prefix flatten strategy must not define aggregations"

      case JoinStrategy.Aggregate =>
        if (join.nestAs.nonEmpty) errors += s"$prefix aggregate strategy must not define nestAs"
        if (join.aggregations.isEmpty) errors += s"$prefix aggregate strategy requires at least one aggregation"
        validateAggregations(join.aggregations, prefix).foreach(errors += _)
    }

    errors.result()
  }

  private def validateAggregations(aggregations: Seq[AggregationSpec], prefix: String): Seq[String] = {
    val errors = Seq.newBuilder[String]

    aggregations.zipWithIndex.foreach { case (aggregation, index) =>
      val aggregationPrefix = s"$prefix aggregation ${index + 1}"
      if (aggregation.column.trim.isEmpty) errors += s"$aggregationPrefix column must not be blank"
      else if (aggregation.column != aggregation.column.trim)
        errors += s"$aggregationPrefix column must not have surrounding whitespace"
      if (aggregation.alias.trim.isEmpty) errors += s"$aggregationPrefix alias must not be blank"
      else if (aggregation.alias != aggregation.alias.trim)
        errors += s"$aggregationPrefix alias must not have surrounding whitespace"
      validateNonBlankUnique(aggregation.orderBy, s"$aggregationPrefix orderBy columns").foreach(errors += _)

      aggregation.function match {
        case AggregationFunction.First | AggregationFunction.Last if aggregation.orderBy.isEmpty =>
          errors += s"$aggregationPrefix with ${aggregation.function.name} requires orderBy"
        case AggregationFunction.First | AggregationFunction.Last =>
        case _ if aggregation.orderBy.nonEmpty =>
          errors += s"$aggregationPrefix orderBy is only supported by first and last"
        case _ =>
      }
    }

    if (duplicateValues(aggregations.map(spec => normalize(spec.alias))).nonEmpty)
      errors += s"$prefix aggregation aliases must be unique case-insensitively"

    errors.result()
  }

  private def validateNonBlankUnique(values: Seq[String], label: String): Seq[String] = {
    val errors = Seq.newBuilder[String]
    if (values.exists(_.trim.isEmpty)) errors += s"$label must not be blank"
    if (values.exists(value => value.trim.nonEmpty && value != value.trim))
      errors += s"$label must not have surrounding whitespace"
    if (duplicateValues(values.map(normalize)).nonEmpty) errors += s"$label must be unique case-insensitively"
    errors.result()
  }

  private def duplicateValues(values: Seq[String]): Seq[String] =
    values.groupBy(identity).collect { case (value, occurrences) if occurrences.size > 1 => value }.toSeq.sorted

  private def normalize(value: String): String = value.trim.toLowerCase(Locale.ROOT)
}
