package com.etl.framework.aggregation

import com.etl.framework.config.JoinConfig
import com.etl.framework.exceptions.ValidationConfigException
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.slf4j.LoggerFactory

import java.util.Locale

/** Executes different join strategies
  */
class JoinStrategyExecutor {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Applies join based on strategy
    */
  def applyJoin(
      left: DataFrame,
      right: DataFrame,
      joinConfig: JoinConfig
  ): DataFrame = {
    validateJoinInputs(left, right, joinConfig)
    logger.info(
      s"Applying ${joinConfig.strategy} join with type ${joinConfig.`type`}"
    )

    import com.etl.framework.config.JoinStrategy._
    joinConfig.strategy match {
      case Nest      => applyNestJoin(left, right, joinConfig)
      case Flatten   => applyFlattenJoin(left, right, joinConfig)
      case Aggregate => applyAggregateJoin(left, right, joinConfig)
    }
  }

  /** Applies nest join strategy - creates nested array with related records
    */
  private def applyNestJoin(
      left: DataFrame,
      right: DataFrame,
      joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(
      s"Applying nest join strategy, nesting as: ${joinConfig.nestAs.getOrElse("nested_records")}"
    )

    val nestFieldName = joinConfig.nestAs.getOrElse("nested_records")
    rejectOutputNameCollisions(left, Seq(nestFieldName), "nest")
    val rightJoinKeys = joinConfig.conditions.map(_.right)
    val payloadFieldName = "__floe_nested_payload"
    val orderFieldNames = joinConfig.orderBy.indices.map(index => s"__floe_nest_order_$index")
    val sortableEntry = struct(
      (joinConfig.orderBy.zip(orderFieldNames).map { case (columnName, fieldName) =>
        right(columnName).as(fieldName)
      } :+ struct(right.columns.map(right(_)): _*).as(payloadFieldName)): _*
    )
    val sortedEntries = array_sort(
      collect_list(sortableEntry),
      (leftEntry, rightEntry) => compareNestEntries(leftEntry, rightEntry, orderFieldNames)
    )
    val nestedPayloads = transform(sortedEntries, entry => entry.getField(payloadFieldName))

    val groupedRight = right
      .groupBy(rightJoinKeys.map(right(_)): _*)
      .agg(nestedPayloads.as(nestFieldName))

    val joined = left.join(
      groupedRight,
      joinConfig.conditions
        .map { cond =>
          left(cond.left) === groupedRight(cond.right)
        }
        .reduce(_ && _),
      joinConfig.`type`.sparkType
    )

    // Use parent-qualified column references to avoid ambiguity when the join key
    // has the same name in both parent and groupedRight
    val columnsToKeep = selectParentColumns(left, groupedRight, joinConfig) :+
      coalesce(
        joined(nestFieldName),
        array().cast(joined.schema(nestFieldName).dataType)
      ).as(nestFieldName)

    joined.select(columnsToKeep: _*)
  }

  /** Applies flatten join strategy - flattens child fields into parent record
    */
  private def applyFlattenJoin(
      left: DataFrame,
      right: DataFrame,
      joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(
      s"Applying flatten join strategy with type ${joinConfig.`type`.name}"
    )

    val caseSensitive = isCaseSensitive(left)
    val leftColumns = left.columns.map(normalizeColumnName(_, caseSensitive)).toSet
    val rightJoinKeys = joinConfig.conditions.map(_.right).toSet
    val rightDataColumns = right.columns.toSeq.filterNot(rightJoinKeys)

    val prefix = s"${joinConfig.`with`}_"
    val renamedRight = rightDataColumns.foldLeft(right) { (df, rightCol) =>
      if (leftColumns.contains(normalizeColumnName(rightCol, caseSensitive))) {
        df.withColumnRenamed(rightCol, s"$prefix$rightCol")
      } else {
        df
      }
    }

    val joinExpr = joinConfig.conditions
      .map { cond => left(cond.left) === renamedRight(cond.right) }
      .reduce(_ && _)

    val joined = left.join(renamedRight, joinExpr, joinConfig.`type`.sparkType)

    val finalRightColumns = rightDataColumns.map { rightCol =>
      val outputName =
        if (leftColumns.contains(normalizeColumnName(rightCol, caseSensitive))) s"$prefix$rightCol" else rightCol
      renamedRight(outputName)
    }

    // Preserve parent column order, append child columns after
    val allColumns = selectParentColumns(left, renamedRight, joinConfig) ++ finalRightColumns
    joined.select(allColumns: _*)
  }

  /** Applies aggregate join strategy - aggregates child records with functions
    */
  private def applyAggregateJoin(
      left: DataFrame,
      right: DataFrame,
      joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(
      s"Applying aggregate join strategy with ${joinConfig.aggregations.size} aggregations"
    )

    val rightJoinKeys = joinConfig.conditions.map(_.right)
    rejectOutputNameCollisions(left, joinConfig.aggregations.map(_.alias), "aggregate")

    val aggExprs = joinConfig.aggregations.map { aggSpec =>
      import com.etl.framework.config.AggregationFunction._
      val aggFunc = aggSpec.function match {
        case Sum         => sum(right(aggSpec.column))
        case Count       => count(right(aggSpec.column))
        case Avg         => avg(right(aggSpec.column))
        case Min         => min(right(aggSpec.column))
        case Max         => max(right(aggSpec.column))
        case First       => min_by(right(aggSpec.column), orderingColumn(right, aggSpec))
        case Last        => max_by(right(aggSpec.column), orderingColumn(right, aggSpec))
        case CollectList => sort_array(collect_list(right(aggSpec.column)))
        case CollectSet  => sort_array(collect_set(right(aggSpec.column)))
      }
      aggFunc.as(aggSpec.alias)
    }

    if (aggExprs.isEmpty) {
      throw ValidationConfigException(
        s"Aggregate join requires at least one aggregation function"
      )
    }

    val aggregatedRight = right
      .groupBy(rightJoinKeys.map(right(_)): _*)
      .agg(aggExprs.head, aggExprs.tail: _*)

    val result = left.join(
      aggregatedRight,
      joinConfig.conditions
        .map { cond =>
          left(cond.left) === aggregatedRight(cond.right)
        }
        .reduce(_ && _),
      joinConfig.`type`.sparkType
    )

    // Use parent-qualified references for parent columns and aggregatedRight references
    // for aggregation aliases to avoid ambiguity on shared join key names
    val leftSelects = selectParentColumns(left, aggregatedRight, joinConfig)
    val aggSelects = joinConfig.aggregations.map(spec => aggregatedRight(spec.alias))
    result.select((leftSelects ++ aggSelects): _*)
  }

  private def orderingColumn(right: DataFrame, spec: com.etl.framework.config.AggregationSpec) = {
    if (spec.orderBy.isEmpty)
      throw ValidationConfigException(s"Aggregation '${spec.alias}' with ${spec.function.name} requires orderBy")
    val caseSensitive = isCaseSensitive(right)
    val missing = spec.orderBy.filterNot(hasColumn(right, _, caseSensitive))
    if (missing.nonEmpty)
      throw ValidationConfigException(
        s"Aggregation '${spec.alias}' orderBy columns not found: ${missing.mkString(", ")}"
      )
    if (spec.orderBy.size == 1) right(spec.orderBy.head)
    else struct(spec.orderBy.map(right(_)): _*)
  }

  private def rejectOutputNameCollisions(left: DataFrame, outputNames: Seq[String], strategy: String): Unit = {
    val caseSensitive = isCaseSensitive(left)
    val duplicates = outputNames
      .groupBy(normalizeColumnName(_, caseSensitive))
      .collect { case (_, values) if values.size > 1 => values.head }
      .toSeq
    val parentColumns = left.columns.map(normalizeColumnName(_, caseSensitive)).toSet
    val parentCollisions = outputNames.distinct.filter { name =>
      parentColumns.contains(normalizeColumnName(name, caseSensitive))
    }
    val collisions = (duplicates ++ parentCollisions).distinct
    if (collisions.nonEmpty)
      throw ValidationConfigException(
        s"$strategy join output names collide with parent or each other: ${collisions.mkString(", ")}"
      )
  }

  private def validateJoinInputs(left: DataFrame, right: DataFrame, joinConfig: JoinConfig): Unit = {
    val errors = Seq.newBuilder[String]
    val caseSensitive = isCaseSensitive(left)

    if (joinConfig.conditions.isEmpty) errors += "join requires at least one condition"
    val conditionKeys = joinConfig.conditions.map { condition =>
      normalizeColumnName(condition.left.trim, caseSensitive) ->
        normalizeColumnName(condition.right.trim, caseSensitive)
    }
    if (conditionKeys.distinct.size != conditionKeys.size) errors += "join contains duplicate conditions"

    joinConfig.conditions.foreach { condition =>
      if (condition.left.trim.isEmpty) errors += "join condition left column must not be blank"
      else if (!hasColumn(left, condition.left, caseSensitive))
        errors += s"join condition left column '${condition.left}' was not found"
      if (condition.right.trim.isEmpty) errors += "join condition right column must not be blank"
      else if (!hasColumn(right, condition.right, caseSensitive))
        errors += s"join condition right column '${condition.right}' was not found"

      for {
        leftField <- findField(left, condition.left, caseSensitive)
        rightField <- findField(right, condition.right, caseSensitive)
        if leftField.dataType != rightField.dataType
      } errors +=
        s"join key types must match for '${condition.left}' and '${condition.right}': " +
          s"${leftField.dataType.catalogString} != ${rightField.dataType.catalogString}"
    }

    import com.etl.framework.config.JoinStrategy._
    joinConfig.strategy match {
      case Nest =>
        if (joinConfig.nestAs.exists(_.trim.isEmpty)) errors += "nest join nestAs must not be blank"
        if (joinConfig.nestAs.exists(value => value != value.trim))
          errors += "nest join nestAs must not have surrounding whitespace"
        if (joinConfig.aggregations.nonEmpty) errors += "nest join must not define aggregations"
        if (joinConfig.orderBy.isEmpty) errors += "nest join requires orderBy"
        validateRuntimeOrderBy(right, joinConfig.orderBy, caseSensitive, "nest join", errors)
      case Flatten =>
        if (joinConfig.nestAs.nonEmpty) errors += "flatten join must not define nestAs"
        if (joinConfig.aggregations.nonEmpty) errors += "flatten join must not define aggregations"
        if (joinConfig.orderBy.nonEmpty) errors += "flatten join must not define orderBy"
      case Aggregate =>
        if (joinConfig.nestAs.nonEmpty) errors += "aggregate join must not define nestAs"
        if (joinConfig.orderBy.nonEmpty) errors += "aggregate join must not define join-level orderBy"
        if (joinConfig.aggregations.isEmpty) errors += "aggregate join requires at least one aggregation function"
        joinConfig.aggregations.foreach { aggregation =>
          if (aggregation.column.trim.isEmpty) errors += "aggregation column must not be blank"
          else if (!hasColumn(right, aggregation.column, caseSensitive))
            errors += s"aggregation column '${aggregation.column}' was not found"
          if (aggregation.alias.trim.isEmpty) errors += "aggregation alias must not be blank"
          if (aggregation.orderBy.exists(_.trim.isEmpty)) errors += "aggregation orderBy columns must not be blank"
          aggregation.orderBy.filter(_.trim.nonEmpty).foreach { column =>
            if (!hasColumn(right, column, caseSensitive))
              errors += s"aggregation orderBy column '$column' was not found"
          }
          aggregation.function match {
            case com.etl.framework.config.AggregationFunction.First |
                com.etl.framework.config.AggregationFunction.Last =>
              if (aggregation.orderBy.isEmpty)
                errors += s"aggregation '${aggregation.alias}' with ${aggregation.function.name} requires orderBy"
            case _ if aggregation.orderBy.nonEmpty =>
              errors += s"aggregation '${aggregation.alias}' orderBy is only supported by first and last"
            case _ =>
          }
        }
    }

    val validationErrors = errors.result()
    if (validationErrors.nonEmpty) throw ValidationConfigException(validationErrors.distinct.mkString("; "))
  }

  private def validateRuntimeOrderBy(
      right: DataFrame,
      orderBy: Seq[String],
      caseSensitive: Boolean,
      label: String,
      errors: scala.collection.mutable.Builder[String, Seq[String]]
  ): Unit = {
    if (orderBy.exists(_.trim.isEmpty)) errors += s"$label orderBy columns must not be blank"
    if (orderBy.exists(column => column.trim.nonEmpty && column != column.trim))
      errors += s"$label orderBy columns must not have surrounding whitespace"

    val normalized = orderBy.map(column => normalizeColumnName(column.trim, caseSensitive))
    if (normalized.distinct.size != normalized.size)
      errors += s"$label orderBy columns must be unique"

    orderBy.filter(_.trim.nonEmpty).foreach { column =>
      findField(right, column, caseSensitive) match {
        case None => errors += s"$label orderBy column '$column' was not found"
        case Some(field) if !isOrderable(field.dataType) =>
          errors += s"$label orderBy column '$column' has non-orderable type ${field.dataType.catalogString}"
        case _ =>
      }
    }
  }

  private def isOrderable(dataType: DataType): Boolean = dataType match {
    case _: MapType                => false
    case ArrayType(elementType, _) => isOrderable(elementType)
    case StructType(fields)        => fields.forall(field => isOrderable(field.dataType))
    case _                         => true
  }

  private def compareNestEntries(left: Column, right: Column, orderFields: Seq[String]): Column =
    orderFields.reverse.foldLeft(lit(0)) { (nextComparison, fieldName) =>
      val leftValue = left.getField(fieldName)
      val rightValue = right.getField(fieldName)
      when(leftValue.isNull && rightValue.isNotNull, lit(-1))
        .when(leftValue.isNotNull && rightValue.isNull, lit(1))
        .when(leftValue < rightValue, lit(-1))
        .when(leftValue > rightValue, lit(1))
        .otherwise(nextComparison)
    }

  private def hasColumn(dataFrame: DataFrame, name: String, caseSensitive: Boolean): Boolean =
    findField(dataFrame, name, caseSensitive).nonEmpty

  private def findField(dataFrame: DataFrame, name: String, caseSensitive: Boolean) =
    dataFrame.schema.fields.find(field =>
      normalizeColumnName(field.name, caseSensitive) == normalizeColumnName(name, caseSensitive)
    )

  private def selectParentColumns(left: DataFrame, right: DataFrame, joinConfig: JoinConfig): Seq[Column] = {
    import com.etl.framework.config.JoinType.{FullOuter, RightOuter}
    val preserveRightKeys = joinConfig.`type` == RightOuter || joinConfig.`type` == FullOuter
    val caseSensitive = isCaseSensitive(left)

    left.columns.toSeq.map { leftColumn =>
      val matchingCondition = joinConfig.conditions.find { condition =>
        normalizeColumnName(condition.left, caseSensitive) == normalizeColumnName(leftColumn, caseSensitive)
      }
      if (preserveRightKeys)
        matchingCondition
          .map(condition => coalesce(left(leftColumn), right(condition.right)).as(leftColumn))
          .getOrElse(left(leftColumn))
      else left(leftColumn)
    }
  }

  private def isCaseSensitive(dataFrame: DataFrame): Boolean =
    dataFrame.sparkSession.conf.getOption("spark.sql.caseSensitive").exists(_.toBoolean)

  private def normalizeColumnName(name: String, caseSensitive: Boolean): String =
    if (caseSensitive) name else name.toLowerCase(Locale.ROOT)
}
