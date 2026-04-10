package com.etl.framework.aggregation

import com.etl.framework.config.JoinConfig
import com.etl.framework.exceptions.ValidationConfigException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

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
    val rightJoinKeys = joinConfig.conditions.map(_.right)
    val rightStruct = struct(right.columns.map(right(_)): _*)

    val groupedRight = right
      .groupBy(rightJoinKeys.map(right(_)): _*)
      .agg(collect_list(rightStruct).as(nestFieldName))

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
    val columnsToKeep = left.columns.map(left(_)) :+
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

    val leftColumns = left.columns.toSet
    val rightColumns = right.columns.toSet
    val rightJoinKeys = joinConfig.conditions.map(_.right).toSet
    val rightDataColumns = rightColumns -- rightJoinKeys

    val prefix = s"${joinConfig.`with`}_"
    val renamedRight = rightDataColumns.foldLeft(right) { (df, rightCol) =>
      if (leftColumns.contains(rightCol)) {
        df.withColumnRenamed(rightCol, s"$prefix$rightCol")
      } else {
        df
      }
    }

    val joinExpr = joinConfig.conditions
      .map { cond => left(cond.left) === renamedRight(cond.right) }
      .reduce(_ && _)

    val joined = left.join(renamedRight, joinExpr, joinConfig.`type`.sparkType)

    // Drop child-side join key columns by DataFrame reference to resolve ambiguity
    // when the join key has the same name in both parent and right
    val withoutRightJoinKeys = joinConfig.conditions.foldLeft(joined) { (df, cond) =>
      df.drop(renamedRight(cond.right))
    }

    val finalRightColumns = rightDataColumns.map { rightCol =>
      if (leftColumns.contains(rightCol)) s"$prefix$rightCol" else rightCol
    }

    val allColumns = leftColumns ++ finalRightColumns
    withoutRightJoinKeys.select(allColumns.toSeq.map(col): _*)
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

    val aggExprs = joinConfig.aggregations.map { aggSpec =>
      import com.etl.framework.config.AggregationFunction._
      val aggFunc = aggSpec.function match {
        case Sum         => sum(right(aggSpec.column))
        case Count       => count(right(aggSpec.column))
        case Avg         => avg(right(aggSpec.column))
        case Min         => min(right(aggSpec.column))
        case Max         => max(right(aggSpec.column))
        case First       => first(right(aggSpec.column))
        case Last        => last(right(aggSpec.column))
        case CollectList => collect_list(right(aggSpec.column))
        case CollectSet  => collect_set(right(aggSpec.column))
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
    val leftSelects = left.columns.map(left(_))
    val aggSelects = joinConfig.aggregations.map(spec => aggregatedRight(spec.alias))
    result.select((leftSelects ++ aggSelects): _*)
  }
}
