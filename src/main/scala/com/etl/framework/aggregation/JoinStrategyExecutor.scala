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
      parent: DataFrame,
      child: DataFrame,
      joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(
      s"Applying ${joinConfig.strategy} join with type ${joinConfig.`type`}"
    )

    import com.etl.framework.config.JoinStrategy._
    joinConfig.strategy match {
      case Nest      => applyNestJoin(parent, child, joinConfig)
      case Flatten   => applyFlattenJoin(parent, child, joinConfig)
      case Aggregate => applyAggregateJoin(parent, child, joinConfig)
    }
  }

  /** Applies nest join strategy - creates nested array with related records
    */
  private def applyNestJoin(
      parent: DataFrame,
      child: DataFrame,
      joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(
      s"Applying nest join strategy, nesting as: ${joinConfig.nestAs.getOrElse("nested_records")}"
    )

    val nestFieldName = joinConfig.nestAs.getOrElse("nested_records")
    val childJoinKeys = joinConfig.conditions.map(_.right)
    val childStruct = struct(child.columns.map(child(_)): _*)

    val groupedChild = child
      .groupBy(childJoinKeys.map(child(_)): _*)
      .agg(collect_list(childStruct).as(nestFieldName))

    val joined = parent.join(
      groupedChild,
      joinConfig.conditions
        .map { cond =>
          parent(cond.left) === groupedChild(cond.right)
        }
        .reduce(_ && _),
      joinConfig.`type`.sparkType
    )

    // Use parent-qualified column references to avoid ambiguity when the join key
    // has the same name in both parent and groupedChild
    val columnsToKeep = parent.columns.map(parent(_)) :+
      coalesce(
        joined(nestFieldName),
        array().cast(joined.schema(nestFieldName).dataType)
      ).as(nestFieldName)

    joined.select(columnsToKeep: _*)
  }

  /** Applies flatten join strategy - flattens child fields into parent record
    */
  private def applyFlattenJoin(
      parent: DataFrame,
      child: DataFrame,
      joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(
      s"Applying flatten join strategy with type ${joinConfig.`type`.name}"
    )

    val parentColumns = parent.columns.toSet
    val childColumns = child.columns.toSet
    val childJoinKeys = joinConfig.conditions.map(_.right).toSet
    val childDataColumns = childColumns -- childJoinKeys

    val renamedChild = childDataColumns.foldLeft(child) { (df, childCol) =>
      if (parentColumns.contains(childCol)) {
        df.withColumnRenamed(childCol, s"child_$childCol")
      } else {
        df
      }
    }

    val joinExpr = joinConfig.conditions
      .map { cond => parent(cond.left) === renamedChild(cond.right) }
      .reduce(_ && _)

    val joined = parent.join(renamedChild, joinExpr, joinConfig.`type`.sparkType)

    // Drop child-side join key columns by DataFrame reference to resolve ambiguity
    // when the join key has the same name in both parent and child
    val withoutChildJoinKeys = joinConfig.conditions.foldLeft(joined) { (df, cond) =>
      df.drop(renamedChild(cond.right))
    }

    val finalChildColumns = childDataColumns.map { childCol =>
      if (parentColumns.contains(childCol)) s"child_$childCol" else childCol
    }

    val allColumns = parentColumns ++ finalChildColumns
    withoutChildJoinKeys.select(allColumns.toSeq.map(col): _*)
  }

  /** Applies aggregate join strategy - aggregates child records with functions
    */
  private def applyAggregateJoin(
      parent: DataFrame,
      child: DataFrame,
      joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(
      s"Applying aggregate join strategy with ${joinConfig.aggregations.size} aggregations"
    )

    val childJoinKeys = joinConfig.conditions.map(_.right)

    val aggExprs = joinConfig.aggregations.map { aggSpec =>
      import com.etl.framework.config.AggregationFunction._
      val aggFunc = aggSpec.function match {
        case Sum         => sum(child(aggSpec.column))
        case Count       => count(child(aggSpec.column))
        case Avg         => avg(child(aggSpec.column))
        case Min         => min(child(aggSpec.column))
        case Max         => max(child(aggSpec.column))
        case First       => first(child(aggSpec.column))
        case Last        => last(child(aggSpec.column))
        case CollectList => collect_list(child(aggSpec.column))
        case CollectSet  => collect_set(child(aggSpec.column))
      }
      aggFunc.as(aggSpec.alias)
    }

    if (aggExprs.isEmpty) {
      throw ValidationConfigException(
        s"Aggregate join requires at least one aggregation function"
      )
    }

    val aggregatedChild = child
      .groupBy(childJoinKeys.map(child(_)): _*)
      .agg(aggExprs.head, aggExprs.tail: _*)

    val result = parent.join(
      aggregatedChild,
      joinConfig.conditions
        .map { cond =>
          parent(cond.left) === aggregatedChild(cond.right)
        }
        .reduce(_ && _),
      joinConfig.`type`.sparkType
    )

    // Use parent-qualified references for parent columns and aggregatedChild references
    // for aggregation aliases to avoid ambiguity on shared join key names
    val parentSelects = parent.columns.map(parent(_))
    val aggSelects    = joinConfig.aggregations.map(spec => aggregatedChild(spec.alias))
    result.select((parentSelects ++ aggSelects): _*)
  }
}
