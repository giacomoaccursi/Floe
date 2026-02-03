package com.etl.framework.aggregation

import com.etl.framework.config.JoinConfig
import com.etl.framework.exceptions.ValidationConfigException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/**
 * Executes different join strategies
 */
class JoinStrategyExecutor {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Applies join based on strategy
   */
  def applyJoin(
    parent: DataFrame,
    child: DataFrame,
    joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(s"Applying ${joinConfig.strategy} join with type ${joinConfig.`type`}")
    
    joinConfig.strategy match {
      case "nest" => applyNestJoin(parent, child, joinConfig)
      case "flatten" => applyFlattenJoin(parent, child, joinConfig)
      case "aggregate" => applyAggregateJoin(parent, child, joinConfig)
      case _ => throw new UnsupportedOperationException(s"Unsupported join strategy: ${joinConfig.strategy}")
    }
  }
  
  /**
   * Applies nest join strategy - creates nested array with related records
   */
  private def applyNestJoin(
    parent: DataFrame,
    child: DataFrame,
    joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(s"Applying nest join strategy, nesting as: ${joinConfig.nestAs.getOrElse("nested_records")}")
    
    val nestFieldName = joinConfig.nestAs.getOrElse("nested_records")
    val childJoinKeys = joinConfig.on.map(_.right)
    val childStruct = struct(child.columns.map(child(_)): _*)
    
    val groupedChild = child
      .groupBy(childJoinKeys.map(child(_)): _*)
      .agg(collect_list(childStruct).as(nestFieldName))
    
    val joined = parent.join(
      groupedChild,
      joinConfig.on.map { cond =>
        parent(cond.left) === groupedChild(cond.right)
      }.reduce(_ && _),
      joinConfig.`type`
    )
    
    val columnsToKeep = parent.columns.map(joined(_)) :+ 
      coalesce(joined(nestFieldName), array().cast(joined.schema(nestFieldName).dataType)).as(nestFieldName)
    
    joined.select(columnsToKeep: _*)
  }
  
  /**
   * Applies flatten join strategy - flattens child fields into parent record
   */
  private def applyFlattenJoin(
    parent: DataFrame,
    child: DataFrame,
    joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(s"Applying flatten join strategy with type ${joinConfig.`type`}")
    
    val parentColumns = parent.columns.toSet
    val childColumns = child.columns.toSet
    val childJoinKeys = joinConfig.on.map(_.right).toSet
    val childDataColumns = childColumns -- childJoinKeys
    
    val renamedChild = childDataColumns.foldLeft(child) { (df, childCol) =>
      if (parentColumns.contains(childCol)) {
        df.withColumnRenamed(childCol, s"child_$childCol")
      } else {
        df
      }
    }
    
    val joined = parent.join(
      renamedChild,
      joinConfig.on.map { cond =>
        parent(cond.left) === renamedChild(cond.right)
      }.reduce(_ && _),
      joinConfig.`type`
    )
    
    val finalChildColumns = childDataColumns.map { childCol =>
      if (parentColumns.contains(childCol)) s"child_$childCol" else childCol
    }
    
    val allColumns = parentColumns ++ finalChildColumns
    joined.select(allColumns.toSeq.map(col): _*)
  }
  
  /**
   * Applies aggregate join strategy - aggregates child records with functions
   */
  private def applyAggregateJoin(
    parent: DataFrame,
    child: DataFrame,
    joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(s"Applying aggregate join strategy with ${joinConfig.aggregations.size} aggregations")
    
    val childJoinKeys = joinConfig.on.map(_.right)
    
    val aggExprs = joinConfig.aggregations.map { aggSpec =>
      val aggFunc = aggSpec.function.toLowerCase match {
        case "sum" => sum(child(aggSpec.column))
        case "count" => count(child(aggSpec.column))
        case "avg" => avg(child(aggSpec.column))
        case "min" => min(child(aggSpec.column))
        case "max" => max(child(aggSpec.column))
        case other => throw new UnsupportedOperationException(s"Unsupported aggregation function: $other")
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
      joinConfig.on.map { cond =>
        parent(cond.left) === aggregatedChild(cond.right)
      }.reduce(_ && _),
      joinConfig.`type`
    )
    
    val columnsToKeep = parent.columns ++ joinConfig.aggregations.map(_.alias)
    result.select(columnsToKeep.map(col): _*)
  }
}
