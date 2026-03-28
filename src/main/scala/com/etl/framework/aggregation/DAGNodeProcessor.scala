package com.etl.framework.aggregation

import com.etl.framework.config.DAGNode
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/** Processes individual DAG nodes
  */
class DAGNodeProcessor(joinExecutor: JoinStrategyExecutor)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Executes a single DAG node
    */
  def executeNode(node: DAGNode, nodeResults: Map[String, DataFrame]): DataFrame = {
    logger.info(s"Executing DAG node: ${node.id}")

    val sourceData = node.sourceTable match {
      case Some(table) =>
        logger.debug(s"Loading source data from table: $table")
        spark.table(table)
      case None =>
        logger.debug(s"Loading source data from parquet: ${node.sourcePath}")
        spark.read.parquet(node.sourcePath)
    }
    logger.debug(s"Source data loaded for node: ${node.id}")

    val filtered = applyFilters(sourceData, node.filters)
    val selected = applySelect(filtered, node.select)

    val result = node.join match {
      case Some(joinConfig) =>
        val parentData = nodeResults.getOrElse(
          joinConfig.parent,
          throw new IllegalStateException(
            s"Node '${node.id}' requires parent '${joinConfig.parent}' " +
              s"but it was not found in node results. " +
              s"Available nodes: ${nodeResults.keys.mkString(", ")}"
          )
        )
        joinExecutor.applyJoin(parentData, selected, joinConfig)
      case None =>
        selected
    }

    logger.info(s"Node ${node.id} execution completed")
    result
  }

  /** Applies filters to a DataFrame
    */
  private def applyFilters(data: DataFrame, filters: Seq[String]): DataFrame = {
    if (filters.isEmpty) {
      data
    } else {
      filters.foldLeft(data) { (df, filter) =>
        logger.debug(s"Applying filter: $filter")
        df.filter(filter)
      }
    }
  }

  /** Applies column selection to a DataFrame
    */
  private def applySelect(data: DataFrame, columns: Seq[String]): DataFrame = {
    if (columns.isEmpty) {
      data
    } else {
      logger.debug(s"Selecting columns: ${columns.mkString(", ")}")
      data.select(columns.map(col): _*)
    }
  }
}
