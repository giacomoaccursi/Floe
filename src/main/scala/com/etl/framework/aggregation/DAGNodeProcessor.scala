package com.etl.framework.aggregation

import com.etl.framework.config.DAGNode
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/** Processes individual DAG nodes
  */
/** Processes a single DAG node: loads source data from Iceberg, applies filters and column selection, then executes
  * joins with dependent nodes using the configured strategy.
  */
class DAGNodeProcessor(joinExecutor: JoinStrategyExecutor, catalogName: String, namespace: String = "default")(implicit
    spark: SparkSession
) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Executes a single DAG node
    */
  def executeNode(node: DAGNode, nodeResults: Map[String, DataFrame]): DataFrame = {
    logger.info(s"Executing DAG node: ${node.id}")

    val tableName = node.sourceTable.getOrElse(s"$catalogName.$namespace.${node.sourceFlow}")
    val sourceData = spark.table(tableName)
    logger.debug(s"Source data loaded from table: $tableName")

    val filtered = applyFilters(sourceData, node.filters)
    val selected = applySelect(filtered, node.select)

    val result = if (node.joins.isEmpty) {
      selected
    } else {
      node.joins.foldLeft(selected) { (currentDf, joinConfig) =>
        val withData = nodeResults.getOrElse(
          joinConfig.`with`,
          throw new IllegalStateException(
            s"Node '${node.id}' requires '${joinConfig.`with`}' " +
              s"but it was not found in node results. " +
              s"Available nodes: ${nodeResults.keys.mkString(", ")}"
          )
        )
        joinExecutor.applyJoin(currentDf, withData, joinConfig)
      }
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
