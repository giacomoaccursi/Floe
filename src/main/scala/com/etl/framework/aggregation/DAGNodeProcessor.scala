package com.etl.framework.aggregation

import com.etl.framework.config.DAGNode
import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.util.SqlIdentifier
import org.apache.iceberg.spark.{Spark3Util, SparkSchemaUtil}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.control.NonFatal

/** Prepares a single lazy DAG node DataFrame: references source data, applies filters and column selection, then builds
  * joins with dependent nodes using the configured strategy.
  */
class DAGNodeProcessor(
    joinExecutor: JoinStrategyExecutor,
    catalogName: String,
    namespace: String = "default",
    sourceLoader: Option[String => DataFrame] = None
)(implicit
    spark: SparkSession
) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val resolvedSources = mutable.Map.empty[String, DataFrame]

  private def sourceTableName(node: DAGNode): String =
    node.sourceTable.getOrElse(s"$catalogName.$namespace.${node.sourceFlow}")

  private def resolveSource(tableName: String): DataFrame =
    resolvedSources.getOrElseUpdate(
      tableName,
      sourceLoader.fold(loadPinnedSource(tableName))(loader => loader(tableName))
    )

  /** Captures each distinct source once before the logical DAG is assembled. Iceberg has no cross-table transaction,
    * but no individual source is allowed to drift after its capture or differ between nodes in this DAG.
    */
  private[aggregation] def prepareSources(nodes: Seq[DAGNode]): Unit =
    nodes.map(sourceTableName).distinct.sorted.foreach(tableName => resolveSource(tableName))

  /** Executes a single DAG node
    */
  def executeNode(node: DAGNode, nodeResults: Map[String, DataFrame]): DataFrame = {
    logger.info(s"Preparing DAG node: ${node.id}")

    val tableName = sourceTableName(node)
    val sourceData = resolveSource(tableName)

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

    logger.info(s"DAG node ${node.id} plan prepared")
    result
  }

  /** Resolves the current Iceberg state while constructing the DAG. The returned DataFrame remains pinned even though
    * Spark does not materialize it until the caller triggers an action.
    */
  private def loadPinnedSource(tableName: String): DataFrame = {
    val sqlTableName = SqlIdentifier.quoteMultipart(tableName)
    try {
      val table = Spark3Util.loadIcebergTable(spark, sqlTableName)
      table.refresh()
      Option(table.currentSnapshot()) match {
        case Some(snapshot) =>
          logger.debug(s"DAG source $tableName pinned to snapshot ${snapshot.snapshotId()}")
          spark.read.option("snapshot-id", snapshot.snapshotId()).table(sqlTableName)
        case None =>
          logger.debug(s"DAG source $tableName pinned as a snapshotless empty table")
          val schema = SparkSchemaUtil.convert(table.schema())
          spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      }
    } catch {
      case NonFatal(error) =>
        throw ValidationConfigException(
          s"DAG source '$tableName' must resolve to a readable Iceberg table so its state can be pinned",
          error
        )
    }
  }

  /** Applies filters to a DataFrame
    */
  private def applyFilters(data: DataFrame, filters: Seq[String]): DataFrame = {
    if (filters.isEmpty) {
      data
    } else {
      filters.foldLeft(data) { (df, filter) =>
        logger.debug("Applying configured DAG filter")
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
