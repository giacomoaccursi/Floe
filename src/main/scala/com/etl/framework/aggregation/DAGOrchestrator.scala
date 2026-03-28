package com.etl.framework.aggregation

import com.etl.framework.config.{AggregationConfig, GlobalConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

/** Coordinates DAG-based aggregation
  */
class DAGOrchestrator(
    dagConfig: AggregationConfig,
    globalConfig: GlobalConfig,
    autoDiscoverAdditionalTables: Boolean = false
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  // Initialize components
  private val tableDiscovery = new AdditionalTableDiscovery(globalConfig)
  private val graphBuilder = new DAGGraphBuilder(globalConfig)
  private val joinExecutor = new JoinStrategyExecutor()
  private val nodeProcessor = new DAGNodeProcessor(joinExecutor, globalConfig.iceberg.catalogName)
  private val pool = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors() * 2)
  private val dagExecutor = new DAGExecutor(nodeProcessor, ExecutionContext.fromExecutorService(pool))

  /** Builds execution plan from DAG configuration
    */
  def buildExecutionPlan(): DAGExecutionPlan = {
    logger.info("Building DAG execution plan")

    val configuredNodes = dagConfig.nodes
    logger.info(s"Loaded ${configuredNodes.size} configured DAG nodes")

    val additionalNodes = if (autoDiscoverAdditionalTables) {
      logger.info("Auto-discovery enabled, discovering additional tables")
      tableDiscovery.discoverAdditionalTables()
    } else {
      Seq.empty
    }
    logger.info(s"Discovered ${additionalNodes.size} additional table nodes")

    val allNodes = configuredNodes ++ additionalNodes
    logger.info(s"Total DAG nodes: ${allNodes.size}")

    graphBuilder.buildExecutionPlan(allNodes)
  }

  /** Executes the DAG
    */
  def execute(): DataFrame = {
    logger.info("Starting DAG execution")
    try {
      val plan = buildExecutionPlan()
      dagExecutor.execute(plan)
    } finally {
      pool.shutdown()
    }
  }
}
