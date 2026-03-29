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
    globalConfig: GlobalConfig
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  private val graphBuilder = new DAGGraphBuilder(dagConfig.parallelNodes)
  private val joinExecutor = new JoinStrategyExecutor()
  private val nodeProcessor = new DAGNodeProcessor(joinExecutor, globalConfig.iceberg.catalogName)
  private val pool = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors() * 2)
  private val dagExecutor = new DAGExecutor(nodeProcessor, ExecutionContext.fromExecutorService(pool))

  /** Builds execution plan from DAG configuration
    */
  def buildExecutionPlan(): DAGExecutionPlan = {
    logger.info("Building DAG execution plan")
    val nodes = dagConfig.nodes
    logger.info(s"Total DAG nodes: ${nodes.size}")
    graphBuilder.buildExecutionPlan(nodes)
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
