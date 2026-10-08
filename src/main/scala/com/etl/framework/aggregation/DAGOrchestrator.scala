package com.etl.framework.aggregation

import com.etl.framework.config.{AggregationConfig, GlobalConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

/** Entry point for DAG execution. Creates the execution plan and coordinates node processing with a bounded thread pool
  * for parallel execution.
  */
class DAGOrchestrator(
    dagConfig: AggregationConfig,
    globalConfig: GlobalConfig
)(implicit spark: SparkSession) {

  require(dagConfig.maxParallelNodes > 0, "maxParallelNodes must be greater than zero")

  private val logger = LoggerFactory.getLogger(getClass)
  private val graphBuilder = new DAGGraphBuilder(dagConfig.parallelNodes)

  def buildExecutionPlan(): DAGExecutionPlan = {
    logger.info("Building DAG execution plan")
    graphBuilder.buildExecutionPlan(dagConfig.nodes)
  }

  def execute(): DataFrame = {
    logger.info("Starting DAG execution")
    val workerCount = if (dagConfig.parallelNodes) dagConfig.maxParallelNodes else 1
    val pool = Executors.newFixedThreadPool(workerCount)
    try {
      val joinExecutor = new JoinStrategyExecutor()
      val nodeProcessor =
        new DAGNodeProcessor(joinExecutor, globalConfig.iceberg.catalogName, globalConfig.iceberg.namespace)
      val dagExecutor = new DAGExecutor(nodeProcessor, ExecutionContext.fromExecutorService(pool))
      val plan = buildExecutionPlan()
      dagExecutor.execute(plan)
    } finally {
      pool.shutdown()
    }
  }
}
