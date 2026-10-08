package com.etl.framework.aggregation

import com.etl.framework.config.{AggregationConfig, AggregationConfigValidator, GlobalConfig}
import com.etl.framework.exceptions.ValidationConfigException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/** Entry point for DAG planning. Creates one lazy Spark DataFrame in dependency order; the caller triggers physical
  * execution with an action and the Spark scheduler owns runtime parallelism.
  */
class DAGOrchestrator(
    dagConfig: AggregationConfig,
    globalConfig: GlobalConfig
)(implicit spark: SparkSession) {

  private val validationErrors = AggregationConfigValidator.validate(dagConfig)
  if (validationErrors.nonEmpty)
    throw ValidationConfigException(s"DAG '${dagConfig.name}': ${validationErrors.mkString("; ")}")

  private val logger = LoggerFactory.getLogger(getClass)
  private val graphBuilder = new DAGGraphBuilder()

  def buildExecutionPlan(): DAGExecutionPlan = {
    logger.info("Building DAG execution plan")
    graphBuilder.buildExecutionPlan(dagConfig.nodes)
  }

  def execute(): DataFrame = {
    logger.info("Preparing lazy DAG DataFrame")
    val joinExecutor = new JoinStrategyExecutor()
    val nodeProcessor =
      new DAGNodeProcessor(joinExecutor, globalConfig.iceberg.catalogName, globalConfig.iceberg.namespace)
    val dagExecutor = new DAGExecutor(nodeProcessor)
    val plan = buildExecutionPlan()
    dagExecutor.execute(plan)
  }
}
