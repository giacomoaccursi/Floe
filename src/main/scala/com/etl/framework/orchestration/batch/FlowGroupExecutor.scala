package com.etl.framework.orchestration.batch

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig}
import com.etl.framework.orchestration.flow.{FlowExecutor, FlowResult}
import com.etl.framework.orchestration.ExecutionGroup
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._

/**
 * Executes groups of flows sequentially or in parallel
 */
class FlowGroupExecutor(
  globalConfig: GlobalConfig,
  domainsConfig: Option[DomainsConfig]
)(implicit spark: SparkSession) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Executes a group of flows sequentially
   * Returns early if a flow fails and rollbackOnFailure is enabled
   */
  def executeSequential(
    group: ExecutionGroup,
    batchId: String,
    validatedFlows: Map[String, DataFrame]
  ): Seq[FlowResult] = {
    val results = mutable.ArrayBuffer[FlowResult]()
    
    for (flowConfig <- group.flows) {
      val result = executeFlow(flowConfig, batchId, validatedFlows)
      results.append(result)
      
      // Check if we should stop execution immediately
      // Only stop if rollbackOnFailure is enabled, otherwise continue with remaining flows
      if (globalConfig.processing.rollbackOnFailure) {
        if (!result.success || shouldStopExecution(result)) {
          // Stop executing remaining flows in this group
          return results.toSeq
        }
      }
    }
    
    results.toSeq
  }
  
  /**
   * Executes a group of flows in parallel
   */
  def executeParallel(
    group: ExecutionGroup,
    batchId: String,
    validatedFlows: Map[String, DataFrame]
  ): Seq[FlowResult] = {
    import ExecutionContext.Implicits.global
    
    val futures = group.flows.map { flowConfig =>
      Future {
        executeFlow(flowConfig, batchId, validatedFlows)
      }
    }
    
    val allResults = Future.sequence(futures)
    Await.result(allResults, Duration.Inf)
  }
  
  /**
   * Executes a single flow
   */
  private def executeFlow(
    flowConfig: FlowConfig,
    batchId: String,
    validatedFlows: Map[String, DataFrame]
  ): FlowResult = {
    logger.debug(s"Starting flow ${flowConfig.name} - batchId: $batchId")
    
    val executor = new FlowExecutor(flowConfig, globalConfig, validatedFlows, domainsConfig)
    executor.execute(batchId)
  }
  
  /**
   * Determines if execution should stop based on result
   */
  def shouldStopExecution(result: FlowResult): Boolean = {
    if (!result.success) {
      // Flow failed completely
      return true
    }
    
    // Check if rejection rate exceeds threshold
    if (globalConfig.processing.maxRejectionRate > 0 &&
        result.rejectionRate > globalConfig.processing.maxRejectionRate) {
      logger.warn(
        f"Flow ${result.flowName} rejection rate ${result.rejectionRate}%.2f%% " +
        f"exceeds threshold ${globalConfig.processing.maxRejectionRate}%.2f%% " +
        f"(${result.rejectedRecords} records)"
      )
      return globalConfig.processing.failOnValidationError
    }
    
    // Check if there are any rejected records and fail_on_validation_error is true
    if (globalConfig.processing.failOnValidationError && result.rejectedRecords > 0) {
      logger.warn(
        s"Flow ${result.flowName} has ${result.rejectedRecords} rejected records " +
        s"and failOnValidationError is enabled"
      )
      return true
    }
    
    false
  }
}
