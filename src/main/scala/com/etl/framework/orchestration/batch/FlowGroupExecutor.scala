package com.etl.framework.orchestration.batch

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig}
import com.etl.framework.orchestration.flow.{FlowExecutor, FlowResult}
import com.etl.framework.util.RetryExecutor
import com.etl.framework.validation.Validator
import com.etl.framework.orchestration.ExecutionGroup
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._

/** Executes groups of flows sequentially or in parallel
  */
class FlowGroupExecutor(
    globalConfig: GlobalConfig,
    domainsConfig: Option[DomainsConfig],
    parallelEc: ExecutionContext,
    customValidators: Map[String, () => Validator] = Map.empty
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val MaxParallelTimeout: FiniteDuration = 2.hours

  /** Executes a group of flows sequentially
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

      // Check if we should stop execution immediately on failure
      if (!result.success || shouldStopExecution(result, flowConfig)) {
        // Stop executing remaining flows in this group
        return results.toSeq
      }
    }

    results.toSeq
  }

  /** Executes a group of flows in parallel
    */
  def executeParallel(
      group: ExecutionGroup,
      batchId: String,
      validatedFlows: Map[String, DataFrame]
  ): Seq[FlowResult] = {
    val futures = group.flows.map { flowConfig =>
      Future {
        executeFlow(flowConfig, batchId, validatedFlows)
      }(parallelEc)
    }

    val allResults = Future.sequence(futures)(implicitly, parallelEc)
    Await.result(allResults, MaxParallelTimeout)
  }

  /** Executes a single flow
    */
  private def executeFlow(
      flowConfig: FlowConfig,
      batchId: String,
      validatedFlows: Map[String, DataFrame]
  ): FlowResult = {
    logger.debug(s"Starting flow ${flowConfig.name} - batchId: $batchId")

    val maxRetries = globalConfig.processing.maxRetries
    val backoffMs = globalConfig.processing.retryBackoffMs

    if (maxRetries > 0) {
      RetryExecutor.withRetry(
        maxRetries = maxRetries,
        baseDelayMs = backoffMs,
        operationName = s"Flow ${flowConfig.name}"
      ) {
        val executor = new FlowExecutor(flowConfig, globalConfig, validatedFlows, domainsConfig, customValidators)
        val result = executor.execute(batchId)
        if (!result.success) throw new RuntimeException(result.error.getOrElse("Flow failed"))
        result
      }
    } else {
      val executor = new FlowExecutor(flowConfig, globalConfig, validatedFlows, domainsConfig, customValidators)
      executor.execute(batchId)
    }
  }

  /** Determines if execution should stop based on result. Per-flow maxRejectionRate overrides the global setting.
    */
  def shouldStopExecution(result: FlowResult, flowConfig: FlowConfig): Boolean = {
    if (!result.success) {
      return true
    }

    val threshold = flowConfig.maxRejectionRate.orElse(globalConfig.processing.maxRejectionRate)

    threshold match {
      case Some(rate) if result.rejectedRecords > 0 && result.rejectionRate > rate =>
        logger.warn(
          f"Flow ${result.flowName} rejection rate ${result.rejectionRate}%.2f%% " +
            f"exceeds threshold ${rate}%.2f%% " +
            f"(${result.rejectedRecords} records)"
        )
        true
      case _ =>
        false
    }
  }
}
