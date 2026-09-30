package com.etl.framework.orchestration.batch

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig}
import com.etl.framework.io.readers.DataReaderFactory
import com.etl.framework.orchestration.flow.{FlowExecutor, FlowResult}
import com.etl.framework.orchestration.{ExecutionRequest, RejectionThresholdPolicy}
import com.etl.framework.validation.Validator
import com.etl.framework.orchestration.ExecutionGroup
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.control.NonFatal

/** Executes groups of flows sequentially or in parallel
  */
class FlowGroupExecutor(
    globalConfig: GlobalConfig,
    domainsConfig: Option[DomainsConfig],
    parallelEc: ExecutionContext,
    customValidators: Map[String, () => Validator] = Map.empty,
    customReaders: Map[String, DataReaderFactory.ReaderFactory] = Map.empty
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Executes a group of flows sequentially. Stops on first failure or rejection threshold breach. */
  def executeSequential(
      group: ExecutionGroup,
      request: ExecutionRequest,
      validatedFlows: Map[String, DataFrame]
  ): Seq[FlowResult] = {
    val results = mutable.ArrayBuffer[FlowResult]()

    for (flowConfig <- group.flows) {
      val result = executeFlow(flowConfig, request, validatedFlows)
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
      request: ExecutionRequest,
      validatedFlows: Map[String, DataFrame]
  ): Seq[FlowResult] = {
    val futures = group.flows.map { flowConfig =>
      Future {
        executeFlow(flowConfig, request, validatedFlows)
      }(parallelEc).recover { case NonFatal(error) =>
        logger.error(s"Parallel flow ${flowConfig.name} terminated unexpectedly: ${error.getMessage}", error)
        FlowResult.failure(flowConfig.name, request.attemptId, error.getMessage)
      }(parallelEc)
    }

    val allResults = Future.sequence(futures)(implicitly, parallelEc)
    Await.result(allResults, Duration.Inf)
  }

  /** Executes a single flow
    */
  private def executeFlow(
      flowConfig: FlowConfig,
      request: ExecutionRequest,
      validatedFlows: Map[String, DataFrame]
  ): FlowResult = {
    logger.debug(s"Starting flow ${flowConfig.name} - attemptId: ${request.attemptId}")
    val executor =
      new FlowExecutor(flowConfig, globalConfig, validatedFlows, domainsConfig, customValidators, customReaders)
    executor.execute(request)
  }

  /** Determines if execution should stop based on result. Per-flow maxRejectionRate overrides the global setting.
    */
  def shouldStopExecution(result: FlowResult, flowConfig: FlowConfig): Boolean = {
    if (!result.success) {
      return true
    }

    val threshold = RejectionThresholdPolicy.threshold(flowConfig, globalConfig)

    threshold match {
      case Some(rate)
          if RejectionThresholdPolicy.exceeded(
            result.rejectionRate,
            result.rejectedRecords,
            flowConfig,
            globalConfig
          ) =>
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
