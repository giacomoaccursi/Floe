package com.etl.framework.orchestration

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig}
import com.etl.framework.orchestration.batch.{BatchMetadataWriter, FlowGroupExecutor}
import com.etl.framework.orchestration.flow.FlowResult
import com.etl.framework.orchestration.planning.ExecutionPlanBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
 * Coordinates execution of all flows respecting dependencies.
 * Uses specialized components following Single Responsibility Principle:
 * - ExecutionPlanBuilder: builds execution plan from dependencies
 * - FlowGroupExecutor: executes groups of flows
 * - FlowResultProcessor: processes results and loads validated data
 * - BatchMetadataWriter: writes batch metadata
 * - ExecutionLogger: handles logging
 *
 * @param globalConfig Global framework configuration
 * @param flowConfigs Configurations for all flows to execute
 * @param domainsConfig Optional domain value configurations
 * @param planBuilder Builds execution plans (injected for testability)
 * @param groupExecutor Executes flow groups (injected for testability)
 * @param resultProcessor Processes results (injected for testability)
 * @param metadataWriter Writes batch metadata (injected for testability)
 * @param executionLogger Handles logging (injected for testability)
 * @param spark Implicit SparkSession
 */
class FlowOrchestrator(
  globalConfig: GlobalConfig,
  flowConfigs: Seq[FlowConfig],
  domainsConfig: Option[DomainsConfig],
  planBuilder: ExecutionPlanBuilder,
  groupExecutor: FlowGroupExecutor,
  resultProcessor: FlowResultProcessor,
  metadataWriter: BatchMetadataWriter,
  executionLogger: ExecutionLogger
)(implicit spark: SparkSession) {

  /**
   * Builds execution plan based on FK dependencies.
   */
  def buildExecutionPlan(): ExecutionPlan = planBuilder.build()

  /**
   * Executes all flows in correct order.
   */
  def execute(): IngestionResult = {
    val batchId = metadataWriter.generateBatchId()
    val startTime = System.nanoTime()

    executionLogger.logBatchStart(batchId, flowConfigs.size)

    val plan = buildExecutionPlan()
    val flowResults = mutable.ArrayBuffer[FlowResult]()
    val validatedFlows = mutable.Map[String, DataFrame]()

    try {
      plan.groups.foreach { group =>
        executionLogger.logGroupStart(group)

        val groupResults = executeGroup(group, batchId, validatedFlows.toMap)

        resultProcessor.processGroupResults(groupResults, flowResults, validatedFlows, batchId) match {
          case resultProcessor.StopExecution(result) =>
            return result
          case resultProcessor.Continue =>
          // Continue to next group
        }
      }

      createSuccessResult(batchId, flowResults.toSeq, startTime)

    } catch {
      case e: Exception =>
        handleExecutionFailure(batchId, flowResults.toSeq, startTime, e)
    }
  }

  /**
   * Executes a group of flows (sequential or parallel).
   */
  private def executeGroup(
    group: ExecutionGroup,
    batchId: String,
    validatedFlows: Map[String, DataFrame]
  ): Seq[FlowResult] = {
    if (group.parallel) {
      groupExecutor.executeParallel(group, batchId, validatedFlows)
    } else {
      groupExecutor.executeSequential(group, batchId, validatedFlows)
    }
  }

  /**
   * Creates a successful ingestion result.
   */
  private def createSuccessResult(
    batchId: String,
    flowResults: Seq[FlowResult],
    startTime: Long
  ): IngestionResult = {
    val executionTimeMs = (System.nanoTime() - startTime) / 1000000

    metadataWriter.writeBatchMetadata(
      batchId,
      flowResults,
      executionTimeMs,
      success = true
    )

    executionLogger.logBatchSummary(batchId, flowResults, executionTimeMs)

    IngestionResult(
      batchId = batchId,
      flowResults = flowResults,
      success = true
    )
  }

  /**
   * Handles execution failure.
   */
  private def handleExecutionFailure(
    batchId: String,
    flowResults: Seq[FlowResult],
    startTime: Long,
    error: Exception
  ): IngestionResult = {
    val executionTimeMs = (System.nanoTime() - startTime) / 1000000
    executionLogger.logExecutionFailure(batchId, executionTimeMs, error)

    IngestionResult(
      batchId = batchId,
      flowResults = flowResults,
      success = false,
      error = Some(error.getMessage)
    )
  }
}

/**
 * Factory for creating FlowOrchestrator with default dependencies.
 */
object FlowOrchestrator {

  /**
   * Creates FlowOrchestrator with default component implementations.
   */
  def apply(
    globalConfig: GlobalConfig,
    flowConfigs: Seq[FlowConfig],
    domainsConfig: Option[DomainsConfig] = None
  )(implicit spark: SparkSession): FlowOrchestrator = {
    val groupExecutor = new FlowGroupExecutor(globalConfig, domainsConfig)
    val metadataWriter = new BatchMetadataWriter(globalConfig, flowConfigs)
    val planBuilder = new ExecutionPlanBuilder(flowConfigs, globalConfig)
    val resultProcessor = new FlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    val executionLogger = new ExecutionLogger()

    new FlowOrchestrator(
      globalConfig = globalConfig,
      flowConfigs = flowConfigs,
      domainsConfig = domainsConfig,
      planBuilder = planBuilder,
      groupExecutor = groupExecutor,
      resultProcessor = resultProcessor,
      metadataWriter = metadataWriter,
      executionLogger = executionLogger
    )
  }
}

/**
 * Execution plan containing groups of flows.
 */
case class ExecutionPlan(
  groups: Seq[ExecutionGroup]
)

/**
 * Group of flows that can be executed together.
 */
case class ExecutionGroup(
  flows: Seq[FlowConfig],
  parallel: Boolean
)

/**
 * Result of Ingestion execution.
 */
case class IngestionResult(
  batchId: String,
  flowResults: Seq[FlowResult],
  success: Boolean,
  error: Option[String] = None
)
