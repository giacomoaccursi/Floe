package com.etl.framework.orchestration

import com.etl.framework.orchestration.flow.FlowResult
import org.slf4j.LoggerFactory

/**
 * Handles logging for execution orchestration.
 * Extracted from FlowOrchestrator to follow Single Responsibility Principle.
 */
class ExecutionLogger {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Logs the start of batch execution.
   */
  def logBatchStart(batchId: String, flowCount: Int): Unit = {
    logger.info(s"Starting ingestion execution - batchId: $batchId, flows: $flowCount")
  }

  /**
   * Logs the start of a group execution.
   */
  def logGroupStart(group: ExecutionGroup): Unit = {
    logger.info(s"Executing group [${group.flows.map(_.name).mkString(",")}]")
  }

  /**
   * Logs batch summary statistics.
   */
  def logBatchSummary(
    batchId: String,
    flowResults: Seq[FlowResult],
    executionTimeMs: Long
  ): Unit = {
    val totalInput = flowResults.map(_.inputRecords).sum
    val totalValid = flowResults.map(_.validRecords).sum
    val totalRejected = flowResults.map(_.rejectedRecords).sum
    val rejectionRate = if (totalInput > 0) (totalRejected.toDouble / totalInput * 100) else 0.0

    logger.info(
      f"Batch $batchId completed in ${executionTimeMs}ms - " +
        f"flows: ${flowResults.size}, input: $totalInput, valid: $totalValid, " +
        f"rejected: $totalRejected ($rejectionRate%.2f%%)"
    )
  }

  /**
   * Logs execution failure.
   */
  def logExecutionFailure(batchId: String, executionTimeMs: Long, error: Throwable): Unit = {
    logger.error(s"Ingestion execution failed after ${executionTimeMs}ms - batchId: $batchId", error)
  }

  /**
   * Logs flow failure.
   */
  def logFlowFailure(flowName: String, error: Option[String]): Unit = {
    logger.error(s"Flow $flowName failed: ${error.getOrElse("Unknown error")}")
  }

  /**
   * Logs execution stop due to rejection threshold.
   */
  def logStopDueToRejection(flowName: String, rejectionRate: Double, rejectedRecords: Long): Unit = {
    logger.warn(
      s"Stopping execution - flow $flowName " +
        f"rejection rate: $rejectionRate%.2f%%, rejected: $rejectedRecords"
    )
  }
}

object ExecutionLogger {
  def apply(): ExecutionLogger = new ExecutionLogger()
}
