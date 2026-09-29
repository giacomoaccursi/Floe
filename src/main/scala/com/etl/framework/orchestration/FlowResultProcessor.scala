package com.etl.framework.orchestration

import com.etl.framework.config.{FlowConfig, GlobalConfig, LoadMode}
import com.etl.framework.orchestration.batch.FlowGroupExecutor
import com.etl.framework.orchestration.flow.FlowResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

case class BatchState(
    flowResults: Seq[FlowResult],
    validatedFlows: Map[String, DataFrame]
)

/** Processes flow group results: tracks batch state (flow results + validated DataFrames), stops execution on failure
  * or rejection threshold breach, loads Iceberg tables for FK validation.
  */
class FlowResultProcessor(
    globalConfig: GlobalConfig,
    flowConfigs: Seq[FlowConfig],
    groupExecutor: FlowGroupExecutor
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  sealed trait ProcessingResult
  case class ContinueWith(state: BatchState) extends ProcessingResult
  case class StopExecution(state: BatchState, error: String) extends ProcessingResult

  /** Processes results from a group of flows. Returns StopExecution if any flow failed or exceeded rejection threshold,
    * ContinueWith if all flows succeeded.
    */
  def processGroupResults(
      groupResults: Seq[FlowResult],
      currentState: BatchState,
      _attemptId: String
  ): ProcessingResult = {
    groupResults.foldLeft[ProcessingResult](ContinueWith(currentState)) {
      case (StopExecution(state, error), result) =>
        StopExecution(state.copy(flowResults = state.flowResults :+ result), error)
      case (ContinueWith(state), result) =>
        val newResults = state.flowResults :+ result
        processResult(result, state.validatedFlows, newResults)
    }
  }

  private def processResult(
      result: FlowResult,
      validatedFlows: Map[String, DataFrame],
      allResults: Seq[FlowResult]
  ): ProcessingResult = {
    if (!result.success) {
      val error = s"Flow ${result.flowName} failed: ${result.error.getOrElse("Unknown error")}"
      logger.error(error)
      StopExecution(BatchState(allResults, validatedFlows), error)
    } else if (
      flowConfigs.find(_.name == result.flowName).exists(fc => groupExecutor.shouldStopExecution(result, fc))
    ) {
      logger.warn(
        s"Stopping execution - flow ${result.flowName} " +
          f"rejection rate: ${result.rejectionRate}%.2f%%, rejected: ${result.rejectedRecords}"
      )
      StopExecution(
        BatchState(allResults, validatedFlows),
        s"Flow ${result.flowName} exceeded rejection threshold or has validation errors"
      )
    } else {
      val newValidated = loadValidatedData(result) match {
        case Some(df) => validatedFlows + (result.flowName -> df)
        case None     => validatedFlows
      }
      ContinueWith(BatchState(allResults, newValidated))
    }
  }

  /** Loads the Iceberg table for a completed flow so downstream flows can use it for FK validation. */
  private def loadValidatedData(result: FlowResult): Option[DataFrame] = {
    val tableName = globalConfig.iceberg.fullTableName(result.flowName)
    scala.util.Try {
      val table = spark.table(tableName)
      flowConfigs.find(_.name == result.flowName) match {
        case Some(config) if config.loadMode.`type` == LoadMode.SCD2 =>
          table.filter(col(config.loadMode.isCurrentColumn.getOrElse("is_current")) === true)
        case _ => table
      }
    }.toOption match {
      case some @ Some(_) => some
      case None =>
        logger.warn(s"Could not load Iceberg table $tableName, FK checks against it will fail")
        None
    }
  }
}

object FlowResultProcessor {
  def apply(
      globalConfig: GlobalConfig,
      flowConfigs: Seq[FlowConfig],
      groupExecutor: FlowGroupExecutor
  )(implicit spark: SparkSession): FlowResultProcessor =
    new FlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
}
