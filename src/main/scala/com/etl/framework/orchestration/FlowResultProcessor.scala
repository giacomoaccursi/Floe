package com.etl.framework.orchestration

import com.etl.framework.config.{FlowConfig, GlobalConfig, LoadMode}
import com.etl.framework.orchestration.batch.FlowGroupExecutor
import com.etl.framework.orchestration.flow.FlowResult
import com.etl.framework.util.SensitiveDataRedactor
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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
      loadValidatedData(result) match {
        case Right(df) => ContinueWith(BatchState(allResults, validatedFlows + (result.flowName -> df)))
        case Left(error) =>
          logger.error(error)
          StopExecution(BatchState(allResults, validatedFlows), error)
      }
    }
  }

  /** Loads the Iceberg table for a completed flow so downstream flows can use it for FK validation. */
  private def loadValidatedData(result: FlowResult): Either[String, DataFrame] = {
    val tableName = globalConfig.iceberg.fullTableName(result.flowName)
    try {
      val table = result.resultingSnapshotId match {
        case Some(snapshotId) => spark.read.option("snapshot-id", snapshotId).table(tableName)
        case None =>
          val schema = result.resultingSchemaJson
            .map(json => DataType.fromJson(json).asInstanceOf[StructType])
            .getOrElse(throw new IllegalStateException(s"Missing resulting schema for snapshotless table $tableName"))
          spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      }
      flowConfigs.find(_.name == result.flowName) match {
        case Some(config) if config.loadMode.`type` == LoadMode.SCD2 =>
          Right(table.filter(col(config.loadMode.isCurrentColumn.getOrElse("is_current")) === true))
        case _ => Right(table)
      }
    } catch {
      case scala.util.control.NonFatal(error) =>
        Left(
          s"Could not load attempt-pinned state for flow '${result.flowName}' from $tableName: " +
            SensitiveDataRedactor.message(error)
        )
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
