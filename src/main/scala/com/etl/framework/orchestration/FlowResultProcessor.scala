package com.etl.framework.orchestration

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.orchestration.batch.FlowGroupExecutor
import com.etl.framework.orchestration.flow.FlowResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

case class BatchState(
    flowResults: Seq[FlowResult],
    validatedFlows: Map[String, DataFrame]
)

class FlowResultProcessor(
    globalConfig: GlobalConfig,
    flowConfigs: Seq[FlowConfig],
    groupExecutor: FlowGroupExecutor
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  sealed trait ProcessingResult
  case class ContinueWith(state: BatchState) extends ProcessingResult
  case class StopExecution(result: IngestionResult) extends ProcessingResult

  def processGroupResults(
      groupResults: Seq[FlowResult],
      currentState: BatchState,
      batchId: String
  ): ProcessingResult = {
    groupResults.foldLeft[ProcessingResult](ContinueWith(currentState)) {
      case (stop: StopExecution, _) => stop
      case (ContinueWith(state), result) =>
        val newResults = state.flowResults :+ result
        processResult(result, state.validatedFlows, newResults, batchId)
    }
  }

  private def processResult(
      result: FlowResult,
      validatedFlows: Map[String, DataFrame],
      allResults: Seq[FlowResult],
      batchId: String
  ): ProcessingResult = {
    if (!result.success) {
      logger.error(s"Flow ${result.flowName} failed: ${result.error.getOrElse("Unknown error")}")
      StopExecution(
        IngestionResult(
          batchId = batchId,
          flowResults = allResults,
          success = false,
          error = Some(s"Flow ${result.flowName} failed: ${result.error.getOrElse("Unknown error")}")
        )
      )
    } else if (
      flowConfigs.find(_.name == result.flowName).exists(fc => groupExecutor.shouldStopExecution(result, fc))
    ) {
      logger.warn(
        s"Stopping execution - flow ${result.flowName} " +
          f"rejection rate: ${result.rejectionRate}%.2f%%, rejected: ${result.rejectedRecords}"
      )
      StopExecution(
        IngestionResult(
          batchId = batchId,
          flowResults = allResults,
          success = false,
          error = Some(s"Flow ${result.flowName} exceeded rejection threshold or has validation errors")
        )
      )
    } else {
      val newValidated = loadValidatedData(result) match {
        case Some(df) => validatedFlows + (result.flowName -> df)
        case None     => validatedFlows
      }
      ContinueWith(BatchState(allResults, newValidated))
    }
  }

  private def loadValidatedData(result: FlowResult): Option[DataFrame] = {
    val tableName = s"${globalConfig.iceberg.catalogName}.default.${result.flowName}"
    scala.util.Try(spark.table(tableName)).toOption match {
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
