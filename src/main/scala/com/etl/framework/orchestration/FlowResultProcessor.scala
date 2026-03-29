package com.etl.framework.orchestration

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.orchestration.batch.FlowGroupExecutor
import com.etl.framework.orchestration.flow.FlowResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/** Processes flow execution results and manages validated data loading. Extracted from FlowOrchestrator to follow
  * Single Responsibility Principle.
  */
class FlowResultProcessor(
    globalConfig: GlobalConfig,
    flowConfigs: Seq[FlowConfig],
    groupExecutor: FlowGroupExecutor
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Result of processing a group of flow results.
    */
  sealed trait ProcessingResult
  case object Continue extends ProcessingResult
  case class StopExecution(result: IngestionResult) extends ProcessingResult

  /** Processes group results and determines if execution should continue.
    *
    * @param groupResults
    *   Results from the executed group
    * @param accumulatedResults
    *   All results accumulated so far (will be updated)
    * @param validatedFlows
    *   Map of validated flows (will be updated)
    * @param batchId
    *   Current batch ID
    * @return
    *   ProcessingResult indicating whether to continue or stop
    */
  def processGroupResults(
      groupResults: Seq[FlowResult],
      accumulatedResults: scala.collection.mutable.ArrayBuffer[FlowResult],
      validatedFlows: scala.collection.mutable.Map[String, DataFrame],
      batchId: String
  ): ProcessingResult = {

    groupResults.foldLeft[ProcessingResult](Continue) {
      case (StopExecution(result), _) => StopExecution(result)
      case (Continue, result) =>
        accumulatedResults.append(result)
        processResult(result, validatedFlows, accumulatedResults.toSeq, batchId)
    }
  }

  private def processResult(
      result: FlowResult,
      validatedFlows: scala.collection.mutable.Map[String, DataFrame],
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
    } else if ({
      val flowConfig = flowConfigs.find(_.name == result.flowName)
      flowConfig.exists(fc => groupExecutor.shouldStopExecution(result, fc))
    }) {
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
      loadValidatedData(result, validatedFlows)
      Continue
    }
  }

  /** Loads validated data for a successful flow from the Iceberg table so that downstream flows that reference this
    * flow via FK can find it in validatedFlows.
    */
  def loadValidatedData(
      result: FlowResult,
      validatedFlows: scala.collection.mutable.Map[String, DataFrame]
  ): Unit = {
    val tableName = s"${globalConfig.iceberg.catalogName}.default.${result.flowName}"
    scala.util.Try(spark.table(tableName)).toOption match {
      case Some(data) => validatedFlows(result.flowName) = data
      case None       => logger.warn(s"Could not load Iceberg table $tableName, FK checks against it will fail")
    }
  }

  /** Loads validated data and returns Option for immutable operations.
    */
  def loadValidatedDataOpt(result: FlowResult): Option[DataFrame] = {
    val tableName = s"${globalConfig.iceberg.catalogName}.default.${result.flowName}"
    scala.util.Try(spark.table(tableName)).toOption
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
