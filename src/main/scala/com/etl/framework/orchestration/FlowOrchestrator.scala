package com.etl.framework.orchestration

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig, OrphanAction}
import com.etl.framework.iceberg.{IcebergTableManager, OrphanDetectionResult, OrphanDetector, OrphanReport}
import com.etl.framework.orchestration.batch.{BatchMetadataWriter, FlowGroupExecutor}
import com.etl.framework.orchestration.flow.FlowResult
import com.etl.framework.orchestration.planning.ExecutionPlanBuilder
import com.etl.framework.pipeline.DerivedTableResult
import com.etl.framework.validation.Validator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

/** Coordinates execution of all flows respecting dependencies. Uses specialized components following Single
  * Responsibility Principle:
  *   - ExecutionPlanBuilder: builds execution plan from dependencies
  *   - FlowGroupExecutor: executes groups of flows
  *   - FlowResultProcessor: processes results and loads validated data
  *   - BatchMetadataWriter: writes batch metadata
  *   - ExecutionLogger: handles logging
  *
  * @param globalConfig
  *   Global framework configuration
  * @param flowConfigs
  *   Configurations for all flows to execute
  * @param domainsConfig
  *   Optional domain value configurations
  * @param planBuilder
  *   Builds execution plans (injected for testability)
  * @param groupExecutor
  *   Executes flow groups (injected for testability)
  * @param resultProcessor
  *   Processes results (injected for testability)
  * @param metadataWriter
  *   Writes batch metadata (injected for testability)
  * @param executionLogger
  *   Handles logging (injected for testability)
  * @param spark
  *   Implicit SparkSession
  */
class FlowOrchestrator(
    globalConfig: GlobalConfig,
    flowConfigs: Seq[FlowConfig],
    domainsConfig: Option[DomainsConfig],
    planBuilder: ExecutionPlanBuilder,
    groupExecutor: FlowGroupExecutor,
    resultProcessor: FlowResultProcessor,
    metadataWriter: BatchMetadataWriter,
    executionLogger: ExecutionLogger,
    threadPool: Option[java.util.concurrent.ExecutorService] = None
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Builds execution plan based on FK dependencies.
    */
  def buildExecutionPlan(): ExecutionPlan = planBuilder.build()

  /** Executes all flows in correct order.
    */
  def execute(): IngestionResult = {
    val batchId = metadataWriter.generateBatchId()
    val startTime = System.nanoTime()

    executionLogger.logBatchStart(batchId, flowConfigs.size)

    val flowResults = mutable.ArrayBuffer[FlowResult]()
    val validatedFlows = mutable.Map[String, DataFrame]()

    try {
      val plan = buildExecutionPlan()

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

      createSuccessResult(batchId, flowResults.toSeq, startTime, plan)

    } catch {
      case e: Exception =>
        handleExecutionFailure(batchId, flowResults.toSeq, startTime, e)
    } finally {
      threadPool.foreach(_.shutdown())
    }
  }

  /** Executes a group of flows (sequential or parallel).
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

  /** Creates a successful ingestion result.
    */
  private def createSuccessResult(
      batchId: String,
      flowResults: Seq[FlowResult],
      startTime: Long,
      plan: ExecutionPlan
  ): IngestionResult = {
    val executionTimeMs = (System.nanoTime() - startTime) / 1000000

    // Run orphan detection BEFORE maintenance (maintenance may expire snapshots needed for time travel)
    val orphanResult = runPostBatchOrphanDetection(flowResults, plan)

    metadataWriter.writeBatchMetadata(
      batchId,
      flowResults,
      executionTimeMs,
      success = true,
      orphanReports = orphanResult.reports,
      orphanDetectionError = orphanResult match {
        case OrphanDetectionResult.Failed(err, _) => Some(err)
        case _                                    => None
      }
    )

    // Run Iceberg table maintenance post-batch
    runPostBatchMaintenance()

    executionLogger.logBatchSummary(batchId, flowResults, executionTimeMs)

    IngestionResult(
      batchId = batchId,
      flowResults = flowResults,
      success = true
    )
  }

  /** Handles execution failure.
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

  /** Runs post-batch orphan detection if Iceberg is enabled and any FK has onOrphan != Ignore.
    */
  private def runPostBatchOrphanDetection(
      flowResults: Seq[FlowResult],
      plan: ExecutionPlan
  ): OrphanDetectionResult = {
    val hasOrphanChecks = flowConfigs.exists(
      _.validation.foreignKeys.exists(_.onOrphan != OrphanAction.Ignore)
    )
    if (!hasOrphanChecks) return OrphanDetectionResult.Skipped

    val detector =
      new OrphanDetector(spark, globalConfig.iceberg, flowConfigs, flowResults)
    val result = detector.detectAndResolveOrphans(plan)
    result match {
      case OrphanDetectionResult.Completed(reports) if reports.nonEmpty =>
        logger.info(s"Orphan detection completed: ${reports.size} reports generated")
      case OrphanDetectionResult.Failed(err, partial) =>
        logger.warn(s"Orphan detection failed after ${partial.size} reports: $err")
      case _ =>
    }
    result
  }

  /** Runs Iceberg table maintenance on all flow tables after batch completion.
    */
  private def runPostBatchMaintenance(): Unit = {
    val icebergConfig = globalConfig.iceberg
    val tableManager = new IcebergTableManager(spark, icebergConfig)
    logger.info("Running post-batch Iceberg table maintenance")

    flowConfigs.foreach { flowConfig =>
      try {
        tableManager.runMaintenance(flowConfig, icebergConfig.maintenance)
      } catch {
        case e: Exception =>
          logger.error(
            s"Maintenance failed for flow ${flowConfig.name}: ${e.getMessage}"
          )
      }
    }
  }
}

/** Factory for creating FlowOrchestrator with default dependencies.
  */
object FlowOrchestrator {

  /** Creates FlowOrchestrator with default component implementations.
    */
  def apply(
      globalConfig: GlobalConfig,
      flowConfigs: Seq[FlowConfig],
      domainsConfig: Option[DomainsConfig] = None,
      customValidators: Map[String, () => Validator] = Map.empty
  )(implicit spark: SparkSession): FlowOrchestrator = {
    val pool = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors() * 2)
    val ec = ExecutionContext.fromExecutorService(pool)
    val groupExecutor = new FlowGroupExecutor(globalConfig, domainsConfig, ec, customValidators)
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
      executionLogger = executionLogger,
      threadPool = Some(pool)
    )
  }
}

/** Execution plan containing groups of flows.
  */
case class ExecutionPlan(
    groups: Seq[ExecutionGroup]
)

/** Group of flows that can be executed together.
  */
case class ExecutionGroup(
    flows: Seq[FlowConfig],
    parallel: Boolean
)

/** Result of Ingestion execution.
  */
case class IngestionResult(
    batchId: String,
    flowResults: Seq[FlowResult],
    success: Boolean,
    error: Option[String] = None,
    derivedTableResults: Seq[DerivedTableResult] = Seq.empty
)
