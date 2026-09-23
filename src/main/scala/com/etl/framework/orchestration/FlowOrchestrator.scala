package com.etl.framework.orchestration

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig, OrphanAction}
import com.etl.framework.iceberg.{IcebergTableManager, MaintenanceResult, OrphanDetectionResult, OrphanDetector}
import com.etl.framework.io.readers.DataReaderFactory
import com.etl.framework.orchestration.batch.{
  BatchIdGenerator,
  BatchMetadataWriter,
  FlowGroupExecutor,
  QualityMetricsWriter
}
import com.etl.framework.orchestration.flow.FlowResult
import com.etl.framework.orchestration.planning.ExecutionPlanBuilder
import com.etl.framework.pipeline.{DerivedTableContext, DerivedTableExecutor, DerivedTableResult}
import com.etl.framework.validation.Validator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import java.time.Instant
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
    threadPool: Option[java.util.concurrent.ExecutorService] = None,
    batchListeners: Seq[BatchListener] = Seq.empty,
    customReaders: Map[String, DataReaderFactory.ReaderFactory] = Map.empty,
    derivedTables: Seq[(String, DerivedTableContext => DataFrame)] = Seq.empty,
    maintenanceExecutor: Option[(FlowConfig, com.etl.framework.config.MaintenanceConfig) => Unit] = None
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Builds execution plan based on FK dependencies.
    */
  def buildExecutionPlan(): ExecutionPlan = planBuilder.build()

  /** Executes all flows in correct order.
    */
  def execute(): IngestionResult = {
    val batchId = BatchIdGenerator.generate(globalConfig.processing.batchIdFormat)
    val effectiveAt = Instant.now()
    val startTime = System.nanoTime()

    executionLogger.logBatchStart(batchId, flowConfigs.size)

    var state = BatchState(Seq.empty, Map.empty)

    val result =
      try {
        val plan = buildExecutionPlan()
        var stoppedResult: Option[IngestionResult] = None

        plan.groups.foreach { group =>
          if (stoppedResult.isEmpty) {
            executionLogger.logGroupStart(group)

            val groupResults = executeGroup(group, batchId, state.validatedFlows, effectiveAt)

            resultProcessor.processGroupResults(groupResults, state, batchId) match {
              case resultProcessor.StopExecution(r) => stoppedResult = Some(r)
              case resultProcessor.ContinueWith(newState) => state = newState
            }
          }
        }

        stoppedResult match {
          case Some(failed) => finalizeStoppedResult(failed, startTime)
          case None         => createSuccessResult(batchId, effectiveAt, state.flowResults, startTime, plan)
        }

      } catch {
        case e: Exception =>
          handleExecutionFailure(batchId, state.flowResults, startTime, e)
      } finally {
        threadPool.foreach(_.shutdown())
      }

    notifyListeners(result)
    result
  }

  private def notifyListeners(result: IngestionResult): Unit = {
    batchListeners.foreach { listener =>
      try {
        if (result.success) listener.onBatchCompleted(result)
        else listener.onBatchFailed(result)
      } catch {
        case e: Exception =>
          logger.warn(s"Batch listener ${listener.getClass.getSimpleName} failed: ${e.getMessage}")
      }
    }
  }

  /** Executes a group of flows (sequential or parallel).
    */
  private def executeGroup(
      group: ExecutionGroup,
      batchId: String,
      validatedFlows: Map[String, DataFrame],
      effectiveAt: Instant
  ): Seq[FlowResult] = {
    if (group.parallel) {
      groupExecutor.executeParallel(group, batchId, validatedFlows, effectiveAt)
    } else {
      groupExecutor.executeSequential(group, batchId, validatedFlows, effectiveAt)
    }
  }

  /** Creates a successful ingestion result.
    */
  private def createSuccessResult(
      batchId: String,
      effectiveAt: Instant,
      flowResults: Seq[FlowResult],
      startTime: Long,
      plan: ExecutionPlan
  ): IngestionResult = {
    // Run orphan detection BEFORE maintenance (maintenance may expire snapshots needed for time travel)
    val orphanResult = runPostBatchOrphanDetection(flowResults, plan)
    val orphanError = orphanResult match {
      case OrphanDetectionResult.Failed(err, _) => Some(err)
      case _                                    => None
    }
    val derivedResults = if (derivedTables.nonEmpty && orphanError.isEmpty) {
      logger.info(s"Executing ${derivedTables.size} derived tables before finalizing batch $batchId")
      new DerivedTableExecutor(globalConfig.iceberg).execute(derivedTables, batchId, effectiveAt)
    } else Seq.empty
    val derivedFailures = derivedResults.filterNot(_.success)
    val failureMessages = orphanError.map(err => s"Orphan detection failed: $err").toSeq ++
      (if (derivedFailures.nonEmpty)
         Seq(s"Derived tables failed: ${derivedFailures.map(_.tableName).mkString(", ")}")
       else Seq.empty)
    val batchSuccess = failureMessages.isEmpty
    val batchError = if (batchSuccess) None else Some(failureMessages.mkString("; "))
    val derivedMaintenanceResults = derivedResults.flatMap(_.maintenanceResult)
    val flowMaintenanceResults =
      if (batchSuccess) runPostBatchMaintenance()
      else {
        logger.warn(s"Skipping post-batch maintenance for failed batch $batchId")
        Seq.empty
      }
    val maintenanceResults = derivedMaintenanceResults ++ flowMaintenanceResults
    val executionTimeMs = (System.nanoTime() - startTime) / 1000000

    try {
      metadataWriter.writeBatchMetadata(
        batchId,
        flowResults,
        executionTimeMs,
        success = batchSuccess,
        orphanReports = orphanResult.reports,
        derivedTableResults = derivedResults,
        orphanDetectionError = orphanError,
        maintenanceResults = maintenanceResults
      )
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to write batch metadata for batch $batchId: ${e.getMessage}")
    }

    // Write quality metrics to Iceberg (if configured)
    val qualityWriter = new QualityMetricsWriter(globalConfig, flowConfigs)
    qualityWriter.write(batchId, flowResults, orphanResult.reports, executionTimeMs, batchSuccess)

    executionLogger.logBatchSummary(batchId, flowResults, executionTimeMs)

    IngestionResult(
      batchId = batchId,
      flowResults = flowResults,
      success = batchSuccess,
      error = batchError,
      derivedTableResults = derivedResults,
      maintenanceResults = maintenanceResults
    )
  }

  /** Handles execution failure.
    */
  private def finalizeStoppedResult(result: IngestionResult, startTime: Long): IngestionResult = {
    val executionTimeMs = (System.nanoTime() - startTime) / 1000000
    writeFailedBatchObservability(result.batchId, result.flowResults, executionTimeMs)
    executionLogger.logBatchSummary(result.batchId, result.flowResults, executionTimeMs)
    result
  }

  private def handleExecutionFailure(
      batchId: String,
      flowResults: Seq[FlowResult],
      startTime: Long,
      error: Exception
  ): IngestionResult = {
    val executionTimeMs = (System.nanoTime() - startTime) / 1000000
    executionLogger.logExecutionFailure(batchId, executionTimeMs, error)

    writeFailedBatchObservability(batchId, flowResults, executionTimeMs)

    IngestionResult(
      batchId = batchId,
      flowResults = flowResults,
      success = false,
      error = Some(error.getMessage)
    )
  }

  private def writeFailedBatchObservability(
      batchId: String,
      flowResults: Seq[FlowResult],
      executionTimeMs: Long
  ): Unit = {
    try {
      metadataWriter.writeBatchMetadata(batchId, flowResults, executionTimeMs, success = false)
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to write batch metadata for failed batch $batchId: ${e.getMessage}")
    }
    new QualityMetricsWriter(globalConfig, flowConfigs)
      .write(batchId, flowResults, Seq.empty, executionTimeMs, batchSuccess = false)
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
  private def runPostBatchMaintenance(): Seq[MaintenanceResult] = {
    val icebergConfig = globalConfig.iceberg
    lazy val tableManager = new IcebergTableManager(spark, icebergConfig)
    logger.info("Running post-batch Iceberg table maintenance")

    flowConfigs.map { flowConfig =>
      try {
        maintenanceExecutor match {
          case Some(execute) => execute(flowConfig, icebergConfig.maintenance)
          case None          => tableManager.runMaintenance(flowConfig, icebergConfig.maintenance)
        }
        MaintenanceResult(flowConfig.name, "flow", success = true)
      } catch {
        case e: Exception =>
          logger.error(
            s"Maintenance failed for flow ${flowConfig.name}: ${e.getMessage}",
            e
          )
          MaintenanceResult(flowConfig.name, "flow", success = false, error = Some(e.getMessage))
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
      customValidators: Map[String, () => Validator] = Map.empty,
      batchListeners: Seq[BatchListener] = Seq.empty,
      customReaders: Map[String, DataReaderFactory.ReaderFactory] = Map.empty,
      derivedTables: Seq[(String, DerivedTableContext => DataFrame)] = Seq.empty,
      maintenanceExecutor: Option[(FlowConfig, com.etl.framework.config.MaintenanceConfig) => Unit] = None
  )(implicit spark: SparkSession): FlowOrchestrator = {
    val pool = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors() * 2)
    val ec = ExecutionContext.fromExecutorService(pool)
    val groupExecutor = new FlowGroupExecutor(globalConfig, domainsConfig, ec, customValidators, customReaders)
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
      threadPool = Some(pool),
      batchListeners = batchListeners,
      customReaders = customReaders,
      derivedTables = derivedTables,
      maintenanceExecutor = maintenanceExecutor
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
    derivedTableResults: Seq[DerivedTableResult] = Seq.empty,
    maintenanceResults: Seq[MaintenanceResult] = Seq.empty
)
