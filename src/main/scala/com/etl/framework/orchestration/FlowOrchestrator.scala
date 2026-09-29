package com.etl.framework.orchestration

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig, OrphanAction}
import com.etl.framework.iceberg.{OrphanDetectionResult, OrphanDetector, OrphanReport}
import com.etl.framework.io.readers.DataReaderFactory
import com.etl.framework.orchestration.batch.{BatchIdGenerator, BatchMetadataWriter, FlowGroupExecutor, QualityMetricsWriter}
import com.etl.framework.orchestration.flow.FlowResult
import com.etl.framework.orchestration.planning.ExecutionPlanBuilder
import com.etl.framework.pipeline.{DerivedTableContext, DerivedTableExecutor, DerivedTableResult}
import com.etl.framework.validation.Validator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

/** Executes one immutable pipeline attempt.
  *
  * Floe deliberately keeps no durable coordinator state: the hosting platform owns scheduling, retries, mutual
  * exclusion and retention of the request. A FlowOrchestrator instance is single-use and returns the evidence observed
  * during that attempt; it never resumes or replays an earlier attempt.
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
    pipelineId: String,
    threadPool: Option[java.util.concurrent.ExecutorService] = None,
    batchListeners: Seq[BatchListener] = Seq.empty,
    derivedTables: Seq[(String, DerivedTableContext => DataFrame)] = Seq.empty
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  def buildExecutionPlan(): ExecutionPlan = planBuilder.build()

  /** Convenience entry point for local/ad-hoc runs. It creates a new logical run and attempt. Managed platforms should
    * call `execute(request)` and persist that request before submission.
    */
  def execute(): IngestionResult = {
    val request = ExecutionRequest(
      pipelineId = pipelineId,
      logicalRunId = BatchIdGenerator.generate(globalConfig.processing.batchIdFormat),
      attemptId = UUID.randomUUID().toString,
      effectiveAt = Instant.now()
    )
    execute(request)
  }

  /** Executes exactly one attempt. No automatic retry is performed at flow or job level. */
  def execute(request: ExecutionRequest): IngestionResult = {
    require(
      request.pipelineId == pipelineId,
      s"Execution request pipelineId '${request.pipelineId}' does not match configured pipeline '$pipelineId'"
    )

    val startedAtNanos = System.nanoTime()
    val attemptId = request.attemptId
    executionLogger.logBatchStart(attemptId, flowConfigs.size)

    var state = BatchState(Seq.empty, Map.empty)
    var derivedResults = Seq.empty[DerivedTableResult]
    var orphanReports = Seq.empty[OrphanReport]

    val initialResult =
      try {
        val plan = buildExecutionPlan()
        var stopError = Option.empty[String]

        plan.groups.foreach { group =>
          if (stopError.isEmpty) {
            executionLogger.logGroupStart(group)
            val groupResults = executeGroup(group, attemptId, state.validatedFlows, request.effectiveAt)
            resultProcessor.processGroupResults(groupResults, state, attemptId) match {
              case resultProcessor.ContinueWith(nextState) => state = nextState
              case resultProcessor.StopExecution(nextState, error) =>
                state = nextState
                stopError = Some(error)
            }
          }
        }

        stopError match {
          case Some(error) => failedResult(request, state.flowResults, error)
          case None =>
            val orphanResult = runPostBatchOrphanDetection(state.flowResults, plan)
            orphanReports = orphanResult.reports
            val orphanError = orphanResult match {
              case OrphanDetectionResult.Failed(error, _) => Some(s"Orphan detection failed: $error")
              case _                                      => None
            }

            if (orphanError.isEmpty && derivedTables.nonEmpty) {
              logger.info(s"Executing ${derivedTables.size} derived tables for attempt $attemptId")
              derivedResults =
                new DerivedTableExecutor(globalConfig.iceberg)
                  .execute(derivedTables, attemptId, request.effectiveAt)
            }

            val derivedFailures = derivedResults.filterNot(_.success)
            val errors = orphanError.toSeq ++
              (if (derivedFailures.nonEmpty)
                 Seq(s"Derived tables failed: ${derivedFailures.map(_.tableName).mkString(", ")}")
               else Seq.empty)

            if (errors.isEmpty)
              successfulResult(request, state.flowResults, derivedResults, orphanReports)
            else
              failedResult(
                request,
                state.flowResults,
                errors.mkString("; "),
                derivedResults,
                orphanReports,
                forceUnknown = orphanError.isDefined
              )
        }
      } catch {
        case error: Exception =>
          executionLogger.logExecutionFailure(attemptId, elapsedMillis(startedAtNanos), error)
          failedResult(request, state.flowResults, Option(error.getMessage).getOrElse(error.getClass.getName))
      }

    val executionTimeMs = elapsedMillis(startedAtNanos)
    val observedWarnings = writeObservability(
      request,
      initialResult,
      orphanReports,
      executionTimeMs
    )
    val result = withWarnings(initialResult, observedWarnings, executionTimeMs)

    executionLogger.logBatchSummary(attemptId, result.flowResults, executionTimeMs)
    notifyListeners(result)
    threadPool.foreach(_.shutdown())
    result
  }

  private def executeGroup(
      group: ExecutionGroup,
      attemptId: String,
      validatedFlows: Map[String, DataFrame],
      effectiveAt: Instant
  ): Seq[FlowResult] =
    if (group.parallel) groupExecutor.executeParallel(group, attemptId, validatedFlows, effectiveAt)
    else groupExecutor.executeSequential(group, attemptId, validatedFlows, effectiveAt)

  private def successfulResult(
      request: ExecutionRequest,
      flowResults: Seq[FlowResult],
      derivedResults: Seq[DerivedTableResult],
      orphanReports: Seq[OrphanReport]
  ): IngestionResult = {
    val warnings = flowResults.flatMap(_.warnings)
    IngestionResult(
      request = request,
      flowResults = flowResults,
      success = true,
      derivedTableResults = derivedResults,
      orphanReports = orphanReports,
      status = if (warnings.nonEmpty) ExecutionStatus.SucceededWithWarnings else ExecutionStatus.Succeeded,
      warnings = warnings
    )
  }

  private def failedResult(
      request: ExecutionRequest,
      flowResults: Seq[FlowResult],
      error: String,
      derivedResults: Seq[DerivedTableResult] = Seq.empty,
      orphanReports: Seq[OrphanReport] = Seq.empty,
      forceUnknown: Boolean = false
  ): IngestionResult = {
    val outcomes = flowResults.map(_.dataOutcome) ++ derivedResults.map(_.dataOutcome)
    val status =
      if (forceUnknown || outcomes.contains(DataOutcome.Unknown)) ExecutionStatus.Unknown
      else if (outcomes.exists(outcome => outcome == DataOutcome.Committed || outcome == DataOutcome.NoChange))
        ExecutionStatus.FailedPartial
      else ExecutionStatus.Failed

    IngestionResult(
      request = request,
      flowResults = flowResults,
      success = false,
      error = Some(error),
      derivedTableResults = derivedResults,
      orphanReports = orphanReports,
      status = status,
      warnings = flowResults.flatMap(_.warnings)
    )
  }

  private def writeObservability(
      request: ExecutionRequest,
      result: IngestionResult,
      orphanReports: Seq[OrphanReport],
      executionTimeMs: Long
  ): Seq[String] = {
    val warnings = Vector.newBuilder[String]
    try {
      metadataWriter.writeBatchMetadata(request, result, executionTimeMs)
    } catch {
      case error: Exception =>
        val warning = s"Batch report write failed: ${error.getMessage}"
        logger.warn(warning, error)
        warnings += warning
    }

    // Quality metrics are diagnostic and their writer is intentionally best-effort.
    new QualityMetricsWriter(globalConfig, flowConfigs)
      .write(request.attemptId, result.flowResults, orphanReports, executionTimeMs, result.success)
    warnings.result()
  }

  private def withWarnings(
      result: IngestionResult,
      additionalWarnings: Seq[String],
      executionTimeMs: Long
  ): IngestionResult = {
    val warnings = result.warnings ++ additionalWarnings
    val status =
      if (result.success && warnings.nonEmpty) ExecutionStatus.SucceededWithWarnings
      else result.status
    result.copy(status = status, warnings = warnings, executionTimeMs = executionTimeMs)
  }

  private def runPostBatchOrphanDetection(
      flowResults: Seq[FlowResult],
      plan: ExecutionPlan
  ): OrphanDetectionResult = {
    val enabled = flowConfigs.exists(_.validation.foreignKeys.exists(_.onOrphan != OrphanAction.Ignore))
    if (!enabled) OrphanDetectionResult.Skipped
    else new OrphanDetector(spark, globalConfig.iceberg, flowConfigs, flowResults).detectAndResolveOrphans(plan)
  }

  private def notifyListeners(result: IngestionResult): Unit =
    batchListeners.foreach { listener =>
      try {
        if (result.success) listener.onBatchCompleted(result) else listener.onBatchFailed(result)
      } catch {
        case error: Exception =>
          logger.warn(s"Batch listener ${listener.getClass.getSimpleName} failed: ${error.getMessage}", error)
      }
    }

  private def elapsedMillis(startedAtNanos: Long): Long =
    (System.nanoTime() - startedAtNanos) / 1000000L
}

object FlowOrchestrator {
  def apply(
      globalConfig: GlobalConfig,
      flowConfigs: Seq[FlowConfig],
      domainsConfig: Option[DomainsConfig] = None,
      customValidators: Map[String, () => Validator] = Map.empty,
      batchListeners: Seq[BatchListener] = Seq.empty,
      customReaders: Map[String, DataReaderFactory.ReaderFactory] = Map.empty,
      derivedTables: Seq[(String, DerivedTableContext => DataFrame)] = Seq.empty,
      pipelineId: String = "local"
  )(implicit spark: SparkSession): FlowOrchestrator = {
    val pool = Executors.newFixedThreadPool(math.max(1, Runtime.getRuntime.availableProcessors() * 2))
    val executionContext = ExecutionContext.fromExecutorService(pool)
    val groupExecutor =
      new FlowGroupExecutor(globalConfig, domainsConfig, executionContext, customValidators, customReaders)

    new FlowOrchestrator(
      globalConfig = globalConfig,
      flowConfigs = flowConfigs,
      domainsConfig = domainsConfig,
      planBuilder = new ExecutionPlanBuilder(flowConfigs, globalConfig),
      groupExecutor = groupExecutor,
      resultProcessor = new FlowResultProcessor(globalConfig, flowConfigs, groupExecutor),
      metadataWriter = new BatchMetadataWriter(globalConfig, flowConfigs),
      executionLogger = new ExecutionLogger(),
      pipelineId = pipelineId,
      threadPool = Some(pool),
      batchListeners = batchListeners,
      derivedTables = derivedTables
    )
  }
}

case class ExecutionPlan(groups: Seq[ExecutionGroup])

case class ExecutionGroup(flows: Seq[FlowConfig], parallel: Boolean)

/** Immutable result for one attempt. `success` is the functional outcome; `status` also captures partial/unknown data
  * effects. Snapshot IDs remain in the per-target results.
  */
case class IngestionResult(
    request: ExecutionRequest,
    flowResults: Seq[FlowResult],
    success: Boolean,
    error: Option[String] = None,
    derivedTableResults: Seq[DerivedTableResult] = Seq.empty,
    orphanReports: Seq[OrphanReport] = Seq.empty,
    status: ExecutionStatus = ExecutionStatus.Unknown,
    warnings: Seq[String] = Seq.empty,
    executionTimeMs: Long = 0L
) {
  def pipelineId: String = request.pipelineId
  def logicalRunId: String = request.logicalRunId
  def attemptId: String = request.attemptId
  def effectiveAt: Instant = request.effectiveAt

  /** Compatibility alias for diagnostic paths created by older Floe applications. */
  def batchId: String = attemptId
}
