package com.etl.framework.orchestration

import com.etl.framework.config.{DomainsConfig, FlowConfig, FlowConfigValidator, GlobalConfig, OrphanAction}
import com.etl.framework.iceberg.{IcebergTableManager, OrphanDetectionResult, OrphanDetector, OrphanReport}
import com.etl.framework.io.readers.DataReaderFactory
import com.etl.framework.orchestration.batch.{
  BatchIdGenerator,
  BatchMetadataWriter,
  FlowGroupExecutor,
  QualityMetricsWriter
}
import com.etl.framework.orchestration.flow.FlowResult
import com.etl.framework.orchestration.planning.ExecutionPlanBuilder
import com.etl.framework.pipeline.{DerivedTableDefinition, DerivedTableExecutor, DerivedTableResult}
import com.etl.framework.validation.Validator
import com.etl.framework.util.SensitiveDataRedactor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

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
    pipelineDefinition: PipelineDefinition,
    threadPool: Option[java.util.concurrent.ExecutorService] = None,
    batchListeners: Seq[BatchListener] = Seq.empty,
    derivedTables: Seq[DerivedTableDefinition] = Seq.empty
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val executionStarted = new AtomicBoolean(false)

  def executionDefinition: PipelineDefinition = pipelineDefinition

  def buildExecutionPlan(): ExecutionPlan = planBuilder.build()

  /** Convenience entry point for local/ad-hoc runs. It creates a new logical run and attempt. Managed platforms should
    * call `execute(request)` and persist that request before submission.
    */
  def execute(): IngestionResult = {
    val request = ExecutionRequest(
      pipelineId = pipelineDefinition.pipelineId,
      logicalRunId = BatchIdGenerator.generate(globalConfig.processing.batchIdFormat),
      attemptId = UUID.randomUUID().toString,
      effectiveAt = Instant.now().truncatedTo(ChronoUnit.MICROS),
      codeVersion = pipelineDefinition.codeVersion,
      configDigest = pipelineDefinition.configDigest
    )
    execute(request)
  }

  /** Executes exactly one attempt. No automatic retry is performed at flow or job level. */
  def execute(request: ExecutionRequest): IngestionResult = {
    if (!executionStarted.compareAndSet(false, true))
      throw new IllegalStateException(
        "FlowOrchestrator instances are single-use; create a new orchestrator for each execution attempt"
      )

    try {
      pipelineDefinition.validateRequest(request)
      validateFlowConfigurations()
      validateTargetTables()

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
              val groupResults = executeGroup(group, request, state.validatedFlows)
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
                derivedResults = new DerivedTableExecutor(globalConfig.iceberg)
                  .execute(derivedTables, request, state.validatedFlows)
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
                  orphanReports
                )
          }
        } catch {
          case NonFatal(error) =>
            executionLogger.logExecutionFailure(attemptId, elapsedMillis(startedAtNanos), error)
            failedResult(request, state.flowResults, SensitiveDataRedactor.message(error))
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
      result
    } finally {
      threadPool.foreach(_.shutdown())
    }
  }

  private def validateTargetTables(): Unit = {
    val targetNames =
      flowConfigs.map(_.name) ++ derivedTables.map(_.name) ++ globalConfig.processing.qualityMetricsTable.toSeq
    new IcebergTableManager(spark, globalConfig.iceberg)
      .validateTargetsExist(targetNames.map(globalConfig.iceberg.fullTableName))
  }

  private def validateFlowConfigurations(): Unit = {
    val errors = flowConfigs.flatMap { config =>
      FlowConfigValidator.validate(config).map(error => s"Flow '${config.name}': $error")
    }
    require(errors.isEmpty, s"Invalid flow configuration: ${errors.mkString("; ")}")
  }

  private def executeGroup(
      group: ExecutionGroup,
      request: ExecutionRequest,
      validatedFlows: Map[String, DataFrame]
  ): Seq[FlowResult] =
    if (group.parallel) groupExecutor.executeParallel(group, request, validatedFlows)
    else groupExecutor.executeSequential(group, request, validatedFlows)

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
      orphanReports: Seq[OrphanReport] = Seq.empty
  ): IngestionResult = {
    val outcomes = flowResults.map(_.dataOutcome) ++ derivedResults.map(_.dataOutcome)
    val status =
      if (outcomes.contains(DataOutcome.Unknown)) ExecutionStatus.Unknown
      else if (outcomes.exists(outcome => outcome == DataOutcome.Committed || outcome == DataOutcome.NoChange))
        ExecutionStatus.FailedPartial
      else ExecutionStatus.Failed

    IngestionResult(
      request = request,
      flowResults = flowResults,
      success = false,
      error = Some(SensitiveDataRedactor.redact(error)),
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
    // Write metrics first so the batch report can record any metrics warning in its final observed status.
    val metricsWarnings = new QualityMetricsWriter(globalConfig, flowConfigs)
      .write(request.attemptId, result.flowResults, orphanReports, executionTimeMs, result.success)
      .toSeq
    val reportResult = withWarnings(result, metricsWarnings, executionTimeMs)

    val reportWarnings = Vector.newBuilder[String]
    try {
      metadataWriter.writeBatchMetadata(request, reportResult, executionTimeMs)
    } catch {
      case NonFatal(error) =>
        val warning = s"Batch report write failed: ${SensitiveDataRedactor.message(error)}"
        logger.warn(warning, SensitiveDataRedactor.safeThrowable(error))
        reportWarnings += warning
    }
    metricsWarnings ++ reportWarnings.result()
  }

  private def withWarnings(
      result: IngestionResult,
      additionalWarnings: Seq[String],
      executionTimeMs: Long
  ): IngestionResult = {
    val warnings = SensitiveDataRedactor.redactAll(result.warnings ++ additionalWarnings)
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
        case NonFatal(error) =>
          logger.warn(
            s"Batch listener ${listener.getClass.getSimpleName} failed: ${SensitiveDataRedactor.message(error)}",
            SensitiveDataRedactor.safeThrowable(error)
          )
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
      derivedTables: Seq[DerivedTableDefinition] = Seq.empty,
      pipelineId: String = "local",
      codeVersion: String = "unversioned"
  )(implicit spark: SparkSession): FlowOrchestrator = {
    require(globalConfig.performance.maxParallelFlows > 0, "maxParallelFlows must be greater than zero")
    val workerCount = if (globalConfig.performance.parallelFlows) globalConfig.performance.maxParallelFlows else 1
    val pool = Executors.newFixedThreadPool(workerCount)
    val executionContext = ExecutionContext.fromExecutorService(pool)
    val groupExecutor =
      new FlowGroupExecutor(globalConfig, domainsConfig, executionContext, customValidators, customReaders)

    val semanticSparkConfig = Seq(
      "spark.sql.session.timeZone",
      "spark.sql.ansi.enabled",
      "spark.sql.caseSensitive",
      "spark.sql.legacy.timeParserPolicy"
    ).flatMap(key => spark.conf.getOption(key).map(key -> _)).toMap
    val definition = PipelineDefinitionBuilder.build(
      pipelineId,
      codeVersion,
      globalConfig,
      flowConfigs,
      domainsConfig,
      derivedTables.map(definition => definition.name -> definition.dependencies),
      semanticSparkConfig
    )

    new FlowOrchestrator(
      globalConfig = globalConfig,
      flowConfigs = flowConfigs,
      domainsConfig = domainsConfig,
      planBuilder = new ExecutionPlanBuilder(flowConfigs, globalConfig),
      groupExecutor = groupExecutor,
      resultProcessor = new FlowResultProcessor(globalConfig, flowConfigs, groupExecutor),
      metadataWriter = new BatchMetadataWriter(globalConfig, flowConfigs),
      executionLogger = new ExecutionLogger(),
      pipelineDefinition = definition,
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
