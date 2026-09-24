package com.etl.framework.orchestration

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig, OrphanAction}
import com.etl.framework.iceberg.{
  IcebergTableManager,
  MaintenanceResult,
  MaintenanceStatus,
  OrphanDetectionResult,
  OrphanDetector
}
import com.etl.framework.io.readers.DataReaderFactory
import com.etl.framework.orchestration.batch.{
  BatchIdGenerator,
  BatchMetadataWriter,
  FlowGroupExecutor,
  QualityMetricsWriter
}
import com.etl.framework.orchestration.flow.FlowResult
import com.etl.framework.orchestration.planning.ExecutionPlanBuilder
import com.etl.framework.orchestration.recovery.{RecoveryManager, SourceFingerprint}
import com.etl.framework.orchestration.state.{
  InMemoryRunStore,
  Lease,
  MaintenanceTaskRecord,
  OperationRecord,
  OperationStatus,
  ReleaseManifest,
  ReleaseManifestBuilder,
  RunRecord,
  RunStatus,
  RunStore
}
import com.etl.framework.iceberg.CommitContext
import com.etl.framework.pipeline.{DerivedTableContext, DerivedTableExecutor, DerivedTableResult}
import com.etl.framework.validation.Validator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.time.Instant
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
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
    runStore: RunStore = new InMemoryRunStore()
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Builds execution plan based on FK dependencies.
    */
  def buildExecutionPlan(): ExecutionPlan = planBuilder.build()

  /** Executes all flows in correct order.
    */
  def execute(): IngestionResult = {
    startNewRun(replayOf = None)
  }

  /** Starts a new batch over the exact immutable inputs of an earlier terminal batch. */
  def replay(originalBatchId: String): IngestionResult = {
    runStore.initialize()
    val original = runStore
      .getRun(originalBatchId)
      .getOrElse(throw new NoSuchElementException(s"Unknown batch '$originalBatchId'"))
    require(original.status.terminal, s"Batch '$originalBatchId' is not terminal and must be resumed, not replayed")
    val currentPipelineId = ReleaseManifest.pipelineId(globalConfig, flowConfigs, derivedTables.map(_._1))
    require(
      original.pipelineId == currentPipelineId,
      s"Pipeline definition changed since batch '$originalBatchId'; replay would not be deterministic"
    )
    validateInputFingerprints(originalBatchId, flowConfigs.map(_.name).toSet)
    startNewRun(replayOf = Some(originalBatchId))
  }

  private def startNewRun(replayOf: Option[String]): IngestionResult = {
    val batchId = BatchIdGenerator.generate(globalConfig.processing.batchIdFormat)
    val effectiveAt = Instant.now()
    val pipelineId = ReleaseManifest.pipelineId(globalConfig, flowConfigs, derivedTables.map(_._1))

    runStore.initialize()
    runStore.createRun(RunRecord.planned(batchId, pipelineId, effectiveAt, replayOf))
    createOperationRecords(batchId, effectiveAt)
    executeRun(batchId, pipelineId, effectiveAt, resume = false)
  }

  /** Reconciles and resumes an existing non-published batch. Already committed operations are never executed again. */
  def resume(batchId: String): IngestionResult = {
    runStore.initialize()
    val run = runStore.getRun(batchId).getOrElse(throw new NoSuchElementException(s"Unknown batch '$batchId'"))
    require(
      run.status != RunStatus.Published && run.status != RunStatus.SucceededWithWarnings,
      s"Batch '$batchId' is already published with status ${run.status.name}"
    )
    val currentPipelineId = ReleaseManifest.pipelineId(globalConfig, flowConfigs, derivedTables.map(_._1))
    require(
      run.pipelineId == currentPipelineId,
      s"Pipeline definition changed since batch '$batchId'; resume would not be deterministic"
    )

    val leaseOwner = UUID.randomUUID().toString
    val lease = runStore
      .acquireLease(batchId, leaseOwner, Instant.now(), Duration.ofMinutes(30))
      .getOrElse(throw new IllegalStateException(s"Could not acquire recovery lease for batch $batchId"))
    var handedToExecutor = false
    try {
      val reconciliation = new RecoveryManager(
        globalConfig,
        flowConfigs,
        derivedTables.map(_._1),
        runStore
      ).reconcileBatch(batchId, applyChanges = true)
      require(reconciliation.safeToResume, s"Batch '$batchId' still contains unknown or inconsistent commits")
      validateResumeInputs(batchId)
      handedToExecutor = true
      executeRun(batchId, currentPipelineId, run.effectiveAt, resume = true, preAcquiredLease = Some(lease))
    } finally {
      if (!handedToExecutor) {
        runStore.releaseLease(batchId, lease.owner, lease.fencingToken)
        ()
      }
    }
  }

  private def executeRun(
      batchId: String,
      pipelineId: String,
      effectiveAt: Instant,
      resume: Boolean,
      preAcquiredLease: Option[Lease] = None
  ): IngestionResult = {
    val leaseOwner = UUID.randomUUID().toString
    val startTime = System.nanoTime()

    val leaseTtl = Duration.ofMinutes(30)
    val lease = preAcquiredLease.getOrElse(
      runStore
        .acquireLease(batchId, leaseOwner, Instant.now(), leaseTtl)
        .getOrElse(throw new IllegalStateException(s"Could not acquire execution lease for batch $batchId"))
    )
    val leaseHealthy = new AtomicBoolean(true)
    val heartbeat = Executors.newSingleThreadScheduledExecutor()
    heartbeat.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit =
          try {
            if (!runStore.renewLease(batchId, lease.owner, lease.fencingToken, Instant.now(), leaseTtl))
              leaseHealthy.set(false)
          } catch {
            case error: Exception =>
              leaseHealthy.set(false)
              logger.error(s"Lease heartbeat failed for batch $batchId: ${error.getMessage}", error)
          }
      },
      5L,
      5L,
      TimeUnit.MINUTES
    )

    try {
      transitionRun(batchId, RunStatus.Running)
      executionLogger.logBatchStart(batchId, flowConfigs.size)

      var state = BatchState(Seq.empty, Map.empty)

      val result =
        try {
          val plan = buildExecutionPlan()
          var stoppedResult: Option[IngestionResult] = None

          plan.groups.foreach { group =>
            if (stoppedResult.isEmpty) {
              require(leaseHealthy.get(), s"Execution lease was lost for batch $batchId")
              executionLogger.logGroupStart(group)

              val groupResults = executeOrRecoverGroup(group, batchId, state.validatedFlows, effectiveAt, resume)

              resultProcessor.processGroupResults(groupResults, state, batchId) match {
                case resultProcessor.StopExecution(r)       => stoppedResult = Some(r)
                case resultProcessor.ContinueWith(newState) => state = newState
              }
            }
          }

          stoppedResult match {
            case Some(failed) => finalizeStoppedResult(failed, startTime)
            case None =>
              createSuccessResult(batchId, pipelineId, effectiveAt, state.flowResults, startTime, plan, resume)
          }

        } catch {
          case e: Exception =>
            handleExecutionFailure(batchId, state.flowResults, startTime, e)
        }

      require(leaseHealthy.get(), s"Execution lease was lost for batch $batchId; refusing to publish")
      val runStatus = persistFinalRunState(result, pipelineId, effectiveAt)
      val finalResult = result.copy(status = runStatus)
      notifyListeners(finalResult)
      finalResult
    } finally {
      heartbeat.shutdownNow()
      threadPool.foreach(_.shutdown())
      if (!runStore.releaseLease(batchId, lease.owner, lease.fencingToken))
        logger.warn(s"Execution lease for batch $batchId was no longer owned by ${lease.owner}")
    }
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

  private def executeOrRecoverGroup(
      group: ExecutionGroup,
      batchId: String,
      validatedFlows: Map[String, DataFrame],
      effectiveAt: Instant,
      resume: Boolean
  ): Seq[FlowResult] = {
    if (!resume) {
      markGroupRunning(batchId, effectiveAt, group)
      val results = executeGroup(group, batchId, validatedFlows, effectiveAt)
      recordFlowResults(batchId, effectiveAt, results)
      results
    } else {
      val operations = runStore.getOperations(batchId).map(operation => operation.targetName -> operation).toMap
      val (completed, pending) = group.flows.partition { flow =>
        val status =
          operations.getOrElse(flow.name, throw new IllegalStateException(s"Missing state for ${flow.name}")).status
        Set[OperationStatus](
          OperationStatus.Committed,
          OperationStatus.CommittedNoChange,
          OperationStatus.ReconciledCommitted
        ).contains(status)
      }
      val recovered = completed.map(flow => recoverFlowResult(flow, batchId, operations(flow.name)))
      val executed =
        if (pending.isEmpty) Seq.empty
        else {
          val pendingGroup = group.copy(flows = pending)
          markGroupRunning(batchId, effectiveAt, pendingGroup)
          val results = executeGroup(pendingGroup, batchId, validatedFlows, effectiveAt)
          recordFlowResults(batchId, effectiveAt, results)
          results
        }
      val byName = (recovered ++ executed).map(result => result.flowName -> result).toMap
      group.flows.map(flow => byName(flow.name))
    }
  }

  private def recoverFlowResult(
      flow: FlowConfig,
      batchId: String,
      operation: OperationRecord
  ): FlowResult = {
    val tableManager = new IcebergTableManager(spark, globalConfig.iceberg)
    val snapshotId = operation.snapshotId.orElse(tableManager.getCurrentSnapshotId(flow))
    val metadata = snapshotId.flatMap(
      tableManager.getSnapshotMetadata(flow, _, recordsWritten = 0L, batchId = batchId)
    )
    FlowResult
      .success(flow.name, batchId, 0L, 0L, 0L, 0L, Map.empty, metadata)
      .copy(writeAttempted = true, retryable = false)
  }

  /** Creates a successful ingestion result.
    */
  private def createSuccessResult(
      batchId: String,
      pipelineId: String,
      effectiveAt: Instant,
      flowResults: Seq[FlowResult],
      startTime: Long,
      plan: ExecutionPlan,
      resume: Boolean
  ): IngestionResult = {
    // Run orphan detection BEFORE maintenance (maintenance may expire snapshots needed for time travel)
    val orphanResult = runPostBatchOrphanDetection(flowResults, plan)
    val orphanError = orphanResult match {
      case OrphanDetectionResult.Failed(err, _) => Some(err)
      case _                                    => None
    }
    val derivedResults = if (derivedTables.nonEmpty && orphanError.isEmpty) {
      logger.info(s"Executing ${derivedTables.size} derived tables before finalizing batch $batchId")
      executeOrRecoverDerived(batchId, effectiveAt, resume)
    } else Seq.empty
    val derivedFailures = derivedResults.filterNot(_.success)
    val failureMessages = orphanError.map(err => s"Orphan detection failed: $err").toSeq ++
      (if (derivedFailures.nonEmpty)
         Seq(s"Derived tables failed: ${derivedFailures.map(_.tableName).mkString(", ")}")
       else Seq.empty)
    val batchSuccess = failureMessages.isEmpty
    val batchError = if (batchSuccess) None else Some(failureMessages.mkString("; "))
    val maintenanceResults =
      if (batchSuccess) enqueuePostBatchMaintenance(batchId)
      else {
        logger.warn(s"Skipping maintenance scheduling for failed batch $batchId")
        Seq.empty
      }
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

  private def executeOrRecoverDerived(
      batchId: String,
      effectiveAt: Instant,
      resume: Boolean
  ): Seq[DerivedTableResult] = {
    if (!resume) {
      markDerivedRunning(batchId, effectiveAt, derivedTables.map(_._1).toSet)
      val results = new DerivedTableExecutor(globalConfig.iceberg).execute(derivedTables, batchId, effectiveAt)
      recordDerivedResults(batchId, effectiveAt, results)
      results
    } else {
      val operations =
        runStore.getOperations(batchId).filter(_.targetType == "derived").map(op => op.targetName -> op).toMap
      val (completed, pending) = derivedTables.partition { case (name, _) =>
        val status = operations.getOrElse(name, throw new IllegalStateException(s"Missing state for $name")).status
        Set[OperationStatus](
          OperationStatus.Committed,
          OperationStatus.CommittedNoChange,
          OperationStatus.ReconciledCommitted
        ).contains(status)
      }
      val recovered = completed.map { case (name, _) =>
        val operation = operations(name)
        DerivedTableResult(
          tableName = name,
          success = true,
          snapshotId = operation.snapshotId,
          operationId = Some(operation.operationId),
          reconciled = true
        )
      }
      val executed =
        if (pending.isEmpty) Seq.empty
        else {
          markDerivedRunning(batchId, effectiveAt, pending.map(_._1).toSet)
          val results = new DerivedTableExecutor(globalConfig.iceberg).execute(pending, batchId, effectiveAt)
          recordDerivedResults(batchId, effectiveAt, results)
          results
        }
      val byName = (recovered ++ executed).map(result => result.tableName -> result).toMap
      derivedTables.map { case (name, _) => byName(name) }
    }
  }

  private def createOperationRecords(batchId: String, effectiveAt: Instant): Unit = {
    flowConfigs.foreach { flow =>
      val context = CommitContext.forFlow(batchId, flow.name, flow.loadMode.`type`.name, effectiveAt)
      val fingerprint = SourceFingerprint.compute(flow, spark.sparkContext.hadoopConfiguration)
      runStore.createOperation(
        OperationRecord.pending(batchId, context.operationId, flow.name, "flow").copy(inputFingerprint = fingerprint)
      )
    }
    derivedTables.foreach { case (name, _) =>
      val context = CommitContext.forFlow(batchId, name, "derived-full", effectiveAt)
      runStore.createOperation(OperationRecord.pending(batchId, context.operationId, name, "derived"))
    }
  }

  private def markGroupRunning(batchId: String, effectiveAt: Instant, group: ExecutionGroup): Unit =
    group.flows.foreach { flow =>
      val operationId = CommitContext.forFlow(batchId, flow.name, flow.loadMode.`type`.name, effectiveAt).operationId
      transitionOperation(batchId, operationId, OperationStatus.Running)
    }

  private def markDerivedRunning(batchId: String, effectiveAt: Instant, names: Set[String]): Unit =
    derivedTables.filter { case (name, _) => names.contains(name) }.foreach { case (name, _) =>
      val operationId = CommitContext.forFlow(batchId, name, "derived-full", effectiveAt).operationId
      transitionOperation(batchId, operationId, OperationStatus.Running)
    }

  private def validateResumeInputs(batchId: String): Unit = {
    val operations = runStore.getOperations(batchId).map(operation => operation.targetName -> operation).toMap
    val executableStatuses = Set[OperationStatus](
      OperationStatus.Pending,
      OperationStatus.FailedPreWrite,
      OperationStatus.ReconciledAbsent
    )

    val names = flowConfigs.collect {
      case flow
          if executableStatuses.contains(
            operations
              .getOrElse(
                flow.name,
                throw new IllegalStateException(s"Missing state for flow '${flow.name}'")
              )
              .status
          ) =>
        flow.name
    }.toSet
    validateInputFingerprints(batchId, names)
  }

  private def validateInputFingerprints(batchId: String, names: Set[String]): Unit = {
    val operations = runStore.getOperations(batchId).map(operation => operation.targetName -> operation).toMap
    flowConfigs.filter(flow => names.contains(flow.name)).foreach { flow =>
      val operation = operations.getOrElse(
        flow.name,
        throw new IllegalStateException(s"Missing state for flow '${flow.name}'")
      )
      val current = SourceFingerprint.compute(flow, spark.sparkContext.hadoopConfiguration)
      require(
        operation.inputFingerprint.isDefined,
        s"Flow '${flow.name}' cannot be recovered safely: the original input was not fingerprinted. " +
          "Configure source.options.replayToken for JDBC and custom sources."
      )
      require(
        current == operation.inputFingerprint,
        s"Flow '${flow.name}' cannot be recovered safely: its input changed since batch '$batchId' started"
      )
    }
  }

  private def recordFlowResults(batchId: String, effectiveAt: Instant, results: Seq[FlowResult]): Unit =
    results.foreach { result =>
      val config = flowConfigs
        .find(_.name == result.flowName)
        .getOrElse(
          throw new IllegalArgumentException(s"No configuration found for flow ${result.flowName}")
        )
      val operationId = CommitContext
        .forFlow(batchId, result.flowName, config.loadMode.`type`.name, effectiveAt)
        .operationId
      val status =
        if (result.icebergMetadata.isDefined) OperationStatus.Committed
        else if (result.success && result.writeAttempted) OperationStatus.CommittedNoChange
        else if (result.success) OperationStatus.CommittedNoChange
        else if (result.writeAttempted) OperationStatus.UnknownCommit
        else OperationStatus.FailedPreWrite
      val publishedSnapshotId = result.icebergMetadata.map(_.snapshotId).orElse {
        if (result.success && result.writeAttempted)
          new IcebergTableManager(spark, globalConfig.iceberg).getCurrentSnapshotId(config)
        else None
      }
      transitionOperation(
        batchId,
        operationId,
        status,
        publishedSnapshotId,
        result.error
      )
    }

  private def recordDerivedResults(batchId: String, effectiveAt: Instant, results: Seq[DerivedTableResult]): Unit =
    results.foreach { result =>
      val operationId = CommitContext.forFlow(batchId, result.tableName, "derived-full", effectiveAt).operationId
      val status =
        if (result.success && result.snapshotId.isDefined) OperationStatus.Committed
        else if (result.success) OperationStatus.CommittedNoChange
        else OperationStatus.FailedPreWrite
      transitionOperation(batchId, operationId, status, result.snapshotId, result.error)
    }

  private def transitionOperation(
      batchId: String,
      operationId: String,
      status: OperationStatus,
      snapshotId: Option[Long] = None,
      error: Option[String] = None
  ): Unit = {
    val current = runStore
      .getOperations(batchId)
      .find(_.operationId == operationId)
      .getOrElse(throw new IllegalStateException(s"Missing operation '$operationId' in run store"))
    if (!runStore.transitionOperation(batchId, operationId, current.version, status, snapshotId, error))
      throw new IllegalStateException(s"Concurrent state transition for operation '$operationId'")
  }

  private def transitionRun(
      batchId: String,
      status: RunStatus,
      manifest: Option[String] = None,
      error: Option[String] = None
  ): Unit = {
    val current = runStore.getRun(batchId).getOrElse(throw new IllegalStateException(s"Missing batch '$batchId'"))
    if (!runStore.transitionRun(batchId, current.version, status, manifest, error))
      throw new IllegalStateException(s"Concurrent state transition for batch '$batchId'")
  }

  private def persistFinalRunState(
      result: IngestionResult,
      pipelineId: String,
      effectiveAt: Instant
  ): RunStatus = {
    val operations = runStore.getOperations(result.batchId)
    val hasCommitted = operations.exists(operation =>
      Set[OperationStatus](
        OperationStatus.Committed,
        OperationStatus.CommittedNoChange,
        OperationStatus.ReconciledCommitted
      )
        .contains(operation.status)
    )
    val hasUnknown = operations.exists(_.status == OperationStatus.UnknownCommit)

    if (result.success) {
      val manifest = new ReleaseManifestBuilder(globalConfig, flowConfigs)
        .build(result.batchId, pipelineId, effectiveAt, result.flowResults, result.derivedTableResults, operations)
      val status =
        if (
          result.maintenanceResults
            .exists(_.status == MaintenanceStatus.Failed) || result.flowResults.exists(_.warnings.nonEmpty)
        )
          RunStatus.SucceededWithWarnings
        else RunStatus.Published
      transitionRun(result.batchId, status, Some(ReleaseManifest.toJson(manifest)), result.error)
      status
    } else {
      val status =
        if (hasUnknown) RunStatus.Unknown else if (hasCommitted) RunStatus.FailedPartial else RunStatus.Failed
      transitionRun(result.batchId, status, error = result.error)
      status
    }
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

  private def enqueuePostBatchMaintenance(batchId: String): Seq[MaintenanceResult] = {
    val flowTargets = flowConfigs.map(flow =>
      (flow.name, "flow", new IcebergTableManager(spark, globalConfig.iceberg).resolveTableName(flow))
    )
    val derivedTargets = derivedTables.map { case (name, _) =>
      (name, "derived", globalConfig.iceberg.fullTableName(name))
    }
    (flowTargets ++ derivedTargets).map { case (name, targetType, tableName) =>
      runStore.enqueueMaintenance(MaintenanceTaskRecord.queued(batchId, name, targetType, tableName))
      MaintenanceResult(name, targetType, MaintenanceStatus.Queued)
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
      runStore: RunStore = new InMemoryRunStore()
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
      runStore = runStore
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
    maintenanceResults: Seq[MaintenanceResult] = Seq.empty,
    status: RunStatus = RunStatus.Unknown
)
