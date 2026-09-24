package com.etl.framework.orchestration.state

import com.etl.framework.iceberg.MaintenanceStatus
import java.time.Instant

sealed abstract class RunStatus(val name: String, val terminal: Boolean)
object RunStatus {
  case object Planned extends RunStatus("PLANNED", terminal = false)
  case object Running extends RunStatus("RUNNING", terminal = false)
  case object Reconciling extends RunStatus("RECONCILING", terminal = false)
  case object Published extends RunStatus("PUBLISHED", terminal = true)
  case object SucceededWithWarnings extends RunStatus("SUCCEEDED_WITH_WARNINGS", terminal = true)
  case object Failed extends RunStatus("FAILED", terminal = true)
  case object FailedPartial extends RunStatus("FAILED_PARTIAL", terminal = true)
  case object Unknown extends RunStatus("UNKNOWN", terminal = false)

  val values: Seq[RunStatus] = Seq(
    Planned,
    Running,
    Reconciling,
    Published,
    SucceededWithWarnings,
    Failed,
    FailedPartial,
    Unknown
  )

  def fromName(name: String): RunStatus =
    values.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Unknown run status: $name"))
}

sealed abstract class OperationStatus(val name: String, val terminal: Boolean)
object OperationStatus {
  case object Pending extends OperationStatus("PENDING", terminal = false)
  case object Running extends OperationStatus("RUNNING", terminal = false)
  case object Committed extends OperationStatus("COMMITTED", terminal = true)
  case object CommittedNoChange extends OperationStatus("COMMITTED_NO_CHANGE", terminal = true)
  case object FailedPreWrite extends OperationStatus("FAILED_PRE_WRITE", terminal = true)
  case object UnknownCommit extends OperationStatus("UNKNOWN_COMMIT", terminal = false)
  case object ReconciledCommitted extends OperationStatus("RECONCILED_COMMITTED", terminal = true)
  case object ReconciledAbsent extends OperationStatus("RECONCILED_ABSENT", terminal = true)
  case object Inconsistent extends OperationStatus("INCONSISTENT", terminal = true)

  val values: Seq[OperationStatus] = Seq(
    Pending,
    Running,
    Committed,
    CommittedNoChange,
    FailedPreWrite,
    UnknownCommit,
    ReconciledCommitted,
    ReconciledAbsent,
    Inconsistent
  )

  def fromName(name: String): OperationStatus =
    values.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Unknown operation status: $name"))
}

case class Lease(owner: String, expiresAt: Instant, fencingToken: Long)

case class RunRecord(
    batchId: String,
    pipelineId: String,
    effectiveAt: Instant,
    status: RunStatus,
    version: Long,
    lease: Option[Lease] = None,
    replayOf: Option[String] = None,
    releaseManifest: Option[String] = None,
    error: Option[String] = None,
    createdAt: Instant,
    updatedAt: Instant
)

object RunRecord {
  def planned(
      batchId: String,
      pipelineId: String,
      effectiveAt: Instant,
      replayOf: Option[String] = None
  ): RunRecord =
    RunRecord(
      batchId = batchId,
      pipelineId = pipelineId,
      effectiveAt = effectiveAt,
      status = RunStatus.Planned,
      version = 0L,
      replayOf = replayOf,
      createdAt = effectiveAt,
      updatedAt = effectiveAt
    )
}

case class OperationRecord(
    batchId: String,
    operationId: String,
    targetName: String,
    targetType: String,
    status: OperationStatus,
    version: Long,
    snapshotId: Option[Long] = None,
    inputFingerprint: Option[String] = None,
    error: Option[String] = None,
    updatedAt: Instant
)

object OperationRecord {
  def pending(batchId: String, operationId: String, targetName: String, targetType: String): OperationRecord =
    OperationRecord(
      batchId,
      operationId,
      targetName,
      targetType,
      OperationStatus.Pending,
      version = 0L,
      updatedAt = Instant.now()
    )
}

case class MaintenanceTaskRecord(
    batchId: String,
    targetName: String,
    targetType: String,
    tableName: String,
    status: MaintenanceStatus,
    attempts: Int = 0,
    version: Long = 0L,
    error: Option[String] = None,
    updatedAt: Instant = Instant.now()
)

object MaintenanceTaskRecord {
  def queued(batchId: String, targetName: String, targetType: String, tableName: String): MaintenanceTaskRecord =
    MaintenanceTaskRecord(batchId, targetName, targetType, tableName, MaintenanceStatus.Queued)
}
