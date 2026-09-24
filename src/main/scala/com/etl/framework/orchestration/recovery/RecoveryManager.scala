package com.etl.framework.orchestration.recovery

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.iceberg.IcebergTableManager
import com.etl.framework.orchestration.state.{OperationRecord, OperationStatus, RunStatus, RunStore}
import org.apache.spark.sql.SparkSession

sealed abstract class ReconciliationOutcome(val name: String)
object ReconciliationOutcome {
  case object Committed extends ReconciliationOutcome("COMMITTED")
  case object Absent extends ReconciliationOutcome("ABSENT")
  case object Unknown extends ReconciliationOutcome("UNKNOWN")
  case object Inconsistent extends ReconciliationOutcome("INCONSISTENT")
}

case class ReconciliationItem(
    operation: OperationRecord,
    outcome: ReconciliationOutcome,
    discoveredSnapshotId: Option[Long],
    details: Option[String] = None
)

case class ReconciliationReport(batchId: String, items: Seq[ReconciliationItem], applied: Boolean) {
  def safeToResume: Boolean =
    items.forall(item =>
      item.outcome == ReconciliationOutcome.Committed || item.outcome == ReconciliationOutcome.Absent
    )
}

/** Reconciles durable workflow state with the snapshots that Iceberg actually committed. */
class RecoveryManager(
    globalConfig: GlobalConfig,
    flowConfigs: Seq[FlowConfig],
    derivedTableNames: Seq[String],
    runStore: RunStore
)(implicit spark: SparkSession) {
  private val tableManager = new IcebergTableManager(spark, globalConfig.iceberg)
  private val flowByName = flowConfigs.map(flow => flow.name -> flow).toMap
  private val derived = derivedTableNames.toSet

  def reconcileBatch(batchId: String, applyChanges: Boolean = false): ReconciliationReport = {
    val run = runStore.getRun(batchId).getOrElse(throw new NoSuchElementException(s"Unknown batch '$batchId'"))
    val items = runStore.getOperations(batchId).map(reconcileOperation)
    val report = ReconciliationReport(batchId, items, applied = applyChanges)

    if (applyChanges) {
      if (!runStore.transitionRun(batchId, run.version, RunStatus.Reconciling))
        throw new IllegalStateException(s"Concurrent state transition while reconciling batch '$batchId'")
      items.foreach(applyOutcome)
    }
    report
  }

  private def reconcileOperation(operation: OperationRecord): ReconciliationItem = {
    val tableName = resolveTableName(operation)
    try {
      if (!tableManager.tableExists(tableName))
        return ReconciliationItem(operation, ReconciliationOutcome.Absent, None)
      if (operation.status == OperationStatus.CommittedNoChange) {
        val snapshotIsValid = operation.snapshotId match {
          case Some(snapshotId) => tableManager.snapshotExists(tableName, snapshotId)
          case None             => tableManager.getCurrentSnapshotId(tableName).isEmpty
        }
        return if (snapshotIsValid)
          ReconciliationItem(operation, ReconciliationOutcome.Committed, operation.snapshotId)
        else
          ReconciliationItem(
            operation,
            ReconciliationOutcome.Inconsistent,
            None,
            Some("the snapshot pinned for a no-change operation is no longer addressable")
          )
      }
      val snapshots = tableManager.findSnapshotsByOperationId(tableName, operation.operationId)
      snapshots.size match {
        case 0
            if operation.status == OperationStatus.Committed || operation.status == OperationStatus.ReconciledCommitted =>
          ReconciliationItem(
            operation,
            ReconciliationOutcome.Inconsistent,
            None,
            Some("run store says committed but the operation ID is absent from Iceberg history")
          )
        case 0 => ReconciliationItem(operation, ReconciliationOutcome.Absent, None)
        case 1 => ReconciliationItem(operation, ReconciliationOutcome.Committed, Some(snapshots.head.snapshotId))
        case _ =>
          ReconciliationItem(
            operation,
            ReconciliationOutcome.Inconsistent,
            None,
            Some(s"operation ID occurs in snapshots ${snapshots.map(_.snapshotId).mkString(", ")}")
          )
      }
    } catch {
      case error: Throwable =>
        ReconciliationItem(operation, ReconciliationOutcome.Unknown, None, Some(error.getMessage))
    }
  }

  private def resolveTableName(operation: OperationRecord): String =
    operation.targetType match {
      case "flow" =>
        val flow = flowByName.getOrElse(
          operation.targetName,
          throw new IllegalArgumentException(s"Unknown flow target '${operation.targetName}'")
        )
        tableManager.resolveTableName(flow)
      case "derived" if derived.contains(operation.targetName) =>
        globalConfig.iceberg.fullTableName(operation.targetName)
      case other => throw new IllegalArgumentException(s"Unknown operation target type '$other'")
    }

  private def applyOutcome(item: ReconciliationItem): Unit = {
    val nextStatus = item.outcome match {
      case ReconciliationOutcome.Committed    => OperationStatus.ReconciledCommitted
      case ReconciliationOutcome.Absent       => OperationStatus.ReconciledAbsent
      case ReconciliationOutcome.Unknown      => OperationStatus.UnknownCommit
      case ReconciliationOutcome.Inconsistent => OperationStatus.Inconsistent
    }
    if (
      !runStore.transitionOperation(
        item.operation.batchId,
        item.operation.operationId,
        item.operation.version,
        nextStatus,
        item.discoveredSnapshotId,
        item.details
      )
    )
      throw new IllegalStateException(s"Concurrent state transition for operation '${item.operation.operationId}'")
  }
}
