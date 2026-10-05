package com.etl.framework.orchestration.recovery

import com.etl.framework.config.IcebergConfig
import com.etl.framework.iceberg.{CommitContext, CommittedSnapshot, IcebergTableManager}
import com.etl.framework.orchestration.ExecutionRequest
import com.etl.framework.util.{SensitiveDataRedactor, SqlIdentifier}
import org.apache.hadoop.fs.Path
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.util.SnapshotUtil
import org.apache.spark.sql.SparkSession

import scala.util.control.NonFatal

final case class InspectionTarget(targetName: String, tableName: String, operationType: String)

final case class SnapshotInspection(
    snapshotId: Long,
    parentSnapshotId: Option[Long],
    committedAtMs: Long,
    relationToMain: String
)

final case class TargetInspection(
    targetName: String,
    tableName: String,
    operationType: String,
    operationId: String,
    status: String,
    tableUuid: Option[String] = None,
    currentMainSnapshotId: Option[Long] = None,
    matchingSnapshots: Seq[SnapshotInspection] = Seq.empty,
    error: Option[String] = None
)

final case class ReportInspection(
    path: String,
    status: String,
    lengthBytes: Option[Long] = None,
    error: Option[String] = None
)

/** Read-only evidence for one attempt. `retrySafe` is deliberately always false: inspection facts require an operator
  * decision and cannot prove that no delayed backend effect remains possible.
  */
final case class AttemptInspection(
    request: ExecutionRequest,
    status: String,
    assessment: String,
    complete: Boolean,
    retrySafe: Boolean,
    report: ReportInspection,
    targets: Seq[TargetInspection]
)

class AttemptInspector(spark: SparkSession, icebergConfig: IcebergConfig, metadataPath: String) {

  private val tableManager = new IcebergTableManager(spark, icebergConfig)

  def inspect(request: ExecutionRequest, targets: Seq[InspectionTarget]): AttemptInspection = {
    val targetEvidence = targets.map(inspectTarget(request, _))
    val reportEvidence = inspectReport(request)
    val complete = !targetEvidence.exists(_.status == "INSPECTION_FAILED") &&
      reportEvidence.status != "INSPECTION_FAILED"
    AttemptInspection(
      request = request,
      status = if (complete) "INSPECTION_COMPLETED" else "INSPECTION_INCOMPLETE",
      assessment = assessment(targetEvidence, reportEvidence, complete),
      complete = complete,
      retrySafe = false,
      report = reportEvidence,
      targets = targetEvidence
    )
  }

  private def inspectTarget(request: ExecutionRequest, target: InspectionTarget): TargetInspection = {
    val context = CommitContext.forFlow(request, target.targetName, target.operationType)
    try {
      if (!tableManager.tableExists(target.tableName))
        TargetInspection(
          target.targetName,
          target.tableName,
          target.operationType,
          context.operationId,
          status = "TABLE_MISSING"
        )
      else {
        val table = Spark3Util.loadIcebergTable(spark, SqlIdentifier.quoteMultipart(target.tableName))
        table.refresh()
        val currentSnapshotId = Option(table.currentSnapshot()).map(_.snapshotId())
        val matches = tableManager.findSnapshotsByOperationId(target.tableName, context.operationId)
        val snapshots = matches.map(snapshot => inspectSnapshot(table, currentSnapshotId, snapshot))
        TargetInspection(
          targetName = target.targetName,
          tableName = target.tableName,
          operationType = target.operationType,
          operationId = context.operationId,
          status = matches.size match {
            case 0 => "NO_COMMIT_EVIDENCE"
            case 1 => "COMMIT_OBSERVED"
            case _ => "MULTIPLE_COMMITS_OBSERVED"
          },
          tableUuid = Option(table.uuid()).map(_.toString),
          currentMainSnapshotId = currentSnapshotId,
          matchingSnapshots = snapshots
        )
      }
    } catch {
      case NonFatal(error) =>
        TargetInspection(
          target.targetName,
          target.tableName,
          target.operationType,
          context.operationId,
          status = "INSPECTION_FAILED",
          error = Some(message(error))
        )
    }
  }

  private def inspectSnapshot(
      table: org.apache.iceberg.Table,
      currentSnapshotId: Option[Long],
      snapshot: CommittedSnapshot
  ): SnapshotInspection = {
    val relation = currentSnapshotId match {
      case None                                                                            => "NO_CURRENT_MAIN_SNAPSHOT"
      case Some(current) if current == snapshot.snapshotId                                 => "CURRENT_MAIN"
      case Some(current) if SnapshotUtil.isAncestorOf(table, current, snapshot.snapshotId) => "ANCESTOR_OF_MAIN"
      case Some(_)                                                                         => "NOT_ANCESTOR_OF_MAIN"
    }
    SnapshotInspection(snapshot.snapshotId, snapshot.parentSnapshotId, snapshot.committedAtMs, relation)
  }

  private def inspectReport(request: ExecutionRequest): ReportInspection = {
    val pathValue = s"${metadataPath.stripSuffix("/")}/${request.artifactKey}/summary.json"
    val diagnosticPath = SensitiveDataRedactor.redact(pathValue)
    try {
      val path = new Path(pathValue)
      val filesystem = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      if (!filesystem.exists(path)) ReportInspection(diagnosticPath, "MISSING")
      else {
        val status = filesystem.getFileStatus(path)
        if (status.isFile) ReportInspection(diagnosticPath, "PRESENT", Some(status.getLen))
        else ReportInspection(diagnosticPath, "NOT_A_FILE")
      }
    } catch {
      case NonFatal(error) => ReportInspection(diagnosticPath, "INSPECTION_FAILED", error = Some(message(error)))
    }
  }

  private def assessment(
      targets: Seq[TargetInspection],
      report: ReportInspection,
      complete: Boolean
  ): String =
    if (!complete) "INSPECTION_INCOMPLETE"
    else if (targets.exists(_.status == "MULTIPLE_COMMITS_OBSERVED")) "ANOMALOUS_MULTIPLE_COMMITS"
    else if (targets.isEmpty && report.status == "PRESENT") "NO_TRACKED_MUTATIONS"
    else {
      val observed = targets.count(_.status == "COMMIT_OBSERVED")
      if (targets.nonEmpty && observed == targets.size) "ALL_TARGET_COMMITS_OBSERVED"
      else if (observed > 0) "PARTIAL_COMMIT_EVIDENCE"
      else "NO_COMMIT_EVIDENCE"
    }

  private def message(error: Throwable): String = SensitiveDataRedactor.message(error)
}
