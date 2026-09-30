package com.etl.framework.iceberg

import com.etl.framework.orchestration.{ExecutionRequest, PipelineDefinition}

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID

/** Logical and attempt identity for one table mutation.
  *
  * `logicalOperationId` is stable across approved attempts of the same logical run. `operationId` identifies exactly
  * one attempt and is diagnostic evidence, not a lock, fencing token, or authorization to retry.
  */
case class CommitContext(
    pipelineId: String,
    logicalRunId: String,
    attemptId: String,
    targetName: String,
    operationType: String,
    operationId: String,
    logicalOperationId: String,
    contractVersion: Int,
    codeVersion: String,
    configDigest: String,
    effectiveAt: Instant
) {
  def batchId: String = attemptId

  val snapshotProperties: Map[String, String] = Map(
    "floe.pipeline-id" -> pipelineId,
    "floe.logical-run-id" -> logicalRunId,
    "floe.attempt-id" -> attemptId,
    "floe.target-name" -> targetName,
    "floe.operation-type" -> operationType,
    "floe.operation-id" -> operationId,
    "floe.logical-operation-id" -> logicalOperationId,
    "floe.contract-version" -> contractVersion.toString,
    "floe.code-version" -> codeVersion,
    "floe.config-digest" -> configDigest,
    "floe.effective-at" -> effectiveAt.toString
  )
}

object CommitContext {
  def forFlow(request: ExecutionRequest, targetName: String, operationType: String): CommitContext = {
    val logicalIdentity = framed(request.pipelineId, request.logicalRunId, targetName, operationType)
    val attemptIdentity = framed(request.attemptId, targetName, operationType)
    CommitContext(
      pipelineId = request.pipelineId,
      logicalRunId = request.logicalRunId,
      attemptId = request.attemptId,
      targetName = targetName,
      operationType = operationType,
      operationId = UUID.nameUUIDFromBytes(attemptIdentity.getBytes(StandardCharsets.UTF_8)).toString,
      logicalOperationId = UUID.nameUUIDFromBytes(logicalIdentity.getBytes(StandardCharsets.UTF_8)).toString,
      contractVersion = request.contractVersion,
      codeVersion = request.codeVersion,
      configDigest = request.configDigest,
      effectiveAt = request.effectiveAt
    )
  }

  def forFlow(
      batchId: String,
      targetName: String,
      operationType: String,
      effectiveAt: Instant = Instant.now()
  ): CommitContext = {
    val definition = PipelineDefinition("adhoc", "unversioned", "0" * 64)
    val request = ExecutionRequest(
      definition.pipelineId,
      batchId,
      batchId,
      effectiveAt,
      definition.codeVersion,
      definition.configDigest
    )
    forFlow(request, targetName, operationType)
  }

  private[iceberg] def adHoc(targetName: String, operationType: String): CommitContext = {
    val batchId = s"adhoc-${UUID.randomUUID()}"
    forFlow(batchId, targetName, operationType)
  }

  private def framed(values: String*): String =
    values.map(value => s"${value.length}:$value").mkString
}

case class CommittedSnapshot(
    snapshotId: Long,
    parentSnapshotId: Option[Long],
    committedAtMs: Long,
    manifestListLocation: String,
    summary: Map[String, String]
)

/** The catalog could not prove whether a commit was applied. Retrying this mutation is unsafe. */
case class AmbiguousCommitException(operationId: String, tableName: String, cause: Throwable)
    extends RuntimeException(
      s"Cannot determine whether operation '$operationId' committed to $tableName; reconcile before retrying",
      cause
    )

case class DuplicateOperationCommitException(operationId: String, tableName: String, snapshotIds: Seq[Long])
    extends IllegalStateException(
      s"Operation '$operationId' appears in multiple snapshots of $tableName: ${snapshotIds.mkString(", ")}"
    )
