package com.etl.framework.iceberg

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID

/** Stable identity and effective time for one logical table mutation.
  *
  * The operation ID is reused when the same mutation is resumed. A business replay must use a new batch ID and thus a
  * new operation ID.
  */
case class CommitContext(
    batchId: String,
    targetName: String,
    operationType: String,
    operationId: String,
    effectiveAt: Instant
) {
  val snapshotProperties: Map[String, String] = Map(
    "floe.batch-id" -> batchId,
    "floe.target-name" -> targetName,
    "floe.operation-type" -> operationType,
    "floe.operation-id" -> operationId,
    "floe.effective-at" -> effectiveAt.toString
  )
}

object CommitContext {
  def forFlow(
      batchId: String,
      targetName: String,
      operationType: String,
      effectiveAt: Instant = Instant.now()
  ): CommitContext = {
    val identity = s"${batchId.length}:$batchId${targetName.length}:$targetName${operationType.length}:$operationType"
    val operationId = UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)).toString
    CommitContext(batchId, targetName, operationType, operationId, effectiveAt)
  }

  private[iceberg] def adHoc(targetName: String, operationType: String): CommitContext = {
    val batchId = s"adhoc-${UUID.randomUUID()}"
    forFlow(batchId, targetName, operationType)
  }
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
