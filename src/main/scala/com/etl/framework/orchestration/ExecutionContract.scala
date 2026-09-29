package com.etl.framework.orchestration

import java.time.Instant
import java.util.UUID

sealed trait DataOutcome { def name: String }
object DataOutcome {
  case object NotAttempted extends DataOutcome { val name = "NOT_ATTEMPTED" }
  case object NoChange extends DataOutcome { val name = "NO_CHANGE" }
  case object Committed extends DataOutcome { val name = "COMMITTED" }
  case object Unknown extends DataOutcome { val name = "UNKNOWN" }
}

sealed trait ExecutionStatus { def name: String; def terminal: Boolean }
object ExecutionStatus {
  case object Succeeded extends ExecutionStatus { val name = "SUCCEEDED"; val terminal = true }
  case object SucceededWithWarnings extends ExecutionStatus {
    val name = "SUCCEEDED_WITH_WARNINGS"
    val terminal = true
  }
  case object Failed extends ExecutionStatus { val name = "FAILED"; val terminal = true }
  case object FailedPartial extends ExecutionStatus { val name = "FAILED_PARTIAL"; val terminal = true }
  case object Unknown extends ExecutionStatus { val name = "UNKNOWN"; val terminal = false }
}

final case class ExecutionRequest(
    pipelineId: String,
    logicalRunId: String,
    attemptId: String,
    effectiveAt: Instant,
    contractVersion: Int = ExecutionRequest.CurrentContractVersion
) {
  ExecutionRequest.validateIdentifier("pipelineId", pipelineId)
  ExecutionRequest.validateIdentifier("logicalRunId", logicalRunId)
  ExecutionRequest.validateIdentifier("attemptId", attemptId)
  require(effectiveAt != null, "effectiveAt is required")
  require(
    contractVersion == ExecutionRequest.CurrentContractVersion,
    s"Unsupported execution contract version $contractVersion"
  )
}

object ExecutionRequest {
  val CurrentContractVersion: Int = 1
  private val Identifier = "[A-Za-z0-9][A-Za-z0-9._:-]{0,127}".r

  def create(pipelineId: String, logicalRunId: String, effectiveAt: Instant): ExecutionRequest =
    ExecutionRequest(
      pipelineId = pipelineId,
      logicalRunId = logicalRunId,
      attemptId = UUID.randomUUID().toString,
      effectiveAt = effectiveAt
    )

  def validateIdentifier(field: String, value: String): Unit = {
    require(value != null && Identifier.pattern.matcher(value).matches(), s"Invalid $field")
    require(value != "." && value != "..", s"Invalid $field")
  }
}
