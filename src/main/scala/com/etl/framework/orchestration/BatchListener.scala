package com.etl.framework.orchestration

/** Trait for receiving batch lifecycle notifications. Implement this to send alerts (Slack, SNS, email) on batch
  * completion or failure.
  */
trait BatchListener {

  /** Called when all flows complete successfully. */
  def onBatchCompleted(result: IngestionResult): Unit

  /** Called when any flow fails or the batch is aborted. */
  def onBatchFailed(result: IngestionResult): Unit
}
