package com.etl.framework.orchestration

trait BatchListener {
  def onBatchCompleted(result: IngestionResult): Unit
  def onBatchFailed(result: IngestionResult): Unit
}
