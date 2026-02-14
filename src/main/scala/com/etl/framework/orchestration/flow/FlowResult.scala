package com.etl.framework.orchestration.flow

import com.etl.framework.iceberg.IcebergFlowMetadata

/**
 * Result of flow execution
 */
case class FlowResult(
  flowName: String,
  batchId: String,
  success: Boolean,
  inputRecords: Long = 0,
  mergedRecords: Long = 0,
  validRecords: Long = 0,
  rejectedRecords: Long = 0,
  rejectionRate: Double = 0.0,
  executionTimeMs: Long = 0,
  rejectionReasons: Map[String, Long] = Map.empty,
  error: Option[String] = None,
  icebergMetadata: Option[IcebergFlowMetadata] = None
)

/**
 * Factory for creating FlowResult instances
 */
object FlowResult {
  
  /**
   * Creates a successful FlowResult
   */
  def success(
    flowName: String,
    batchId: String,
    inputRecords: Long,
    mergedRecords: Long,
    validRecords: Long,
    rejectedRecords: Long,
    rejectionReasons: Map[String, Long],
    icebergMetadata: Option[IcebergFlowMetadata] = None
  ): FlowResult = {
    FlowResult(
      flowName = flowName,
      batchId = batchId,
      success = true,
      inputRecords = inputRecords,
      mergedRecords = mergedRecords,
      validRecords = validRecords,
      rejectedRecords = rejectedRecords,
      rejectionRate = if (inputRecords > 0) rejectedRecords.toDouble / inputRecords else 0.0,
      executionTimeMs = 0L, // Will be set by caller
      rejectionReasons = rejectionReasons,
      icebergMetadata = icebergMetadata
    )
  }
  
  /**
   * Creates a failed FlowResult
   */
  def failure(
    flowName: String,
    batchId: String,
    error: String
  ): FlowResult = {
    FlowResult(
      flowName = flowName,
      batchId = batchId,
      success = false,
      error = Some(error)
    )
  }
}
