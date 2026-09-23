package com.etl.framework.orchestration

import com.etl.framework.config.{FlowConfig, GlobalConfig}

/** Shared rejection-threshold policy used before writes and when processing flow results. */
object RejectionThresholdPolicy {
  def threshold(flowConfig: FlowConfig, globalConfig: GlobalConfig): Option[Double] =
    flowConfig.maxRejectionRate.orElse(globalConfig.processing.maxRejectionRate)

  def exceeded(
      rejectionRate: Double,
      rejectedRecords: Long,
      flowConfig: FlowConfig,
      globalConfig: GlobalConfig
  ): Boolean =
    rejectedRecords > 0 && threshold(flowConfig, globalConfig).exists(rejectionRate > _)
}
