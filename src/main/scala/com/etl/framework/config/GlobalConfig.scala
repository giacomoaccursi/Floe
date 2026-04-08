package com.etl.framework.config

case class GlobalConfig(
    paths: PathsConfig,
    processing: ProcessingConfig = ProcessingConfig(),
    performance: PerformanceConfig,
    iceberg: IcebergConfig
)

case class PathsConfig(
    outputPath: String,
    rejectedPath: String,
    metadataPath: String,
    warningsPath: Option[String] = None
)

case class ProcessingConfig(
    batchIdFormat: String = "yyyyMMdd_HHmmss",
    maxRejectionRate: Option[Double] = None,
    maxRetries: Int = 0,
    retryBackoffMs: Long = 1000,
    qualityMetricsTable: Option[String] = None
)

case class PerformanceConfig(
    parallelFlows: Boolean = false
)
