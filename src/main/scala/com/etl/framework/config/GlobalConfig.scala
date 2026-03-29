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
    maxRejectionRate: Option[Double] = None
)

case class PerformanceConfig(
    parallelFlows: Boolean,
    parallelNodes: Boolean
)
