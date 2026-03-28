package com.etl.framework.config

case class GlobalConfig(
    paths: PathsConfig,
    processing: ProcessingConfig,
    performance: PerformanceConfig,
    iceberg: IcebergConfig
)

case class PathsConfig(
    outputPath: String,
    rejectedPath: String,
    metadataPath: String
)

case class ProcessingConfig(
    batchIdFormat: String,
    failOnValidationError: Boolean,
    maxRejectionRate: Double
)

case class PerformanceConfig(
    parallelFlows: Boolean,
    parallelNodes: Boolean
)
