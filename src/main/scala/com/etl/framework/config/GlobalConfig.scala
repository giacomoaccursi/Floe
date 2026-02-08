package com.etl.framework.config

/**
 * Global configuration for the ETL framework
 */
case class GlobalConfig(
                         paths: PathsConfig,
                         processing: ProcessingConfig,
                         performance: PerformanceConfig
                       )

/**
 * Path configuration for input/output directories
 */
case class PathsConfig(
                        fullPath: String,
                        deltaPath: String,
                        inputPath: String,
                        rejectedPath: String,
                        metadataPath: String
                      )

/**
 * Processing configuration
 */
case class ProcessingConfig(
                             batchIdFormat: String,
                             failOnValidationError: Boolean,
                             maxRejectionRate: Double
                           )

/**
 * Performance configuration
 */
case class PerformanceConfig(
                              parallelFlows: Boolean,
                              parallelNodes: Boolean
                            )
