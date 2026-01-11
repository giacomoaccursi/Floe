package com.etl.framework.config

/**
 * Global configuration for the ETL framework
 */
case class GlobalConfig(
                         spark: SparkConfig,
                         paths: PathsConfig,
                         processing: ProcessingConfig,
                         performance: PerformanceConfig,
                         monitoring: MonitoringConfig,
                       )

/**
 * Spark configuration
 */
case class SparkConfig(
                        appName: String,
                        master: String,
                        config: Map[String, String]
                      )

/**
 * Path configuration for input/output directories
 */
case class PathsConfig(
                        inputBase: String,
                        outputBase: String,
                        validatedPath: String,
                        rejectedPath: String,
                        metadataPath: String,
                        modelPath: String,
                        stagingPath: String,
                        checkpointPath: String
                      )

/**
 * Processing configuration
 */
case class ProcessingConfig(
                             batchIdFormat: String,
                             executionMode: String,
                             failOnValidationError: Boolean,
                             maxRejectionRate: Double,
                             rollbackOnFailure: Boolean,
                             checkpointEnabled: Boolean,
                             checkpointInterval: String
                           )

/**
 * Performance configuration
 */
case class PerformanceConfig(
                              parallelFlows: Boolean,
                              parallelNodes: Boolean,
                              broadcastThreshold: Long,
                              cacheValidated: Boolean,
                              shufflePartitions: Int
                            )

/**
 * Monitoring configuration
 */
case class MonitoringConfig(
                             enabled: Boolean,
                             metricsExporter: Option[String],
                             metricsEndpoint: Option[String],
                             logLevel: String
                           )

