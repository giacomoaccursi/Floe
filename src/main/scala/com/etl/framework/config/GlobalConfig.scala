package com.etl.framework.config

/**
 * Global configuration for the ETL framework
 */
case class GlobalConfig(
                         paths: PathsConfig,
                         processing: ProcessingConfig,
                         performance: PerformanceConfig,
                         monitoring: MonitoringConfig,
                         security: SecurityConfig
                       )

/**
 * Path configuration for input/output directories
 */
case class PathsConfig(
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

/**
 * Security configuration
 */
case class SecurityConfig(
                           encryptionEnabled: Boolean,
                           kmsKeyId: Option[String],
                           authenticationEnabled: Boolean
                         )
