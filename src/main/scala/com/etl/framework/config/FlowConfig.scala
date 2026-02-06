package com.etl.framework.config

import com.etl.framework.core.FlowTransformation

/**
 * Configuration for a single data flow
 */
case class FlowConfig(
                       name: String,
                       description: String,
                       version: String,
                       owner: String,
                       source: SourceConfig,
                       schema: SchemaConfig,
                       loadMode: LoadModeConfig,
                       validation: ValidationConfig,
                       output: OutputConfig,
                       preValidationTransformation: Option[FlowTransformation] = None,
                       postValidationTransformation: Option[FlowTransformation] = None
                     )

/**
 * Source configuration
 */
case class SourceConfig(
                         `type`: String,  // "file" | "jdbc" | "api" | "kafka"
                         path: String,
                         format: String,  // "csv" | "parquet" | "json" | "avro"
                         options: Map[String, String],
                         filePattern: Option[String] = None
                       )

/**
 * Schema configuration
 */
case class SchemaConfig(
                         enforceSchema: Boolean,
                         allowExtraColumns: Boolean,
                         columns: Seq[ColumnConfig]
                       )

/**
 * Column configuration
 */
case class ColumnConfig(
                         name: String,
                         `type`: String,
                         nullable: Boolean,
                         default: Option[String] = None,
                         description: String
                       )

/**
 * Load mode configuration
 */
case class LoadModeConfig(
                           `type`: String,  // "full" | "delta" | "scd2"
                           mergeStrategy: Option[String] = None,  // "upsert" | "append"
                           updateTimestampColumn: Option[String] = None,
                           validFromColumn: Option[String] = None,
                           validToColumn: Option[String] = None,
                           isCurrentColumn: Option[String] = None,
                           compareColumns: Seq[String] = Seq.empty
                         )

/**
 * Validation configuration
 */
case class ValidationConfig(
                             primaryKey: Seq[String],
                             foreignKeys: Seq[ForeignKeyConfig],
                             rules: Seq[ValidationRule]
                           )

/**
 * Foreign key configuration
 */
case class ForeignKeyConfig(
                             name: String,
                             column: String,
                             references: ReferenceConfig
                           )

/**
 * Reference configuration
 */
case class ReferenceConfig(
                            flow: String,
                            column: String
                          )

/**
 * Validation rule configuration
 */
case class ValidationRule(
                           `type`: String,  // "pk_uniqueness" | "fk_integrity" | "regex" | "range" | "domain" | "custom"
                           column: Option[String] = None,
                           pattern: Option[String] = None,
                           min: Option[String] = None,
                           max: Option[String] = None,
                           domainName: Option[String] = None,
                           `class`: Option[String] = None,
                           config: Option[Map[String, String]] = None,
                           description: Option[String] = None,
                           skipNull: Option[Boolean] = None,
                           onFailure: String = "reject"  // "reject" | "warn"
                         )

/**
 * Output configuration
 */
case class OutputConfig(
                         path: Option[String] = None,
                         rejectedPath: Option[String] = None,
                         format: String = "parquet",
                         partitionBy: Seq[String] = Seq.empty,
                         compression: String = "snappy",
                         options: Map[String, String] = Map.empty
                       )
