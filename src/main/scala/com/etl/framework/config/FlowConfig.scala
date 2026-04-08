package com.etl.framework.config

import com.etl.framework.core.FlowTransformation

/** Configuration for a single data flow
  */
case class FlowConfig(
    name: String,
    description: String = "",
    version: String = "",
    owner: String = "",
    source: SourceConfig,
    schema: SchemaConfig,
    loadMode: LoadModeConfig,
    validation: ValidationConfig = ValidationConfig(),
    output: OutputConfig = OutputConfig(),
    dependsOn: Seq[String] = Seq.empty,
    maxRejectionRate: Option[Double] = None,
    preValidationTransformation: Option[FlowTransformation] = None,
    postValidationTransformation: Option[FlowTransformation] = None
)

/** Source configuration
  */
case class SourceConfig(
    `type`: SourceType = SourceType.File,
    path: String,
    format: Option[FileFormat] = None,
    options: Map[String, String] = Map.empty
)

/** Schema configuration
  */
case class SchemaConfig(
    enforceSchema: Boolean,
    allowExtraColumns: Boolean = true,
    columns: Seq[ColumnConfig] = Seq.empty
)

/** Column configuration
  */
case class ColumnConfig(
    name: String,
    `type`: String,
    nullable: Boolean,
    description: String = "",
    sourceColumn: Option[String] = None
)

/** Load mode configuration
  */
case class LoadModeConfig(
    `type`: LoadMode, // "full" | "delta" | "scd2"
    validFromColumn: Option[String] = None,
    validToColumn: Option[String] = None,
    isCurrentColumn: Option[String] = None,
    compareColumns: Seq[String] = Seq.empty,
    detectDeletes: Boolean = false,
    isActiveColumn: Option[String] = None
)

/** Validation configuration
  */
case class ValidationConfig(
    primaryKey: Seq[String] = Seq.empty,
    foreignKeys: Seq[ForeignKeyConfig] = Seq.empty,
    rules: Seq[ValidationRule] = Seq.empty
)

/** Foreign key configuration
  */
case class ForeignKeyConfig(
    columns: Seq[String],
    references: ReferenceConfig,
    onOrphan: OrphanAction = OrphanAction.Warn
) {
  def displayName: String = {
    val cols = columns.mkString(", ")
    val refCols = references.columns.mkString(", ")
    s"($cols) -> ${references.flow}.($refCols)"
  }
}

/** Reference configuration
  */
case class ReferenceConfig(
    flow: String,
    columns: Seq[String]
)

/** Validation rule configuration
  */
case class ValidationRule(
    `type`: ValidationRuleType, // "pk_uniqueness" | "fk_integrity" | "regex" | "range" | "domain" | "custom"
    column: Option[String] = None,
    pattern: Option[String] = None,
    min: Option[String] = None,
    max: Option[String] = None,
    domainName: Option[String] = None,
    `class`: Option[String] = None,
    config: Option[Map[String, String]] = None,
    description: Option[String] = None,
    skipNull: Option[Boolean] = None,
    onFailure: OnFailureAction = OnFailureAction.Reject // "reject" | "warn"
)

/** Output configuration
  */
case class OutputConfig(
    rejectedPath: Option[String] = None,
    sortOrder: Seq[String] = Seq.empty,
    icebergPartitions: Seq[String] = Seq.empty,
    tableProperties: Map[String, String] = Map.empty
)
