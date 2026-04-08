package com.etl.framework

import com.etl.framework.config._

object TestFixtures {

  def flowConfig(
      name: String,
      loadMode: LoadMode = LoadMode.Full,
      primaryKey: Seq[String] = Seq("id"),
      foreignKeys: Seq[ForeignKeyConfig] = Seq.empty,
      columns: Seq[ColumnConfig] = Seq.empty,
      enforceSchema: Boolean = false,
      allowExtraColumns: Boolean = true,
      sourcePath: String = "/tmp/test",
      format: Option[FileFormat] = Some(FileFormat.CSV),
      rules: Seq[ValidationRule] = Seq.empty,
      output: OutputConfig = OutputConfig(),
      detectDeletes: Boolean = false,
      compareColumns: Seq[String] = Seq.empty,
      dependsOn: Seq[String] = Seq.empty,
      maxRejectionRate: Option[Double] = None
  ): FlowConfig = FlowConfig(
    name = name,
    source = SourceConfig(path = sourcePath, format = format),
    schema = SchemaConfig(
      enforceSchema = enforceSchema,
      allowExtraColumns = allowExtraColumns,
      columns = columns
    ),
    loadMode = LoadModeConfig(
      `type` = loadMode,
      compareColumns =
        if (compareColumns.nonEmpty) compareColumns
        else if (loadMode == LoadMode.SCD2) Seq("name")
        else Seq.empty,
      validFromColumn = if (loadMode == LoadMode.SCD2) Some("valid_from") else None,
      validToColumn = if (loadMode == LoadMode.SCD2) Some("valid_to") else None,
      isCurrentColumn = if (loadMode == LoadMode.SCD2) Some("is_current") else None,
      detectDeletes = detectDeletes,
      isActiveColumn = if (detectDeletes) Some("is_active") else None
    ),
    validation = ValidationConfig(
      primaryKey = primaryKey,
      foreignKeys = foreignKeys,
      rules = rules
    ),
    output = output,
    dependsOn = dependsOn,
    maxRejectionRate = maxRejectionRate
  )

  def globalConfig(
      outputPath: String = "/tmp/test/output",
      rejectedPath: String = "/tmp/test/rejected",
      metadataPath: String = "/tmp/test/metadata",
      parallelFlows: Boolean = false,
      batchIdFormat: String = "yyyyMMdd_HHmmss",
      maxRejectionRate: Option[Double] = None,
      iceberg: IcebergConfig = IcebergConfig(warehouse = "/tmp/test/warehouse")
  ): GlobalConfig = GlobalConfig(
    paths = PathsConfig(outputPath, rejectedPath, metadataPath),
    processing = ProcessingConfig(batchIdFormat, maxRejectionRate),
    performance = PerformanceConfig(parallelFlows),
    iceberg = iceberg
  )
}
