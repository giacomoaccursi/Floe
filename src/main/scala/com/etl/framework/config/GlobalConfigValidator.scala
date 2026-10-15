package com.etl.framework.config

import com.etl.framework.util.ManagedTableName
import java.time.format.DateTimeFormatter
import scala.util.Try

/** Pure structural validation for global settings, shared by builder and direct execution APIs. */
object GlobalConfigValidator {

  def validate(config: GlobalConfig): Seq[String] = {
    val errors = Vector.newBuilder[String]
    val requiredPaths = Seq(
      "outputPath" -> config.paths.outputPath,
      "rejectedPath" -> config.paths.rejectedPath,
      "metadataPath" -> config.paths.metadataPath
    )
    requiredPaths.foreach { case (name, value) =>
      if (value.trim.isEmpty) errors += s"paths.$name must not be blank"
    }
    if (config.paths.warningsPath.exists(_.trim.isEmpty))
      errors += "paths.warningsPath must not be blank when configured"

    val batchIdFormat = config.processing.batchIdFormat
    if (batchIdFormat.trim.isEmpty) errors += "processing.batchIdFormat must not be blank"
    else if (
      batchIdFormat != "timestamp" && batchIdFormat != "datetime" &&
      Try(DateTimeFormatter.ofPattern(batchIdFormat)).isFailure
    ) errors += "processing.batchIdFormat is not a valid DateTimeFormatter pattern"

    config.processing.qualityMetricsTable.foreach { tableName =>
      if (tableName.trim.isEmpty)
        errors += "processing.qualityMetricsTable must not be blank"
      else if (tableName != tableName.trim)
        errors += "processing.qualityMetricsTable must not have leading or trailing whitespace"
      if (tableName.contains("."))
        errors += "processing.qualityMetricsTable must be unqualified; configure catalog and namespace globally"
      if (ManagedTableName.containsUnsafeCharacters(tableName))
        errors += s"processing.qualityMetricsTable ${ManagedTableName.unsafeCharactersMessage}"
    }
    config.processing.maxRejectionRate.foreach { rate =>
      if (!java.lang.Double.isFinite(rate)) errors += "processing.maxRejectionRate must be finite"
      else if (rate < 0.0 || rate > 1.0)
        errors += "processing.maxRejectionRate must be between 0.0 and 1.0"
    }
    if (config.performance.maxParallelFlows <= 0)
      errors += "performance.maxParallelFlows must be greater than zero"

    if (config.iceberg.catalogName.trim.isEmpty) errors += "iceberg.catalogName must not be blank"
    if (config.iceberg.namespace.trim.isEmpty) errors += "iceberg.namespace must not be blank"
    if (!Set("avro", "orc", "parquet").contains(config.iceberg.fileFormat))
      errors += "iceberg.fileFormat must be one of: avro, orc, parquet"
    if (config.iceberg.catalogProperties.keys.exists(_.trim.isEmpty))
      errors += "iceberg.catalogProperties keys must not be blank"
    config.iceberg.maintenance.validationErrors.foreach(error => errors += s"Iceberg maintenance: $error")

    errors.result()
  }

  def requireValid(config: GlobalConfig): Unit = {
    val errors = validate(config)
    require(errors.isEmpty, s"Invalid global configuration: ${errors.mkString("; ")}")
  }
}
