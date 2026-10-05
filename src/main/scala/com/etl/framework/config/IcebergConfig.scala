package com.etl.framework.config

case class IcebergConfig(
    catalogMode: CatalogMode = CatalogMode.Existing,
    ddlMode: DdlMode = DdlMode.Validate,
    catalogType: String = "hadoop",
    catalogName: String = "floe",
    namespace: String = "default",
    warehouse: String = "",
    catalogProperties: Map[String, String] = Map.empty,
    fileFormat: String = "parquet",
    enableSnapshotTagging: Boolean = true,
    maintenance: MaintenanceConfig = MaintenanceConfig()
) {
  val formatVersion: Int = 2

  def fullTableName(table: String): String =
    s"$catalogName.$namespace.$table"
}

case class MaintenanceConfig(
    enabled: Boolean = false,
    snapshotRetentionDays: Option[Int] = Some(7),
    targetFileSizeMb: Option[Int] = Some(128),
    orphanRetentionMinutes: Option[Int] = Some(4320),
    enableManifestRewrite: Boolean = false
) {
  def validationErrors: Seq[String] = {
    val errors = Seq.newBuilder[String]
    snapshotRetentionDays.foreach { days =>
      if (days <= 0) errors += "snapshotRetentionDays must be greater than zero"
    }
    targetFileSizeMb.foreach { size =>
      if (size <= 0) errors += "targetFileSizeMb must be greater than zero"
    }
    orphanRetentionMinutes.foreach { minutes =>
      if (minutes < 1440) errors += "orphanRetentionMinutes must be at least 1440"
    }
    if (
      enabled &&
      snapshotRetentionDays.isEmpty &&
      targetFileSizeMb.isEmpty &&
      orphanRetentionMinutes.isEmpty &&
      !enableManifestRewrite
    ) errors += "at least one maintenance action must be configured"
    errors.result()
  }

  def requireRunnable(): Unit = {
    require(enabled, "maintenance.enabled must be true")
    val errors = validationErrors
    require(errors.isEmpty, s"Invalid maintenance configuration: ${errors.mkString("; ")}")
  }
}
