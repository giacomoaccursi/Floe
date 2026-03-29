package com.etl.framework.config

case class IcebergConfig(
    catalogType: String = "hadoop",
    catalogName: String = "spark_catalog",
    warehouse: String,
    catalogProperties: Map[String, String] = Map.empty,
    fileFormat: String = "parquet",
    enableSnapshotTagging: Boolean = true,
    maintenance: MaintenanceConfig = MaintenanceConfig()
) {
  val formatVersion: Int = 2
}

case class MaintenanceConfig(
    snapshotRetentionDays: Option[Int] = Some(7),
    targetFileSizeMb: Option[Int] = Some(128),
    orphanRetentionMinutes: Option[Int] = Some(1440),
    enableManifestRewrite: Boolean = false
)
