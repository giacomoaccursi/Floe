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
    enableSnapshotExpiration: Boolean = true,
    snapshotRetentionDays: Int = 7,
    enableCompaction: Boolean = true,
    targetFileSizeMb: Int = 128,
    enableOrphanCleanup: Boolean = true,
    orphanRetentionMinutes: Int = 60,
    enableManifestRewrite: Boolean = false
)
