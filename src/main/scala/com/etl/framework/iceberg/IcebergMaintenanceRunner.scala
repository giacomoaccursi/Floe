package com.etl.framework.iceberg

import com.etl.framework.config.{IcebergConfig, MaintenanceConfig}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class IcebergMaintenanceRunner(spark: SparkSession, icebergConfig: IcebergConfig) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val sqlTimestampFmt = DateTimeFormatter
    .ofPattern("yyyy-MM-dd HH:mm:ss")
    .withZone(ZoneOffset.UTC)

  def run(tableName: String, config: MaintenanceConfig): Unit = {
    logger.info(s"Running maintenance on $tableName")
    config.snapshotRetentionDays.foreach(days => expireSnapshots(tableName, days))
    config.targetFileSizeMb.foreach(size => compactDataFiles(tableName, size))
    config.orphanRetentionMinutes.foreach(mins => removeOrphanFiles(tableName, mins))
    if (config.enableManifestRewrite) rewriteManifests(tableName)
  }

  private def expireSnapshots(tableName: String, retentionDays: Int): Unit = {
    spark.sql(
      s"CALL ${icebergConfig.catalogName}.system.expire_snapshots(" +
        s"table => '$tableName', " +
        s"older_than => TIMESTAMP '${sqlTimestampFmt.format(java.time.Instant.now().minusSeconds(retentionDays.toLong * 86400))}'" +
        s")"
    )
    logger.info(s"Expired snapshots older than $retentionDays days on $tableName")
  }

  private def compactDataFiles(tableName: String, targetFileSizeMb: Int): Unit = {
    spark.sql(
      s"CALL ${icebergConfig.catalogName}.system.rewrite_data_files(" +
        s"table => '$tableName', " +
        s"options => map('target-file-size-bytes', '${targetFileSizeMb.toLong * 1024 * 1024}')" +
        s")"
    )
    logger.info(s"Compacted data files on $tableName (target: ${targetFileSizeMb}MB)")
  }

  private def removeOrphanFiles(tableName: String, retentionMinutes: Int): Unit = {
    val effectiveMinutes = if (retentionMinutes < 1440) {
      logger.warn(
        s"orphanRetentionMinutes=$retentionMinutes is below Iceberg's 24-hour minimum. " +
          s"Clamping to 1440 minutes to prevent data corruption."
      )
      1440
    } else retentionMinutes
    spark.sql(
      s"CALL ${icebergConfig.catalogName}.system.remove_orphan_files(" +
        s"table => '$tableName', " +
        s"older_than => TIMESTAMP '${sqlTimestampFmt.format(java.time.Instant.now().minusSeconds(effectiveMinutes.toLong * 60))}'" +
        s")"
    )
    logger.info(s"Removed orphan files older than ${effectiveMinutes}min on $tableName")
  }

  private def rewriteManifests(tableName: String): Unit = {
    spark.sql(s"CALL ${icebergConfig.catalogName}.system.rewrite_manifests('$tableName')")
    logger.info(s"Rewrote manifests on $tableName")
  }
}
