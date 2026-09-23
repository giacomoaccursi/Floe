package com.etl.framework.iceberg

import com.etl.framework.config.{IcebergConfig, MaintenanceConfig}
import com.etl.framework.util.SqlIdentifier
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._

/** Runs post-batch Iceberg maintenance: expire snapshots, compact data files, remove orphan files, and rewrite
  * manifests. Each step is optional; the order is operational, not a universal correctness requirement.
  */
class IcebergMaintenanceRunner(spark: SparkSession, icebergConfig: IcebergConfig) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val sqlTimestampFmt = DateTimeFormatter
    .ofPattern("yyyy-MM-dd HH:mm:ss")
    .withZone(ZoneOffset.UTC)

  def run(tableName: String, config: MaintenanceConfig): Unit = {
    logger.info(s"Running maintenance on $tableName")
    // Expiration releases only files exclusive to expired snapshots; tagged snapshots
    // remain protected until their references expire. Compaction creates a new
    // snapshot, so its old files may be released by a later expiration run.
    // Orphan cleanup targets unreferenced files left outside normal commits.
    config.snapshotRetentionDays.foreach(days => expireSnapshots(tableName, days))
    config.targetFileSizeMb.foreach(size => compactDataFiles(tableName, size))
    config.orphanRetentionMinutes.foreach(mins => removeOrphanFiles(tableName, mins))
    if (config.enableManifestRewrite) rewriteManifests(tableName)
  }

  private def expireSnapshots(tableName: String, retentionDays: Int): Unit = {
    val result = spark.sql(
      s"CALL ${SqlIdentifier.quote(icebergConfig.catalogName)}.system.expire_snapshots(" +
        s"table => ${SqlIdentifier.stringLiteral(tableName)}, " +
        s"older_than => TIMESTAMP '${sqlTimestampFmt.format(java.time.Instant.now().minusSeconds(retentionDays.toLong * 86400))}', " +
        s"stream_results => true" +
        s")"
    )
    val row = result.first()
    val deletedDataFiles = row.getAs[Long]("deleted_data_files_count")
    val deletedManifests = row.getAs[Long]("deleted_manifest_files_count")
    val deletedManifestLists = row.getAs[Long]("deleted_manifest_lists_count")
    logger.info(
      s"Expired snapshots on $tableName (retention: ${retentionDays}d): " +
        s"$deletedDataFiles data files, $deletedManifests manifests, $deletedManifestLists manifest lists deleted"
    )
  }

  private def compactDataFiles(tableName: String, targetFileSizeMb: Int): Unit = {
    val result = spark.sql(
      s"CALL ${SqlIdentifier.quote(icebergConfig.catalogName)}.system.rewrite_data_files(" +
        s"table => ${SqlIdentifier.stringLiteral(tableName)}, " +
        s"options => map('target-file-size-bytes', '${targetFileSizeMb.toLong * 1024 * 1024}')" +
        s")"
    )
    val row = result.first()
    val rewrittenFiles = row.getAs[Int]("rewritten_data_files_count")
    val addedFiles = row.getAs[Int]("added_data_files_count")
    logger.info(
      s"Compacted data files on $tableName (target: ${targetFileSizeMb}MB): " +
        s"$rewrittenFiles files rewritten into $addedFiles files"
    )
  }

  private def removeOrphanFiles(tableName: String, retentionMinutes: Int): Unit = {
    val effectiveMinutes = if (retentionMinutes < 1440) {
      logger.warn(
        s"orphanRetentionMinutes=$retentionMinutes is below Iceberg's 24-hour minimum. " +
          s"Clamping to 1440 minutes to prevent data corruption."
      )
      1440
    } else retentionMinutes
    val olderThan = java.time.Instant.now().minusSeconds(effectiveMinutes.toLong * 60).toEpochMilli
    val table = Spark3Util.loadIcebergTable(spark, SqlIdentifier.quoteMultipart(tableName))
    val result = SparkActions
      .get(spark)
      .deleteOrphanFiles(table)
      .olderThan(olderThan)
      .option("stream-results", "true")
      .execute()
    val sampledLocations = result.orphanFileLocations().asScala.size
    logger.info(
      s"Removed orphan files on $tableName (retention: ${effectiveMinutes}min, " +
        s"sampled result paths: $sampledLocations; streaming mode may cap the sample)"
    )
  }

  private def rewriteManifests(tableName: String): Unit = {
    val result = spark.sql(
      s"CALL ${SqlIdentifier.quote(icebergConfig.catalogName)}.system.rewrite_manifests(" +
        s"${SqlIdentifier.stringLiteral(tableName)})"
    )
    val row = result.first()
    val rewritten = row.getAs[Int]("rewritten_manifests_count")
    val added = row.getAs[Int]("added_manifests_count")
    logger.info(s"Rewrote manifests on $tableName: $rewritten rewritten into $added")
  }
}
