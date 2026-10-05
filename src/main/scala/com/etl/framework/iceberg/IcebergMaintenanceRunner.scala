package com.etl.framework.iceberg

import com.etl.framework.config.{IcebergConfig, MaintenanceConfig}
import com.etl.framework.util.{SensitiveDataRedactor, SqlIdentifier}
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory

import java.time.{Clock, Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** Executes one synchronous maintenance plan for one Iceberg table.
  *
  * The caller owns scheduling, mutual exclusion, persistence, and retry decisions. Execution stops after the first
  * uncertain action: later actions are not started and ingestion is never retried as a side effect.
  */
class IcebergMaintenanceRunner(
    spark: SparkSession,
    icebergConfig: IcebergConfig,
    clock: Clock = Clock.systemUTC()
) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val sqlTimestampFmt = DateTimeFormatter
    .ofPattern("yyyy-MM-dd HH:mm:ss")
    .withZone(ZoneOffset.UTC)

  def run(tableName: String, config: MaintenanceConfig): MaintenanceResult = {
    require(tableName != null && tableName.trim.nonEmpty, "tableName must not be blank")
    require(tableName.length <= 512 && !tableName.exists(_.isControl), "tableName contains invalid characters")
    config.requireRunnable()

    val plan = selectedActions(config)
    logger.info(s"Running ${plan.size} maintenance action(s) on ${SensitiveDataRedactor.redact(tableName)}")

    val initialState =
      try {
        val table = Spark3Util.loadIcebergTable(spark, SqlIdentifier.quoteMultipart(tableName))
        table.refresh()
        Right(
          Option(table.uuid()).map(_.toString) ->
            Option(table.currentSnapshot()).map(_.snapshotId())
        )
      } catch {
        case NonFatal(error) => Left(SensitiveDataRedactor.message(error))
      }

    initialState match {
      case Left(error) =>
        MaintenanceResult(
          contractVersion = MaintenanceResult.CurrentContractVersion,
          tableName = tableName,
          tableUuid = None,
          startingSnapshotId = None,
          status = MaintenanceStatus.Failed,
          actions = plan.map(notAttempted(_, config)),
          error = Some(error)
        )
      case Right((tableUuid, snapshotId)) => executePlan(tableName, tableUuid, snapshotId, config, plan)
    }
  }

  private def executePlan(
      tableName: String,
      tableUuid: Option[String],
      startingSnapshotId: Option[Long],
      config: MaintenanceConfig,
      plan: Seq[MaintenanceAction]
  ): MaintenanceResult = {
    val completed = Seq.newBuilder[MaintenanceActionResult]
    val iterator = plan.iterator.zipWithIndex
    while (iterator.hasNext) {
      val (action, index) = iterator.next()
      try {
        val metrics = executeAction(action, tableName, config)
        completed += MaintenanceActionResult(
          action = action,
          status = MaintenanceActionStatus.Succeeded,
          parameters = parameters(action, config),
          metrics = metrics
        )
      } catch {
        case NonFatal(error) =>
          val safeError = SensitiveDataRedactor.message(error)
          logger.error(
            s"Maintenance action ${action.name} on ${SensitiveDataRedactor.redact(tableName)} has an unknown outcome: $safeError"
          )
          val results = completed.result() ++ Seq(
            MaintenanceActionResult(
              action = action,
              status = MaintenanceActionStatus.Unknown,
              parameters = parameters(action, config),
              error = Some(safeError)
            )
          ) ++ plan.drop(index + 1).map(notAttempted(_, config))
          return MaintenanceResult(
            contractVersion = MaintenanceResult.CurrentContractVersion,
            tableName = tableName,
            tableUuid = tableUuid,
            startingSnapshotId = startingSnapshotId,
            status = MaintenanceStatus.Unknown,
            actions = results,
            error = Some(safeError)
          )
      }
    }

    MaintenanceResult(
      contractVersion = MaintenanceResult.CurrentContractVersion,
      tableName = tableName,
      tableUuid = tableUuid,
      startingSnapshotId = startingSnapshotId,
      status = MaintenanceStatus.Succeeded,
      actions = completed.result()
    )
  }

  protected[iceberg] def executeAction(
      action: MaintenanceAction,
      tableName: String,
      config: MaintenanceConfig
  ): Map[String, Long] = action match {
    case MaintenanceAction.ExpireSnapshots   => expireSnapshots(tableName, config.snapshotRetentionDays.get)
    case MaintenanceAction.RewriteDataFiles  => compactDataFiles(tableName, config.targetFileSizeMb.get)
    case MaintenanceAction.RemoveOrphanFiles => removeOrphanFiles(tableName, config.orphanRetentionMinutes.get)
    case MaintenanceAction.RewriteManifests  => rewriteManifests(tableName)
  }

  private def expireSnapshots(tableName: String, retentionDays: Int): Map[String, Long] = {
    val result = spark.sql(
      s"CALL ${SqlIdentifier.quote(icebergConfig.catalogName)}.system.expire_snapshots(" +
        s"table => ${SqlIdentifier.stringLiteral(tableName)}, " +
        s"older_than => TIMESTAMP '${sqlTimestampFmt.format(Instant.now(clock).minusSeconds(retentionDays.toLong * 86400))}', " +
        s"stream_results => true" +
        s")"
    )
    val row = result.first()
    val metrics = metricsFrom(
      row,
      "deleted_data_files_count",
      "deleted_position_delete_files_count",
      "deleted_equality_delete_files_count",
      "deleted_manifest_files_count",
      "deleted_manifest_lists_count"
    )
    logger.info(s"Expired eligible snapshots on ${safeTableName(tableName)}: ${renderMetrics(metrics)}")
    metrics
  }

  private def compactDataFiles(tableName: String, targetFileSizeMb: Int): Map[String, Long] = {
    val result = spark.sql(
      s"CALL ${SqlIdentifier.quote(icebergConfig.catalogName)}.system.rewrite_data_files(" +
        s"table => ${SqlIdentifier.stringLiteral(tableName)}, " +
        s"options => map(" +
        s"'target-file-size-bytes', '${targetFileSizeMb.toLong * 1024 * 1024}', " +
        s"'partial-progress.enabled', 'false')" +
        s")"
    )
    val row = result.first()
    val metrics = metricsFrom(
      row,
      "rewritten_data_files_count",
      "added_data_files_count",
      "rewritten_bytes_count",
      "failed_data_files_count",
      "removed_delete_files_count"
    )
    logger.info(s"Rewrote data files on ${safeTableName(tableName)}: ${renderMetrics(metrics)}")
    metrics
  }

  private def removeOrphanFiles(tableName: String, retentionMinutes: Int): Map[String, Long] = {
    val olderThan = Instant.now(clock).minusSeconds(retentionMinutes.toLong * 60).toEpochMilli
    val table = Spark3Util.loadIcebergTable(spark, SqlIdentifier.quoteMultipart(tableName))
    val result = SparkActions
      .get(spark)
      .deleteOrphanFiles(table)
      .olderThan(olderThan)
      .option("stream-results", "true")
      .execute()
    val deletedCount = result.orphanFileLocations().asScala.iterator.foldLeft(0L) { (count, _) => count + 1L }
    val metrics = Map("deleted_orphan_files_count" -> deletedCount)
    logger.info(s"Removed orphan files on ${safeTableName(tableName)}: ${renderMetrics(metrics)}")
    metrics
  }

  private def rewriteManifests(tableName: String): Map[String, Long] = {
    val result = spark.sql(
      s"CALL ${SqlIdentifier.quote(icebergConfig.catalogName)}.system.rewrite_manifests(" +
        s"table => ${SqlIdentifier.stringLiteral(tableName)})"
    )
    val row = result.first()
    val metrics = metricsFrom(row, "rewritten_manifests_count", "added_manifests_count")
    logger.info(s"Rewrote manifests on ${safeTableName(tableName)}: ${renderMetrics(metrics)}")
    metrics
  }

  private def selectedActions(config: MaintenanceConfig): Seq[MaintenanceAction] =
    config.snapshotRetentionDays.map(_ => MaintenanceAction.ExpireSnapshots).toSeq ++
      config.targetFileSizeMb.map(_ => MaintenanceAction.RewriteDataFiles).toSeq ++
      config.orphanRetentionMinutes.map(_ => MaintenanceAction.RemoveOrphanFiles).toSeq ++
      (if (config.enableManifestRewrite) Seq(MaintenanceAction.RewriteManifests) else Seq.empty)

  private def parameters(action: MaintenanceAction, config: MaintenanceConfig): Map[String, String] = action match {
    case MaintenanceAction.ExpireSnapshots =>
      Map("snapshot_retention_days" -> config.snapshotRetentionDays.get.toString)
    case MaintenanceAction.RewriteDataFiles => Map("target_file_size_mb" -> config.targetFileSizeMb.get.toString)
    case MaintenanceAction.RemoveOrphanFiles =>
      Map("orphan_retention_minutes" -> config.orphanRetentionMinutes.get.toString)
    case MaintenanceAction.RewriteManifests => Map.empty
  }

  private def notAttempted(action: MaintenanceAction, config: MaintenanceConfig): MaintenanceActionResult =
    MaintenanceActionResult(
      action = action,
      status = MaintenanceActionStatus.NotAttempted,
      parameters = parameters(action, config)
    )

  private def metricsFrom(row: Row, names: String*): Map[String, Long] =
    names.map { name =>
      val value =
        Option(row.getAs[Any](name)).collect { case number: java.lang.Number => number.longValue() }.getOrElse {
          throw new IllegalStateException(s"Iceberg maintenance result '$name' is missing or is not numeric")
        }
      name -> value
    }.toMap

  private def renderMetrics(metrics: Map[String, Long]): String =
    metrics.toSeq.sortBy(_._1).map { case (name, value) => s"$name=$value" }.mkString(", ")

  private def safeTableName(tableName: String): String = SensitiveDataRedactor.redact(tableName)
}
