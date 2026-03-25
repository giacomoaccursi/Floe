package com.etl.framework.iceberg

import com.etl.framework.config.{FlowConfig, IcebergConfig, MaintenanceConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

class IcebergTableManager(
    spark: SparkSession,
    icebergConfig: IcebergConfig
) {

  private val logger = LoggerFactory.getLogger(getClass)

  def resolveTableName(flowConfig: FlowConfig): String = {
    s"${icebergConfig.catalogName}.default.${flowConfig.name}"
  }

  def createOrUpdateTable(
      flowConfig: FlowConfig,
      schema: StructType
  ): Unit = {
    val tableName = resolveTableName(flowConfig)

    if (tableExists(tableName)) {
      logger.info(s"Table $tableName exists, applying config updates")
      updateTableConfig(tableName, flowConfig)
    } else {
      createTable(tableName, schema, flowConfig)
    }
  }

  private def updateTableConfig(tableName: String, flowConfig: FlowConfig): Unit = {
    // Apply new or changed table properties
    if (flowConfig.output.tableProperties.nonEmpty) {
      val currentProps = spark.sql(s"SHOW TBLPROPERTIES $tableName")
        .collect()
        .map(row => row.getString(0) -> row.getString(1))
        .toMap
      val toApply = flowConfig.output.tableProperties
        .filterNot { case (k, v) => currentProps.get(k).contains(v) }
      if (toApply.nonEmpty) {
        toApply.foreach { case (key, value) =>
          spark.sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('$key' = '$value')")
        }
        logger.info(s"Applied ${toApply.size} property updates to $tableName: ${toApply.keys.mkString(", ")}")
      }
    }

    // Add missing partition fields — idempotent: Iceberg throws if field already exists
    if (flowConfig.output.icebergPartitions.nonEmpty) {
      flowConfig.output.icebergPartitions.foreach { partition =>
        val partitionExpr = parsePartitionTransform(partition)
        try {
          spark.sql(s"ALTER TABLE $tableName ADD PARTITION FIELD $partitionExpr")
          logger.info(s"Added partition field $partitionExpr to $tableName")
          logger.warn(
            s"Partition field '$partitionExpr' was added to an existing table ($tableName). " +
              s"Data written before this change is NOT retroactively partitioned: " +
              s"partition pruning will apply only to files written from this run onwards. " +
              s"To apply the partition layout to all existing data, perform a full reload " +
              s"(set loadMode.type=full for one run, then revert to delta)."
          )
        } catch {
          case e: Exception if isPartitionAlreadyExistsError(e) =>
            logger.debug(s"Partition field $partitionExpr already present on $tableName, skipping")
        }
      }
    }
  }

  private def isPartitionAlreadyExistsError(e: Exception): Boolean = {
    val msg = Option(e.getMessage).getOrElse("").toLowerCase
    msg.contains("already exists") || msg.contains("redundant") || msg.contains("duplicate")
  }

  private def tableExists(tableName: String): Boolean = {
    try {
      spark.sql(s"DESCRIBE TABLE $tableName")
      true
    } catch {
      case _: org.apache.spark.sql.AnalysisException => false
    }
  }

  private def createTable(
      tableName: String,
      schema: StructType,
      flowConfig: FlowConfig
  ): Unit = {
    val columns = schema.fields.map { field =>
      s"${field.name} ${field.dataType.sql}"
    }.mkString(", ")

    val createSql = s"CREATE TABLE IF NOT EXISTS $tableName ($columns) USING iceberg"
    logger.info(s"Creating Iceberg table: $createSql")
    spark.sql(createSql)

    // Apply partition spec if configured
    if (flowConfig.output.icebergPartitions.nonEmpty) {
      applyPartitionSpec(tableName, flowConfig.output.icebergPartitions)
    }

    // Apply sort order if configured
    if (flowConfig.output.sortOrder.nonEmpty) {
      applySortOrder(tableName, flowConfig.output.sortOrder)
    }

    // Apply table properties
    val allProperties = Map(
      "format-version" -> icebergConfig.formatVersion.toString,
      "write.format.default" -> icebergConfig.fileFormat
    ) ++ flowConfig.output.tableProperties

    allProperties.foreach { case (key, value) =>
      spark.sql(
        s"ALTER TABLE $tableName SET TBLPROPERTIES ('$key' = '$value')"
      )
    }

    logger.info(s"Iceberg table $tableName created successfully")
  }

  private def applyPartitionSpec(
      tableName: String,
      partitions: Seq[String]
  ): Unit = {
    partitions.foreach { partition =>
      val partitionExpr = parsePartitionTransform(partition)
      spark.sql(
        s"ALTER TABLE $tableName ADD PARTITION FIELD $partitionExpr"
      )
    }
    logger.info(
      s"Partition spec applied to $tableName: ${partitions.mkString(", ")}"
    )
  }

  private[iceberg] def parsePartitionTransform(partition: String): String = {
    val transformPattern = """^(\w+)\((.+)\)$""".r

    partition match {
      case transformPattern(func, args) =>
        func.toLowerCase match {
          case "year" | "month" | "day" | "hour" =>
            s"${func.toLowerCase}($args)"
          case "bucket" =>
            val parts = args.split(",").map(_.trim)
            s"bucket(${parts(0)}, ${parts(1)})"
          case "truncate" =>
            val parts = args.split(",").map(_.trim)
            s"truncate(${parts(0)}, ${parts(1)})"
          case _ =>
            partition
        }
      case _ =>
        partition
    }
  }

  private def applySortOrder(
      tableName: String,
      sortColumns: Seq[String]
  ): Unit = {
    val sortExpr = sortColumns.mkString(", ")
    spark.sql(
      s"ALTER TABLE $tableName WRITE ORDERED BY $sortExpr"
    )
    logger.info(s"Sort order applied to $tableName: $sortExpr")
  }

  def getCurrentSnapshotId(flowConfig: FlowConfig): Option[Long] = {
    val tableName = resolveTableName(flowConfig)
    try {
      val snapshots = spark.sql(
        s"SELECT snapshot_id FROM $tableName.snapshots ORDER BY committed_at DESC LIMIT 1"
      )
      if (snapshots.isEmpty) None
      else Some(snapshots.first().getLong(0))
    } catch {
      case _: org.apache.spark.sql.AnalysisException => None
    }
  }

  def tagSnapshot(
      flowConfig: FlowConfig,
      snapshotId: Long,
      batchId: String
  ): Unit = {
    if (!icebergConfig.enableSnapshotTagging) return

    val tableName = resolveTableName(flowConfig)
    val tagName = s"batch_$batchId"
    try {
      spark.sql(
        s"ALTER TABLE $tableName CREATE TAG `$tagName` AS OF VERSION $snapshotId"
      )
      logger.info(s"Tagged snapshot $snapshotId as '$tagName' on $tableName")
    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to tag snapshot $snapshotId on $tableName: ${e.getMessage}"
        )
    }
  }

  def getSnapshotMetadata(
      flowConfig: FlowConfig,
      snapshotId: Long,
      recordsWritten: Long,
      batchId: String
  ): Option[IcebergFlowMetadata] = {
    val tableName = resolveTableName(flowConfig)
    try {
      val row = spark
        .sql(
          s"SELECT parent_id, committed_at, manifest_list, summary " +
            s"FROM $tableName.snapshots WHERE snapshot_id = $snapshotId"
        )
        .first()

      val parentId =
        if (row.isNullAt(0)) None else Some(row.getLong(0))
      val committedAt = row.getTimestamp(1).getTime
      val manifestList = row.getString(2)
      val summary = row.getMap[String, String](3).toMap

      val tag =
        if (icebergConfig.enableSnapshotTagging) Some(s"batch_$batchId")
        else None

      Some(
        IcebergFlowMetadata(
          tableName = tableName,
          snapshotId = snapshotId,
          snapshotTag = tag,
          parentSnapshotId = parentId,
          snapshotTimestampMs = committedAt,
          recordsWritten = recordsWritten,
          manifestListLocation = manifestList,
          summary = summary
        )
      )
    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to get snapshot metadata for $tableName: ${e.getMessage}"
        )
        None
    }
  }

  def rollbackToSnapshot(
      flowConfig: FlowConfig,
      snapshotId: Long
  ): Unit = {
    val tableName = resolveTableName(flowConfig)
    spark.sql(
      s"CALL ${icebergConfig.catalogName}.system.rollback_to_snapshot('$tableName', $snapshotId)"
    )
    logger.info(s"Rolled back $tableName to snapshot $snapshotId")
  }

  def runMaintenance(
      flowConfig: FlowConfig,
      config: MaintenanceConfig
  ): Unit = {
    val tableName = resolveTableName(flowConfig)
    logger.info(s"Running maintenance on $tableName")

    if (config.enableSnapshotExpiration) {
      expireSnapshots(tableName, config.snapshotRetentionDays)
    }
    if (config.enableCompaction) {
      compactDataFiles(tableName, config.targetFileSizeMb)
    }
    if (config.enableOrphanCleanup) {
      removeOrphanFiles(tableName, config.orphanRetentionMinutes)
    }
    if (config.enableManifestRewrite) {
      rewriteManifests(tableName)
    }
  }

  private def expireSnapshots(
      tableName: String,
      retentionDays: Int
  ): Unit = {
    spark.sql(
      s"CALL ${icebergConfig.catalogName}.system.expire_snapshots(" +
        s"table => '$tableName', " +
        s"older_than => TIMESTAMP '${java.time.Instant.now().minusSeconds(retentionDays.toLong * 86400)}'" +
        s")"
    )
    logger.info(s"Expired snapshots older than $retentionDays days on $tableName")
  }

  private def compactDataFiles(
      tableName: String,
      targetFileSizeMb: Int
  ): Unit = {
    spark.sql(
      s"CALL ${icebergConfig.catalogName}.system.rewrite_data_files(" +
        s"table => '$tableName', " +
        s"options => map('target-file-size-bytes', '${targetFileSizeMb.toLong * 1024 * 1024}')" +
        s")"
    )
    logger.info(s"Compacted data files on $tableName (target: ${targetFileSizeMb}MB)")
  }

  private def removeOrphanFiles(
      tableName: String,
      retentionMinutes: Int
  ): Unit = {
    spark.sql(
      s"CALL ${icebergConfig.catalogName}.system.remove_orphan_files(" +
        s"table => '$tableName', " +
        s"older_than => TIMESTAMP '${java.time.Instant.now().minusSeconds(retentionMinutes.toLong * 60)}'" +
        s")"
    )
    logger.info(
      s"Removed orphan files older than ${retentionMinutes}min on $tableName"
    )
  }

  private def rewriteManifests(tableName: String): Unit = {
    spark.sql(
      s"CALL ${icebergConfig.catalogName}.system.rewrite_manifests('$tableName')"
    )
    logger.info(s"Rewrote manifests on $tableName")
  }
}
