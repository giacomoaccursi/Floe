package com.etl.framework.iceberg

import com.etl.framework.config.{FlowConfig, IcebergConfig, MaintenanceConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/** Manages Iceberg table lifecycle: creation, schema evolution (add columns, widen types), partition evolution, table
  * properties, snapshot tagging, and maintenance delegation.
  */
class IcebergTableManager(
    spark: SparkSession,
    icebergConfig: IcebergConfig
) {

  private val logger = LoggerFactory.getLogger(getClass)

  private val sqlTimestampFmt = DateTimeFormatter
    .ofPattern("yyyy-MM-dd HH:mm:ss")
    .withZone(ZoneOffset.UTC)

  /** Returns the fully qualified Iceberg table name for a flow (catalogName.namespace.flowName). */
  def resolveTableName(flowConfig: FlowConfig): String =
    icebergConfig.fullTableName(flowConfig.name)

  /** Creates the Iceberg table if it doesn't exist, or updates schema/properties/partitions if it does. */
  def createOrUpdateTable(
      flowConfig: FlowConfig,
      schema: StructType
  ): Unit = {
    val tableName = resolveTableName(flowConfig)

    if (tableExists(tableName)) {
      logger.info(s"Table $tableName exists, applying config updates")
      updateTableConfig(tableName, schema, flowConfig)
    } else {
      createTable(tableName, schema, flowConfig)
    }
  }

  /** Applies all config updates to an existing table: new columns, type widening, properties, partitions. */
  private def updateTableConfig(tableName: String, schema: StructType, flowConfig: FlowConfig): Unit = {
    addNewColumns(tableName, schema, flowConfig)
    widenColumnTypes(tableName, schema)
    updateTableProperties(tableName, flowConfig)
    addPartitionFields(tableName, flowConfig)
  }

  /** Adds columns present in the incoming schema but missing from the table. */
  private def addNewColumns(tableName: String, schema: StructType, flowConfig: FlowConfig): Unit = {
    val currentColumns = spark.table(tableName).schema.fieldNames.toSet
    val newColumns = schema.fields.filterNot(f => currentColumns.contains(f.name))
    val isActiveCol = flowConfig.loadMode.isActiveColumn
    newColumns.foreach { field =>
      spark.sql(s"ALTER TABLE $tableName ADD COLUMN ${field.name} ${field.dataType.sql}")
      logger.info(s"Added column ${field.name} (${field.dataType.sql}) to $tableName")
      if (isActiveCol.contains(field.name)) {
        logger.warn(
          s"Column '${field.name}' (is_active) was added to an existing table ($tableName). " +
            s"Rows written before this change have ${field.name}=NULL and will be excluded by " +
            s"queries filtering on ${field.name} = true. " +
            s"Run a full reload to backfill ${field.name}=true on all current records."
        )
      }
    }
  }

  /** Widens column types where safe (int→long, float→double, decimal precision up). */
  private def widenColumnTypes(tableName: String, schema: StructType): Unit = {
    val currentSchema = spark.table(tableName).schema
    schema.fields.foreach { incomingField =>
      currentSchema.fields.find(_.name == incomingField.name).foreach { existingField =>
        if (existingField.dataType != incomingField.dataType) {
          if (isSafeWidening(existingField.dataType, incomingField.dataType)) {
            spark.sql(s"ALTER TABLE $tableName ALTER COLUMN ${incomingField.name} TYPE ${incomingField.dataType.sql}")
            logger.info(
              s"Widened column ${incomingField.name} from ${existingField.dataType.sql} to ${incomingField.dataType.sql} on $tableName"
            )
          } else {
            logger.warn(
              s"Column ${incomingField.name} type mismatch on $tableName: " +
                s"table has ${existingField.dataType.sql}, incoming has ${incomingField.dataType.sql}. " +
                s"Not a safe widening — skipping. Resolve manually with ALTER TABLE."
            )
          }
        }
      }
    }
  }

  /** Applies table properties from flow config, skipping properties that already have the target value. */
  private def updateTableProperties(tableName: String, flowConfig: FlowConfig): Unit = {
    if (flowConfig.output.tableProperties.nonEmpty) {
      val currentProps = spark
        .sql(s"SHOW TBLPROPERTIES $tableName")
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
  }

  /** Adds partition fields from flow config. Silently skips fields that already exist (partition evolution). */
  private def addPartitionFields(tableName: String, flowConfig: FlowConfig): Unit = {
    if (flowConfig.output.icebergPartitions.nonEmpty) {
      flowConfig.output.icebergPartitions.foreach { partition =>
        val partitionExpr = parsePartitionTransform(partition)
        try {
          spark.sql(s"ALTER TABLE $tableName ADD PARTITION FIELD $partitionExpr")
          logger.info(s"Added partition field $partitionExpr to $tableName")
          logger.warn(
            s"Partition field '$partitionExpr' was added to an existing table ($tableName). " +
              s"Data written before this change is NOT retroactively partitioned: " +
              s"partition pruning will apply only to files written from this run onwards."
          )
        } catch {
          case e: Exception if isPartitionAlreadyExistsError(e) =>
            logger.debug(s"Partition field $partitionExpr already present on $tableName, skipping")
        }
      }
    }
  }

  /** Checks if a type change is safe (no data loss). Only int→long, float→double, decimal precision up. */
  private[iceberg] def isSafeWidening(from: DataType, to: DataType): Boolean = (from, to) match {
    case (IntegerType, LongType)                                    => true
    case (FloatType, DoubleType)                                    => true
    case (d1: DecimalType, d2: DecimalType) if d1.scale == d2.scale => d2.precision > d1.precision
    case _                                                          => false
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
    val columns = schema.fields
      .map { field =>
        s"${field.name} ${field.dataType.sql}"
      }
      .mkString(", ")

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

  /** Returns the latest snapshot ID for a flow's table, or None if the table has no snapshots. */
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

  /** Tags a snapshot with the batch ID for time travel (e.g. batch_20260115_100000). */
  def tagSnapshot(
      flowConfig: FlowConfig,
      snapshotId: Long,
      batchId: String
  ): Boolean = {
    if (!icebergConfig.enableSnapshotTagging) return false

    val tableName = resolveTableName(flowConfig)
    val tagName = s"batch_$batchId"
    try {
      spark.sql(
        s"ALTER TABLE $tableName CREATE TAG `$tagName` AS OF VERSION $snapshotId"
      )
      logger.info(s"Tagged snapshot $snapshotId as '$tagName' on $tableName")
      true
    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to tag snapshot $snapshotId on $tableName: ${e.getMessage}"
        )
        false
    }
  }

  /** Collects snapshot metadata (parent ID, timestamp, manifest list, summary) for batch metadata JSON. */
  def getSnapshotMetadata(
      flowConfig: FlowConfig,
      snapshotId: Long,
      recordsWritten: Long,
      batchId: String,
      tagCreated: Boolean = false
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
        if (tagCreated) Some(s"batch_$batchId")
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

  /** Runs post-batch maintenance operations on a flow's table. */
  def runMaintenance(
      flowConfig: FlowConfig,
      config: MaintenanceConfig
  ): Unit = {
    val tableName = resolveTableName(flowConfig)
    new IcebergMaintenanceRunner(spark, icebergConfig).run(tableName, config)
  }
}
