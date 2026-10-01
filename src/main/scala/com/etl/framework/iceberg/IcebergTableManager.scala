package com.etl.framework.iceberg

import com.etl.framework.config.{DdlMode, FlowConfig, IcebergConfig, MaintenanceConfig}
import com.etl.framework.util.SqlIdentifier
import org.apache.iceberg.{HasTableOperations, Table, TableProperties}
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/** Enforces the configured table-management policy and provides Iceberg snapshot and maintenance operations.
  */
class IcebergTableManager(
    spark: SparkSession,
    icebergConfig: IcebergConfig
) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Returns the fully qualified Iceberg table name for a flow (catalogName.namespace.flowName). */
  def resolveTableName(flowConfig: FlowConfig): String =
    icebergConfig.fullTableName(flowConfig.name)

  /** Validates a platform-provisioned table or explicitly applies automatic DDL, depending on `ddlMode`. */
  def prepareTable(
      flowConfig: FlowConfig,
      schema: StructType
  ): Unit =
    prepareTable(
      resolveTableName(flowConfig),
      schema,
      flowConfig.output.tableProperties,
      flowConfig.output.icebergPartitions,
      flowConfig.output.sortOrder,
      flowConfig.loadMode.isActiveColumn
    )

  /** Applies the same DDL policy to derived and framework-owned tables that have no FlowConfig. */
  def prepareTable(tableName: String, schema: StructType): Unit =
    prepareTable(tableName, schema, Map.empty, Seq.empty, Seq.empty, None)

  /** Fails before flow execution when validate-only targets have not been provisioned by the platform. */
  def validateTargetsExist(tableNames: Seq[String]): Unit =
    if (icebergConfig.ddlMode == DdlMode.Validate) {
      val missing = tableNames.distinct.sorted.filterNot(tableExists)
      require(
        missing.isEmpty,
        s"ddlMode=validate requires every target table to be provisioned before execution; " +
          s"missing: ${missing.mkString(", ")}"
      )
      tableNames.distinct.foreach { tableName =>
        val _ = Spark3Util.loadIcebergTable(spark, SqlIdentifier.quoteMultipart(tableName))
      }
    }

  private def prepareTable(
      tableName: String,
      schema: StructType,
      tableProperties: Map[String, String],
      partitions: Seq[String],
      sortOrder: Seq[String],
      isActiveColumn: Option[String]
  ): Unit = {
    require(
      !tableProperties.contains(TableProperties.FORMAT_VERSION),
      s"output.tableProperties must not contain '${TableProperties.FORMAT_VERSION}'; " +
        s"Floe owns this invariant through iceberg.formatVersion=${icebergConfig.formatVersion}"
    )

    icebergConfig.ddlMode match {
      case DdlMode.Validate =>
        validateExistingTable(tableName, schema, tableProperties, partitions, sortOrder)
      case DdlMode.Automatic =>
        createOrUpdateTable(tableName, schema, tableProperties, partitions, sortOrder, isActiveColumn)
    }
  }

  private def createOrUpdateTable(
      tableName: String,
      schema: StructType,
      tableProperties: Map[String, String],
      partitions: Seq[String],
      sortOrder: Seq[String],
      isActiveColumn: Option[String]
  ): Unit = {
    if (tableExists(tableName)) {
      logger.info(s"Table $tableName exists, applying config updates")
      updateTableConfig(tableName, schema, tableProperties, partitions, isActiveColumn)
    } else {
      createTable(tableName, schema, tableProperties, partitions, sortOrder)
    }
  }

  /** Applies all config updates to an existing table: new columns, type widening, properties, partitions. */
  private def updateTableConfig(
      tableName: String,
      schema: StructType,
      tableProperties: Map[String, String],
      partitions: Seq[String],
      isActiveColumn: Option[String]
  ): Unit = {
    addNewColumns(tableName, schema, isActiveColumn)
    widenColumnTypes(tableName, schema)
    updateTableProperties(tableName, tableProperties)
    addPartitionFields(tableName, partitions)
  }

  /** Adds columns present in the incoming schema but missing from the table. */
  private def addNewColumns(tableName: String, schema: StructType, isActiveColumn: Option[String]): Unit = {
    val sqlTableName = SqlIdentifier.quoteMultipart(tableName)
    val currentColumns = spark.table(sqlTableName).schema.fieldNames.toSet
    val newColumns = schema.fields.filterNot(f => currentColumns.contains(f.name))
    newColumns.foreach { field =>
      spark.sql(s"ALTER TABLE $sqlTableName ADD COLUMN ${SqlIdentifier.quote(field.name)} ${field.dataType.sql}")
      logger.info(s"Added column ${field.name} (${field.dataType.sql}) to $tableName")
      if (isActiveColumn.contains(field.name)) {
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
    val sqlTableName = SqlIdentifier.quoteMultipart(tableName)
    val currentSchema = spark.table(sqlTableName).schema
    schema.fields.foreach { incomingField =>
      currentSchema.fields.find(_.name == incomingField.name).foreach { existingField =>
        if (existingField.dataType != incomingField.dataType) {
          if (isSafeWidening(existingField.dataType, incomingField.dataType)) {
            spark.sql(
              s"ALTER TABLE $sqlTableName ALTER COLUMN ${SqlIdentifier.quote(incomingField.name)} TYPE ${incomingField.dataType.sql}"
            )
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
  private def updateTableProperties(tableName: String, tableProperties: Map[String, String]): Unit = {
    if (tableProperties.nonEmpty) {
      val currentProps = spark
        .sql(s"SHOW TBLPROPERTIES ${SqlIdentifier.quoteMultipart(tableName)}")
        .collect()
        .map(row => row.getString(0) -> row.getString(1))
        .toMap
      val toApply = tableProperties
        .filterNot { case (k, v) => currentProps.get(k).contains(v) }
      if (toApply.nonEmpty) {
        toApply.foreach { case (key, value) =>
          spark.sql(
            s"ALTER TABLE ${SqlIdentifier.quoteMultipart(tableName)} SET TBLPROPERTIES " +
              s"(${SqlIdentifier.stringLiteral(key)} = ${SqlIdentifier.stringLiteral(value)})"
          )
        }
        logger.info(s"Applied ${toApply.size} property updates to $tableName: ${toApply.keys.mkString(", ")}")
      }
    }
  }

  /** Adds partition fields from flow config. Silently skips fields that already exist (partition evolution). */
  private def addPartitionFields(tableName: String, partitions: Seq[String]): Unit = {
    if (partitions.nonEmpty) {
      partitions.foreach { partition =>
        val partitionExpr = parsePartitionTransform(partition)
        try {
          spark.sql(s"ALTER TABLE ${SqlIdentifier.quoteMultipart(tableName)} ADD PARTITION FIELD $partitionExpr")
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

  /** Read-only validation for the enterprise default. Every mismatch is reported before a data commit is attempted. */
  private def validateExistingTable(
      tableName: String,
      expectedSchema: StructType,
      expectedProperties: Map[String, String],
      expectedPartitions: Seq[String],
      expectedSortOrder: Seq[String]
  ): Unit = {
    require(
      tableExists(tableName),
      s"Iceberg table '$tableName' does not exist and ddlMode=validate forbids Floe from creating it"
    )

    val sqlTableName = SqlIdentifier.quoteMultipart(tableName)
    val actualSchema = spark.table(sqlTableName).schema
    val errors = scala.collection.mutable.ArrayBuffer.empty[String]

    val expectedByName = expectedSchema.fields.map(field => field.name -> field).toMap
    val actualByName = actualSchema.fields.map(field => field.name -> field).toMap
    val missing = expectedByName.keySet.diff(actualByName.keySet).toSeq.sorted
    val unexpected = actualByName.keySet.diff(expectedByName.keySet).toSeq.sorted
    val typeMismatches = expectedByName.keySet
      .intersect(actualByName.keySet)
      .toSeq
      .sorted
      .flatMap { name =>
        val expected = expectedByName(name).dataType
        val actual = actualByName(name).dataType
        if (expected != actual) Some(s"$name expected ${expected.sql} but table has ${actual.sql}") else None
      }

    if (missing.nonEmpty) errors += s"missing columns: ${missing.mkString(", ")}"
    if (unexpected.nonEmpty) errors += s"unexpected columns: ${unexpected.mkString(", ")}"
    if (typeMismatches.nonEmpty) errors += s"column type mismatches: ${typeMismatches.mkString(", ")}"

    val table = Spark3Util.loadIcebergTable(spark, sqlTableName)
    validateTableProperties(table, expectedProperties, errors)

    val actualPartitions = table
      .spec()
      .fields()
      .asScala
      .map { field =>
        s"${field.transform()}(${table.schema().findColumnName(field.sourceId())})"
      }
      .toSeq
    val desiredPartitions = expectedPartitions.map(canonicalPartitionTransform)
    if (actualPartitions != desiredPartitions)
      errors +=
        s"partition spec mismatch: expected [${desiredPartitions.mkString(", ")}], " +
          s"found [${actualPartitions.mkString(", ")}]"

    val actualSortOrder = table
      .sortOrder()
      .fields()
      .asScala
      .map { field =>
        val column = table.schema().findColumnName(field.sourceId())
        s"${field.transform()}($column) ${field.direction()} ${field.nullOrder()}"
      }
      .toSeq
    val desiredSortOrder = expectedSortOrder.map(column => s"identity($column) ASC NULLS FIRST")
    if (actualSortOrder != desiredSortOrder)
      errors +=
        s"sort order mismatch: expected [${desiredSortOrder.mkString(", ")}], " +
          s"found [${actualSortOrder.mkString(", ")}]"

    require(
      errors.isEmpty,
      s"Iceberg table '$tableName' does not satisfy ddlMode=validate: ${errors.mkString("; ")}"
    )
  }

  private def validateTableProperties(
      table: Table,
      expectedProperties: Map[String, String],
      errors: scala.collection.mutable.ArrayBuffer[String]
  ): Unit = {
    table match {
      case operations: HasTableOperations =>
        val actualVersion = operations.operations().current().formatVersion()
        if (actualVersion != icebergConfig.formatVersion)
          errors += s"format-version expected ${icebergConfig.formatVersion} but table has $actualVersion"
      case _ =>
        errors += "table format-version cannot be inspected through this catalog implementation"
    }

    val properties = table.properties().asScala
    val expectedFileFormat = expectedProperties.getOrElse(
      TableProperties.DEFAULT_FILE_FORMAT,
      icebergConfig.fileFormat
    )
    val actualFileFormat = properties.getOrElse(
      TableProperties.DEFAULT_FILE_FORMAT,
      TableProperties.DEFAULT_FILE_FORMAT_DEFAULT
    )
    if (!actualFileFormat.equalsIgnoreCase(expectedFileFormat))
      errors += s"write.format.default expected $expectedFileFormat but table has $actualFileFormat"

    expectedProperties.toSeq.sortBy(_._1).foreach { case (key, expected) =>
      properties.get(key) match {
        case Some(actual) if actual == expected =>
        case Some(actual) => errors += s"table property '$key' expected '$expected' but has '$actual'"
        case None         => errors += s"table property '$key' is missing (expected '$expected')"
      }
    }
  }

  private def canonicalPartitionTransform(partition: String): String = {
    val transformPattern = """^(\w+)\((.+)\)$""".r
    partition.trim match {
      case transformPattern(func, args) =>
        func.toLowerCase match {
          case "year" | "month" | "day" | "hour" =>
            s"${func.toLowerCase}(${args.trim})"
          case "bucket" | "truncate" =>
            val parts = args.split(",").map(_.trim)
            require(
              parts.length == 2 && parts(0).forall(_.isDigit),
              s"Invalid ${func.toLowerCase} partition transform: $partition"
            )
            s"${func.toLowerCase}[${parts(0)}](${parts(1)})"
          case _ =>
            throw new IllegalArgumentException(s"Unsupported Iceberg partition transform: $func")
        }
      case column if column.nonEmpty => s"identity($column)"
      case _                         => throw new IllegalArgumentException("Partition column cannot be blank")
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

  def tableExists(tableName: String): Boolean = {
    try {
      // spark.catalog.tableExists does not support fully-qualified Iceberg names (catalog.namespace.table),
      // so we use DESCRIBE TABLE which works with any catalog.
      spark.sql(s"DESCRIBE TABLE ${SqlIdentifier.quoteMultipart(tableName)}")
      true
    } catch {
      case error: AnalysisException if isTableNotFound(error) => false
    }
  }

  private def isTableNotFound(error: AnalysisException): Boolean =
    Option(error.getErrorClass).contains("TABLE_OR_VIEW_NOT_FOUND")

  /** Checks whether an exact snapshot is still addressable (for release/recovery validation). */
  def snapshotExists(tableName: String, snapshotId: Long): Boolean =
    tableExists(tableName) && Option(
      Spark3Util.loadIcebergTable(spark, SqlIdentifier.quoteMultipart(tableName)).snapshot(snapshotId)
    ).isDefined

  private def createTable(
      tableName: String,
      schema: StructType,
      tableProperties: Map[String, String],
      partitions: Seq[String],
      sortOrder: Seq[String]
  ): Unit = {
    val columns = schema.fields
      .map { field =>
        s"${SqlIdentifier.quote(field.name)} ${field.dataType.sql}"
      }
      .mkString(", ")

    val createSql = s"CREATE TABLE IF NOT EXISTS ${SqlIdentifier.quoteMultipart(tableName)} ($columns) USING iceberg"
    logger.info(s"Creating Iceberg table: $createSql")
    spark.sql(createSql)

    // Apply partition spec if configured
    if (partitions.nonEmpty) {
      applyPartitionSpec(tableName, partitions)
    }

    // Apply sort order if configured
    if (sortOrder.nonEmpty) {
      applySortOrder(tableName, sortOrder)
    }

    // Apply table properties
    val allProperties =
      (Map(TableProperties.DEFAULT_FILE_FORMAT -> icebergConfig.fileFormat) ++ tableProperties) +
        (TableProperties.FORMAT_VERSION -> icebergConfig.formatVersion.toString)

    allProperties.foreach { case (key, value) =>
      spark.sql(
        s"ALTER TABLE ${SqlIdentifier.quoteMultipart(tableName)} SET TBLPROPERTIES " +
          s"(${SqlIdentifier.stringLiteral(key)} = ${SqlIdentifier.stringLiteral(value)})"
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
        s"ALTER TABLE ${SqlIdentifier.quoteMultipart(tableName)} ADD PARTITION FIELD $partitionExpr"
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
            s"${func.toLowerCase}(${SqlIdentifier.quote(args.trim)})"
          case "bucket" =>
            val parts = args.split(",").map(_.trim)
            require(parts.length == 2 && parts(0).forall(_.isDigit), s"Invalid bucket partition transform: $partition")
            s"bucket(${parts(0)}, ${SqlIdentifier.quote(parts(1))})"
          case "truncate" =>
            val parts = args.split(",").map(_.trim)
            require(
              parts.length == 2 && parts(0).forall(_.isDigit),
              s"Invalid truncate partition transform: $partition"
            )
            s"truncate(${parts(0)}, ${SqlIdentifier.quote(parts(1))})"
          case _ =>
            throw new IllegalArgumentException(s"Unsupported Iceberg partition transform: $func")
        }
      case _ =>
        SqlIdentifier.quote(partition.trim)
    }
  }

  private def applySortOrder(
      tableName: String,
      sortColumns: Seq[String]
  ): Unit = {
    val sortExpr = sortColumns.map(SqlIdentifier.quote).mkString(", ")
    spark.sql(
      s"ALTER TABLE ${SqlIdentifier.quoteMultipart(tableName)} WRITE ORDERED BY $sortExpr"
    )
    logger.info(s"Sort order applied to $tableName: $sortExpr")
  }

  /** Returns the current main-branch snapshot ID, or None if the table has no snapshots. */
  def getCurrentSnapshotId(flowConfig: FlowConfig): Option[Long] =
    getCurrentSnapshotId(resolveTableName(flowConfig))

  def getCurrentSnapshotId(tableName: String): Option[Long] = {
    try {
      val refs = spark.sql(
        s"SELECT snapshot_id FROM ${SqlIdentifier.metadataTable(tableName, "refs")} " +
          s"WHERE name = ${SqlIdentifier.stringLiteral("main")}"
      )
      if (refs.isEmpty) None
      else Some(refs.first().getLong(0))
    } catch {
      case error: AnalysisException if isTableNotFound(error) => None
    }
  }

  /** Finds every live snapshot produced by a FLOe logical operation. The operation ID is stored in the snapshot summary
    * as part of the same Iceberg commit, so this is the authoritative reconciliation lookup.
    */
  def findSnapshotsByOperationId(flowConfig: FlowConfig, operationId: String): Seq[CommittedSnapshot] =
    findSnapshotsByOperationId(resolveTableName(flowConfig), operationId)

  def findSnapshotsByOperationId(tableName: String, operationId: String): Seq[CommittedSnapshot] = {
    spark
      .sql(
        s"SELECT snapshot_id, parent_id, committed_at, manifest_list, summary " +
          s"FROM ${SqlIdentifier.metadataTable(tableName, "snapshots")} " +
          s"WHERE summary[${SqlIdentifier.stringLiteral("floe.operation-id")}] = " +
          SqlIdentifier.stringLiteral(operationId) +
          " ORDER BY committed_at"
      )
      .collect()
      .toSeq
      .map { row =>
        CommittedSnapshot(
          snapshotId = row.getLong(0),
          parentSnapshotId = if (row.isNullAt(1)) None else Some(row.getLong(1)),
          committedAtMs = row.getTimestamp(2).getTime,
          manifestListLocation = row.getString(3),
          summary = row.getMap[String, String](4).toMap
        )
      }
  }

  /** Tags a snapshot with the batch ID for time travel (e.g. batch_20260115_100000). */
  def tagSnapshot(
      flowConfig: FlowConfig,
      snapshotId: Long,
      batchId: String
  ): Boolean = tagSnapshot(resolveTableName(flowConfig), snapshotId, batchId)

  def tagSnapshot(
      tableName: String,
      snapshotId: Long,
      batchId: String
  ): Boolean = {
    if (!icebergConfig.enableSnapshotTagging) return false

    val tagName = s"batch_$batchId"
    val retentionMs = icebergConfig.maintenance.snapshotRetentionDays match {
      case Some(days) if days > 0 => Some(days.toLong * 24 * 60 * 60 * 1000)
      case Some(days) =>
        logger.error(s"Cannot tag $tableName: snapshotRetentionDays must be positive, got $days")
        return false
      case None => None
    }
    try {
      val existingSnapshot = spark
        .sql(
          s"SELECT snapshot_id FROM ${SqlIdentifier.metadataTable(tableName, "refs")} " +
            s"WHERE name = ${SqlIdentifier.stringLiteral(tagName)}"
        )
        .collect()
        .headOption
        .map(_.getLong(0))
      if (existingSnapshot.exists(_ != snapshotId)) {
        logger.error(s"Tag '$tagName' on $tableName already references a different snapshot")
        return false
      }
      if (existingSnapshot.contains(snapshotId)) return true

      val table = Spark3Util.loadIcebergTable(spark, SqlIdentifier.quoteMultipart(tableName))
      val update = table.manageSnapshots().createTag(tagName, snapshotId)
      retentionMs.foreach(maxAge => update.setMaxRefAgeMs(tagName, maxAge))
      update.commit()
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
            s"FROM ${SqlIdentifier.metadataTable(tableName, "snapshots")} WHERE snapshot_id = $snapshotId"
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
