package com.etl.framework.pipeline

import com.etl.framework.config.IcebergConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/** Executes derived table functions and writes results to Iceberg as full-load tables. Each derived table gets snapshot
  * tagging and post-write maintenance.
  */
class DerivedTableExecutor(
    icebergConfig: IcebergConfig
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  private val sqlTimestampFmt = DateTimeFormatter
    .ofPattern("yyyy-MM-dd HH:mm:ss")
    .withZone(ZoneOffset.UTC)

  def execute(
      derivedTables: Seq[(String, DerivedTableContext => DataFrame)],
      batchId: String
  ): Seq[DerivedTableResult] = {
    val ctx = DerivedTableContext(spark, batchId, icebergConfig.catalogName)

    val results = derivedTables.map { case (tableName, fn) =>
      executeSingle(tableName, fn, ctx, batchId)
    }

    val successfulTables = results.filter(_.success).map(_.tableName)
    if (successfulTables.nonEmpty) {
      runMaintenance(successfulTables)
    }

    results
  }

  private def executeSingle(
      tableName: String,
      fn: DerivedTableContext => DataFrame,
      ctx: DerivedTableContext,
      batchId: String
  ): DerivedTableResult = {
    val fullTableName = resolveTableName(tableName)
    logger.info(s"Computing derived table: $tableName")
    try {
      val df = fn(ctx)
      val recordsWritten = writeToIceberg(fullTableName, df)
      tagSnapshot(fullTableName, batchId)
      logger.info(s"Derived table $tableName written: $recordsWritten records")
      DerivedTableResult(tableName, success = true, recordsWritten = recordsWritten)
    } catch {
      case e: Exception =>
        logger.error(s"Derived table $tableName failed: ${e.getMessage}", e)
        DerivedTableResult(tableName, success = false, error = Some(e.getMessage))
    }
  }

  private def resolveTableName(tableName: String): String =
    s"${icebergConfig.catalogName}.default.$tableName"

  private def writeToIceberg(fullTableName: String, df: DataFrame): Long = {
    createOrUpdateTable(fullTableName, df.schema)
    val cachedDf = df.cache()
    try {
      cachedDf.writeTo(fullTableName).overwrite(lit(true))
      cachedDf.count()
    } finally {
      cachedDf.unpersist()
    }
  }

  private def tagSnapshot(fullTableName: String, batchId: String): Unit = {
    if (!icebergConfig.enableSnapshotTagging) return

    try {
      val snapshotId = spark
        .sql(s"SELECT snapshot_id FROM $fullTableName.snapshots ORDER BY committed_at DESC LIMIT 1")
        .first()
        .getLong(0)

      val tagName = s"batch_$batchId"
      spark.sql(s"ALTER TABLE $fullTableName CREATE TAG `$tagName` AS OF VERSION $snapshotId")
      logger.info(s"Tagged snapshot $snapshotId as '$tagName' on $fullTableName")
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to tag snapshot on $fullTableName: ${e.getMessage}")
    }
  }

  private def runMaintenance(tableNames: Seq[String]): Unit = {
    val maintenance = icebergConfig.maintenance
    tableNames.foreach { tableName =>
      val fullTableName = resolveTableName(tableName)
      try {
        maintenance.snapshotRetentionDays.foreach { days =>
          val olderThan = java.time.Instant.now().minusSeconds(days.toLong * 86400)
          spark.sql(
            s"CALL ${icebergConfig.catalogName}.system.expire_snapshots(" +
              s"table => '$fullTableName', " +
              s"older_than => TIMESTAMP '${sqlTimestampFmt.format(olderThan)}')"
          )
        }
        maintenance.targetFileSizeMb.foreach { size =>
          spark.sql(
            s"CALL ${icebergConfig.catalogName}.system.rewrite_data_files(" +
              s"table => '$fullTableName', " +
              s"options => map('target-file-size-bytes', '${size.toLong * 1024 * 1024}'))"
          )
        }
        maintenance.orphanRetentionMinutes.foreach { mins =>
          val retentionMinutes = math.max(mins, 1440)
          val olderThan = java.time.Instant.now().minusSeconds(retentionMinutes.toLong * 60)
          spark.sql(
            s"CALL ${icebergConfig.catalogName}.system.remove_orphan_files(" +
              s"table => '$fullTableName', " +
              s"older_than => TIMESTAMP '${sqlTimestampFmt.format(olderThan)}')"
          )
        }
        if (maintenance.enableManifestRewrite) {
          spark.sql(s"CALL ${icebergConfig.catalogName}.system.rewrite_manifests('$fullTableName')")
        }
        logger.info(s"Maintenance completed on $fullTableName")
      } catch {
        case e: Exception =>
          logger.warn(s"Maintenance failed on $fullTableName: ${e.getMessage}")
      }
    }
  }

  private def createOrUpdateTable(fullTableName: String, schema: StructType): Unit = {
    val exists =
      try {
        spark.sql(s"DESCRIBE TABLE $fullTableName")
        true
      } catch {
        case _: org.apache.spark.sql.AnalysisException => false
      }

    if (!exists) {
      val columns = schema.fields.map(f => s"${f.name} ${f.dataType.sql}").mkString(", ")
      spark.sql(s"CREATE TABLE IF NOT EXISTS $fullTableName ($columns) USING iceberg")

      val props = Map(
        "format-version" -> icebergConfig.formatVersion.toString,
        "write.format.default" -> icebergConfig.fileFormat
      )
      props.foreach { case (k, v) =>
        spark.sql(s"ALTER TABLE $fullTableName SET TBLPROPERTIES ('$k' = '$v')")
      }
      logger.info(s"Created Iceberg table $fullTableName")
    } else {
      val currentColumns = spark.table(fullTableName).schema.fieldNames.toSet
      schema.fields.filterNot(f => currentColumns.contains(f.name)).foreach { field =>
        spark.sql(s"ALTER TABLE $fullTableName ADD COLUMN ${field.name} ${field.dataType.sql}")
        logger.info(s"Added column ${field.name} to $fullTableName")
      }
    }
  }
}

case class DerivedTableResult(
    tableName: String,
    success: Boolean,
    recordsWritten: Long = 0,
    error: Option[String] = None
)
