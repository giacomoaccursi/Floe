package com.etl.framework.pipeline

import com.etl.framework.config.IcebergConfig
import com.etl.framework.iceberg.{IcebergMaintenanceRunner, IcebergTableManager, MaintenanceResult}
import com.etl.framework.util.SqlIdentifier
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/** Executes derived table functions and writes results to Iceberg as full-load tables. Each derived table gets snapshot
  * tagging and post-write maintenance.
  */
class DerivedTableExecutor(
    icebergConfig: IcebergConfig
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val tableManager = new IcebergTableManager(spark, icebergConfig)

  /** Executes all derived table functions and writes results to Iceberg. Runs maintenance on successful tables. */
  def execute(
      derivedTables: Seq[(String, DerivedTableContext => DataFrame)],
      batchId: String
  ): Seq[DerivedTableResult] = {
    val ctx = DerivedTableContext(spark, batchId, icebergConfig.catalogName, icebergConfig.namespace)

    val results = derivedTables.map { case (tableName, fn) =>
      executeSingle(tableName, fn, ctx, batchId)
    }

    results.map { result =>
      if (result.success) result.copy(maintenanceResult = Some(runMaintenance(result.tableName)))
      else result
    }
  }

  /** Executes a single derived table: computes the DataFrame, writes to Iceberg, tags the snapshot. */
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
    icebergConfig.fullTableName(tableName)

  private def writeToIceberg(fullTableName: String, df: DataFrame): Long = {
    createOrUpdateTable(fullTableName, df.schema)
    val cachedDf = df.cache()
    try {
      cachedDf.writeTo(SqlIdentifier.quoteMultipart(fullTableName)).overwrite(lit(true))
      cachedDf.count()
    } finally {
      cachedDf.unpersist()
    }
  }

  private def tagSnapshot(fullTableName: String, batchId: String): Unit = {
    if (!icebergConfig.enableSnapshotTagging) return

    try {
      tableManager.getCurrentSnapshotId(fullTableName).foreach { snapshotId =>
        if (tableManager.tagSnapshot(fullTableName, snapshotId, batchId))
          logger.info(s"Tagged derived table snapshot $snapshotId on $fullTableName")
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to tag snapshot on $fullTableName: ${e.getMessage}")
    }
  }

  private def runMaintenance(tableName: String): MaintenanceResult = {
    val runner = new IcebergMaintenanceRunner(spark, icebergConfig)
    val fullTableName = resolveTableName(tableName)
    try {
      runner.run(fullTableName, icebergConfig.maintenance)
      logger.info(s"Maintenance completed on $fullTableName")
      MaintenanceResult(tableName, "derived", success = true)
    } catch {
      case e: Exception =>
        logger.warn(s"Maintenance failed on $fullTableName: ${e.getMessage}", e)
        MaintenanceResult(tableName, "derived", success = false, error = Some(e.getMessage))
    }
  }

  /** Creates the Iceberg table if it doesn't exist, or adds missing columns if it does. */
  private def createOrUpdateTable(fullTableName: String, schema: StructType): Unit = {
    val exists =
      try {
        spark.sql(s"DESCRIBE TABLE ${SqlIdentifier.quoteMultipart(fullTableName)}")
        true
      } catch {
        case _: org.apache.spark.sql.AnalysisException => false
      }

    if (!exists) {
      val columns = schema.fields.map(f => s"${SqlIdentifier.quote(f.name)} ${f.dataType.sql}").mkString(", ")
      val sqlTableName = SqlIdentifier.quoteMultipart(fullTableName)
      spark.sql(s"CREATE TABLE IF NOT EXISTS $sqlTableName ($columns) USING iceberg")

      val props = Map(
        "format-version" -> icebergConfig.formatVersion.toString,
        "write.format.default" -> icebergConfig.fileFormat
      )
      props.foreach { case (k, v) =>
        spark.sql(
          s"ALTER TABLE $sqlTableName SET TBLPROPERTIES " +
            s"(${SqlIdentifier.stringLiteral(k)} = ${SqlIdentifier.stringLiteral(v)})"
        )
      }
      logger.info(s"Created Iceberg table $fullTableName")
    } else {
      val sqlTableName = SqlIdentifier.quoteMultipart(fullTableName)
      val currentColumns = spark.table(sqlTableName).schema.fieldNames.toSet
      schema.fields.filterNot(f => currentColumns.contains(f.name)).foreach { field =>
        spark.sql(s"ALTER TABLE $sqlTableName ADD COLUMN ${SqlIdentifier.quote(field.name)} ${field.dataType.sql}")
        logger.info(s"Added column ${field.name} to $fullTableName")
      }
    }
  }
}

case class DerivedTableResult(
    tableName: String,
    success: Boolean,
    recordsWritten: Long = 0,
    error: Option[String] = None,
    maintenanceResult: Option[MaintenanceResult] = None
)
