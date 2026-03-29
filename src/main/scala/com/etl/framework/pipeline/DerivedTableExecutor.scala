package com.etl.framework.pipeline

import com.etl.framework.config.IcebergConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/** Executes derived table functions and writes results to Iceberg as full-load tables.
  */
class DerivedTableExecutor(
    icebergConfig: IcebergConfig
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Executes all derived tables and writes each to Iceberg.
    */
  def execute(
      derivedTables: Seq[(String, DerivedTableContext => DataFrame)],
      batchId: String
  ): Seq[DerivedTableResult] = {
    val ctx = DerivedTableContext(spark, batchId, icebergConfig.catalogName)

    derivedTables.map { case (tableName, fn) =>
      executeSingle(tableName, fn, ctx)
    }
  }

  private def executeSingle(
      tableName: String,
      fn: DerivedTableContext => DataFrame,
      ctx: DerivedTableContext
  ): DerivedTableResult = {
    logger.info(s"Computing derived table: $tableName")
    try {
      val df = fn(ctx)
      val recordsWritten = writeToIceberg(tableName, df)
      logger.info(s"Derived table $tableName written: $recordsWritten records")
      DerivedTableResult(tableName, success = true, recordsWritten = recordsWritten)
    } catch {
      case e: Exception =>
        logger.error(s"Derived table $tableName failed: ${e.getMessage}", e)
        DerivedTableResult(tableName, success = false, error = Some(e.getMessage))
    }
  }

  private def writeToIceberg(tableName: String, df: DataFrame): Long = {
    val fullTableName = s"${icebergConfig.catalogName}.default.$tableName"
    createOrUpdateTable(fullTableName, df.schema)

    val cachedDf = df.cache()
    try {
      cachedDf.writeTo(fullTableName).overwrite(lit(true))
      cachedDf.count()
    } finally {
      cachedDf.unpersist()
    }
  }

  private def createOrUpdateTable(tableName: String, schema: StructType): Unit = {
    val exists = try {
      spark.sql(s"DESCRIBE TABLE $tableName")
      true
    } catch {
      case _: org.apache.spark.sql.AnalysisException => false
    }

    if (!exists) {
      val columns = schema.fields.map(f => s"${f.name} ${f.dataType.sql}").mkString(", ")
      spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName ($columns) USING iceberg")

      val props = Map(
        "format-version" -> icebergConfig.formatVersion.toString,
        "write.format.default" -> icebergConfig.fileFormat
      )
      props.foreach { case (k, v) =>
        spark.sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('$k' = '$v')")
      }

      logger.info(s"Created Iceberg table $tableName")
    } else {
      // Add new columns if schema evolved
      val currentColumns = spark.table(tableName).schema.fieldNames.toSet
      schema.fields.filterNot(f => currentColumns.contains(f.name)).foreach { field =>
        spark.sql(s"ALTER TABLE $tableName ADD COLUMN ${field.name} ${field.dataType.sql}")
        logger.info(s"Added column ${field.name} to $tableName")
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
