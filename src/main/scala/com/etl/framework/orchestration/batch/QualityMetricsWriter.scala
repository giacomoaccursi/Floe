package com.etl.framework.orchestration.batch

import com.etl.framework.config.{GlobalConfig, IcebergConfig}
import com.etl.framework.iceberg.{OrphanReport, IcebergTableManager}
import com.etl.framework.orchestration.flow.FlowResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, lit, map, typedLit}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

class QualityMetricsWriter(globalConfig: GlobalConfig)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  def write(
      batchId: String,
      flowResults: Seq[FlowResult],
      orphanReports: Seq[OrphanReport],
      executionTimeMs: Long
  ): Unit = {
    globalConfig.processing.qualityMetricsTable match {
      case None => // disabled
      case Some(tableName) =>
        try {
          val fullTableName = s"${globalConfig.iceberg.catalogName}.default.$tableName"
          ensureTable(fullTableName)
          val df = buildMetricsDataFrame(batchId, flowResults, orphanReports, executionTimeMs)
          df.writeTo(fullTableName).append()
          logger.info(s"Quality metrics written to $fullTableName: ${flowResults.size} flow rows")
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to write quality metrics: ${e.getMessage}")
        }
    }
  }

  private def ensureTable(fullTableName: String): Unit = {
    try {
      spark.sql(s"DESCRIBE TABLE $fullTableName")
    } catch {
      case _: org.apache.spark.sql.AnalysisException =>
        spark.sql(
          s"""CREATE TABLE $fullTableName (
             |  batch_id STRING,
             |  batch_timestamp TIMESTAMP,
             |  flow_name STRING,
             |  input_records LONG,
             |  valid_records LONG,
             |  rejected_records LONG,
             |  rejection_rate DOUBLE,
             |  orphan_count LONG,
             |  execution_time_ms LONG,
             |  success BOOLEAN
             |) USING iceberg""".stripMargin
        )
        logger.info(s"Created quality metrics table: $fullTableName")
    }
  }

  private def buildMetricsDataFrame(
      batchId: String,
      flowResults: Seq[FlowResult],
      orphanReports: Seq[OrphanReport],
      executionTimeMs: Long
  ): DataFrame = {
    import spark.implicits._

    val orphansByFlow = orphanReports.groupBy(_.flowName).mapValues(_.map(_.orphanCount).sum)

    if (flowResults.isEmpty) {
      // Empty batch — write a single summary row
      Seq(
        (batchId, "batch_summary", 0L, 0L, 0L, 0.0, 0L, executionTimeMs, true)
      ).toDF(
        "batch_id",
        "flow_name",
        "input_records",
        "valid_records",
        "rejected_records",
        "rejection_rate",
        "orphan_count",
        "execution_time_ms",
        "success"
      ).withColumn("batch_timestamp", current_timestamp())
        .select(
          col("batch_id"),
          col("batch_timestamp"),
          col("flow_name"),
          col("input_records"),
          col("valid_records"),
          col("rejected_records"),
          col("rejection_rate"),
          col("orphan_count"),
          col("execution_time_ms"),
          col("success")
        )
    } else {
      flowResults
        .map { r =>
          (
            batchId,
            r.flowName,
            r.inputRecords,
            r.validRecords,
            r.rejectedRecords,
            r.rejectionRate,
            orphansByFlow.getOrElse(r.flowName, 0L),
            r.executionTimeMs,
            r.success
          )
        }
        .toDF(
          "batch_id",
          "flow_name",
          "input_records",
          "valid_records",
          "rejected_records",
          "rejection_rate",
          "orphan_count",
          "execution_time_ms",
          "success"
        )
        .withColumn("batch_timestamp", current_timestamp())
        .select(
          col("batch_id"),
          col("batch_timestamp"),
          col("flow_name"),
          col("input_records"),
          col("valid_records"),
          col("rejected_records"),
          col("rejection_rate"),
          col("orphan_count"),
          col("execution_time_ms"),
          col("success")
        )
    }
  }
}
