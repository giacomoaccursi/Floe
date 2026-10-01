package com.etl.framework.orchestration.batch

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.iceberg.{IcebergTableManager, OrphanReport}
import com.etl.framework.orchestration.flow.FlowResult
import com.etl.framework.util.SqlIdentifier
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp}
import org.apache.spark.sql.types.{
  BooleanType,
  DoubleType,
  LongType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.slf4j.LoggerFactory

class QualityMetricsWriter(globalConfig: GlobalConfig, flowConfigs: Seq[FlowConfig])(implicit
    spark: SparkSession
) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val flowConfigMap = flowConfigs.map(fc => fc.name -> fc).toMap
  private val tableManager = new IcebergTableManager(spark, globalConfig.iceberg)
  private val metricsSchema = StructType(
    Seq(
      StructField("batch_id", StringType),
      StructField("batch_timestamp", TimestampType),
      StructField("batch_success", BooleanType),
      StructField("flow_name", StringType),
      StructField("load_mode", StringType),
      StructField("input_records", LongType),
      StructField("valid_records", LongType),
      StructField("rejected_records", LongType),
      StructField("rejection_rate", DoubleType),
      StructField("records_written", LongType),
      StructField("orphan_count", LongType),
      StructField("execution_time_ms", LongType),
      StructField("success", BooleanType)
    )
  )

  def write(
      batchId: String,
      flowResults: Seq[FlowResult],
      orphanReports: Seq[OrphanReport],
      executionTimeMs: Long,
      batchSuccess: Boolean
  ): Unit = {
    globalConfig.processing.qualityMetricsTable match {
      case None =>
      case Some(tableName) =>
        try {
          val fullTableName = globalConfig.iceberg.fullTableName(tableName)
          ensureTable(fullTableName)
          val df = buildMetricsDataFrame(batchId, flowResults, orphanReports, executionTimeMs, batchSuccess)
          df.writeTo(SqlIdentifier.quoteMultipart(fullTableName)).append()
          logger.info(s"Quality metrics written to $fullTableName: ${flowResults.size} flow rows")
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to write quality metrics: ${e.getMessage}")
        }
    }
  }

  private def ensureTable(fullTableName: String): Unit = {
    tableManager.prepareTable(fullTableName, metricsSchema)
  }

  private def buildMetricsDataFrame(
      batchId: String,
      flowResults: Seq[FlowResult],
      orphanReports: Seq[OrphanReport],
      executionTimeMs: Long,
      batchSuccess: Boolean
  ): DataFrame = {
    import spark.implicits._

    val orphansByFlow = orphanReports.groupBy(_.flowName).mapValues(_.map(_.orphanCount).sum).toMap

    val columns = Seq(
      "batch_id",
      "batch_timestamp",
      "batch_success",
      "flow_name",
      "load_mode",
      "input_records",
      "valid_records",
      "rejected_records",
      "rejection_rate",
      "records_written",
      "orphan_count",
      "execution_time_ms",
      "success"
    )

    if (flowResults.isEmpty) {
      Seq((batchId, batchSuccess, "batch_summary", "none", 0L, 0L, 0L, 0.0, 0L, 0L, executionTimeMs, true))
        .toDF(
          "batch_id",
          "batch_success",
          "flow_name",
          "load_mode",
          "input_records",
          "valid_records",
          "rejected_records",
          "rejection_rate",
          "records_written",
          "orphan_count",
          "execution_time_ms",
          "success"
        )
        .withColumn("batch_timestamp", current_timestamp())
        .select(columns.map(col): _*)
    } else {
      flowResults
        .map { r =>
          val loadMode = flowConfigMap.get(r.flowName).map(_.loadMode.`type`.name).getOrElse("unknown")
          val recordsWritten = r.icebergMetadata.map(_.recordsWritten).getOrElse(r.validRecords)
          (
            batchId,
            batchSuccess,
            r.flowName,
            loadMode,
            r.inputRecords,
            r.validRecords,
            r.rejectedRecords,
            r.rejectionRate,
            recordsWritten,
            orphansByFlow.getOrElse(r.flowName, 0L),
            r.executionTimeMs,
            r.success
          )
        }
        .toDF(
          "batch_id",
          "batch_success",
          "flow_name",
          "load_mode",
          "input_records",
          "valid_records",
          "rejected_records",
          "rejection_rate",
          "records_written",
          "orphan_count",
          "execution_time_ms",
          "success"
        )
        .withColumn("batch_timestamp", current_timestamp())
        .select(columns.map(col): _*)
    }
  }
}
