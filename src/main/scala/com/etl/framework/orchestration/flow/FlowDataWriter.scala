package com.etl.framework.orchestration.flow

import com.etl.framework.config.{FlowConfig, GlobalConfig, LoadMode}
import com.etl.framework.iceberg.{IcebergTableWriter, WriteResult}
import com.etl.framework.util.TimingUtil
import com.etl.framework.validation.ValidationColumns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

import java.time.Instant

/** Handles writing of validated, rejected, and additional table data
  */
class FlowDataWriter(
    flowConfig: FlowConfig,
    globalConfig: GlobalConfig,
    icebergTableWriter: IcebergTableWriter
) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Writes validated data to Iceberg
    */
  def writeValidated(
      validData: DataFrame,
      batchId: String
  ): WriteResult = {
    val result = flowConfig.loadMode.`type` match {
      case LoadMode.Full  => icebergTableWriter.writeFullLoad(validData, flowConfig)
      case LoadMode.Delta => icebergTableWriter.writeDeltaLoad(validData, flowConfig)
      case LoadMode.SCD2  => icebergTableWriter.writeSCD2Load(validData, flowConfig)
    }
    icebergTableWriter.tagBatchSnapshot(flowConfig, result, batchId)
  }

  /** Writes rejected data
    */
  def writeRejected(rejectedData: DataFrame, batchId: String): Unit = {
    val rejectedPath = flowConfig.output.rejectedPath.getOrElse(
      s"${globalConfig.paths.rejectedPath}/${flowConfig.name}"
    )

    TimingUtil.timed(logger, s"Write rejected data to $rejectedPath") {
      val rejectedWithAudit = rejectedData
        .withColumn(REJECTED_AT, lit(Instant.now().toString))
        .withColumn(BATCH_ID, lit(batchId))

      rejectedWithAudit.write
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .save(rejectedPath)
    }
  }

  /** Writes validation warnings (PK + warning metadata) to a separate Parquet file.
    */
  def writeWarnings(warnedData: DataFrame, batchId: String): Unit = {
    val basePath = globalConfig.paths.warningsPath.getOrElse(s"${globalConfig.paths.outputPath}/warnings")
    val warningsPath = s"$basePath/${flowConfig.name}"

    TimingUtil.timed(logger, s"Write warnings to $warningsPath") {
      warnedData
        .withColumn(BATCH_ID, lit(batchId))
        .write
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .save(warningsPath)
    }
  }

}
