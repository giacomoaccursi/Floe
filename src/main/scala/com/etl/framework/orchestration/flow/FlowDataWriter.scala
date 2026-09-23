package com.etl.framework.orchestration.flow

import com.etl.framework.config.{FlowConfig, GlobalConfig, LoadMode}
import com.etl.framework.iceberg.{CommitContext, IcebergTableWriter, WriteResult}
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
      batchId: String,
      effectiveAt: Instant
  ): WriteResult = {
    val operationType = flowConfig.loadMode.`type`.name
    val context = CommitContext.forFlow(batchId, flowConfig.name, operationType, effectiveAt)
    val result = flowConfig.loadMode.`type` match {
      case LoadMode.Full  => icebergTableWriter.writeFullLoad(validData, flowConfig, context)
      case LoadMode.Delta => icebergTableWriter.writeDeltaLoad(validData, flowConfig, context)
      case LoadMode.SCD2  => icebergTableWriter.writeSCD2Load(validData, flowConfig, context)
    }
    icebergTableWriter.tagBatchSnapshot(flowConfig, result, batchId)
  }

  /** Writes rejected data
    */
  def writeRejected(rejectedData: DataFrame, batchId: String): Unit = {
    val rejectedBasePath = flowConfig.output.rejectedPath.getOrElse(
      s"${globalConfig.paths.rejectedPath}/${flowConfig.name}"
    )
    val rejectedPath = s"$rejectedBasePath/batch_id=$batchId"

    TimingUtil.timed(logger, s"Write rejected data to $rejectedPath") {
      val rejectedWithAudit = rejectedData
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
    val warningsPath = s"$basePath/${flowConfig.name}/batch_id=$batchId"

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
