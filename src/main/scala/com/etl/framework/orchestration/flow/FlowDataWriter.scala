package com.etl.framework.orchestration.flow

import com.etl.framework.config.{FlowConfig, GlobalConfig, LoadMode}
import com.etl.framework.core.AdditionalTableMetadata
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
    icebergTableWriter: Option[IcebergTableWriter] = None
) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Writes validated data, using Iceberg if available or Parquet fallback
    */
  def writeValidated(
      validData: DataFrame,
      batchId: String
  ): WriteResult = {
    icebergTableWriter match {
      case Some(writer) =>
        writeWithIceberg(writer, validData, batchId)
      case None =>
        writeParquet(validData, batchId)
    }
  }

  private def writeWithIceberg(
      writer: IcebergTableWriter,
      validData: DataFrame,
      batchId: String
  ): WriteResult = {
    val result = flowConfig.loadMode.`type` match {
      case LoadMode.Full  => writer.writeFullLoad(validData, flowConfig)
      case LoadMode.Delta => writer.writeDeltaLoad(validData, flowConfig)
      case LoadMode.SCD2  => writer.writeSCD2Load(validData, flowConfig)
    }
    writer.tagBatchSnapshot(flowConfig, result, batchId)
  }

  private def writeParquet(
      validData: DataFrame,
      batchId: String
  ): WriteResult = {
    val outputPath = flowConfig.output.path.getOrElse(
      s"${globalConfig.paths.outputPath}/${flowConfig.name}"
    )

    TimingUtil.timed(logger, s"Write validated data to $outputPath") {
      var writer = validData.write
        .mode(SaveMode.Overwrite)
        .format(flowConfig.output.format.name)
        .option("compression", flowConfig.output.compression)

      flowConfig.output.options.foreach { case (key, value) =>
        writer = writer.option(key, value)
      }

      if (flowConfig.output.partitionBy.nonEmpty) {
        writer = writer.partitionBy(flowConfig.output.partitionBy: _*)
      }

      writer.save(outputPath)
      logger.info("Validated data written successfully")

      WriteResult(recordsWritten = 0L, snapshotId = None)
    }
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

  /** Writes an additional table
    */
  def writeAdditionalTable(
      tableName: String,
      data: DataFrame,
      outputPath: Option[String],
      dagMetadata: Option[AdditionalTableMetadata]
  ): Unit = {
    val path = outputPath.getOrElse(
      s"${globalConfig.paths.outputPath}/${flowConfig.name}_${tableName}"
    )

    var writer = data.write
      .mode(SaveMode.Overwrite)
      .format("parquet")

    dagMetadata.foreach { metadata =>
      if (metadata.partitionBy.nonEmpty) {
        writer = writer.partitionBy(metadata.partitionBy: _*)
      }
    }

    writer.save(path)

    logger.info(s"Additional table $tableName written to $path")
  }
}
