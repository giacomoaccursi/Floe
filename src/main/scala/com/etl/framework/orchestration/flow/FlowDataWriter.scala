package com.etl.framework.orchestration.flow

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.core.AdditionalTableMetadata
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
    globalConfig: GlobalConfig
) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Writes validated data
    */
  def writeValidated(validData: DataFrame, batchId: String): Unit = {
    val outputPath = flowConfig.output.path.getOrElse(
      s"${globalConfig.paths.outputPath}/${flowConfig.name}"
    )

    TimingUtil.timed(logger, s"Write validated data to $outputPath") {
      val recordCount = validData.count()

      var writer = validData.write
        .mode(SaveMode.Overwrite)
        .format(flowConfig.output.format.name) // Use .name for enum
        .option("compression", flowConfig.output.compression)

      // Apply additional options
      flowConfig.output.options.foreach { case (key, value) =>
        writer = writer.option(key, value)
      }

      // Apply partitioning if configured
      if (flowConfig.output.partitionBy.nonEmpty) {
        writer = writer.partitionBy(flowConfig.output.partitionBy: _*)
      }

      writer.save(outputPath)
      logger.info(s"Wrote $recordCount validated records")
    }
  }

  /** Writes rejected data
    */
  def writeRejected(rejectedData: DataFrame, batchId: String): Unit = {
    val rejectedPath = flowConfig.output.rejectedPath.getOrElse(
      s"${globalConfig.paths.rejectedPath}/${flowConfig.name}"
    )

    TimingUtil.timed(logger, s"Write rejected data to $rejectedPath") {
      val recordCount = rejectedData.count()

      // Add audit fields
      val rejectedWithAudit = rejectedData
        .withColumn(REJECTED_AT, lit(Instant.now().toString))
        .withColumn(BATCH_ID, lit(batchId))

      // Overwrite previous rejected records
      rejectedWithAudit.write
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .save(rejectedPath)

      logger.info(s"Wrote $recordCount rejected records")
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

    val recordCount = data.count()

    var writer = data.write
      .mode(SaveMode.Overwrite)
      .format("parquet")

    // Apply partitioning if specified in metadata
    dagMetadata.foreach { metadata =>
      if (metadata.partitionBy.nonEmpty) {
        writer = writer.partitionBy(metadata.partitionBy: _*)
      }
    }

    writer.save(path)

    logger.info(s"Additional table $tableName: $recordCount records → $path")
  }
}
