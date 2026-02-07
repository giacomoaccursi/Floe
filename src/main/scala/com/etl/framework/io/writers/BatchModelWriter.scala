package com.etl.framework.io.writers

import com.etl.framework.config.OutputConfig
import com.etl.framework.exceptions.DataWriteException
import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/** Writes Dataset[BatchModel] to Parquet
  */
class BatchModelWriter[T: Encoder](
    outputConfig: OutputConfig
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Writes Dataset[T] to Parquet format
    */
  def write(dataset: Dataset[T], outputPath: String): Unit = {
    logger.info(s"Writing Batch Model to: $outputPath")
    logger.info(s"Record count: ${dataset.count()}")

    try {
      var writer = dataset.write
        .mode(SaveMode.Overwrite)
        .format(outputConfig.format.name) // Use .name

      // Apply partitioning if configured
      if (outputConfig.partitionBy.nonEmpty) {
        logger.info(
          s"Partitioning by: ${outputConfig.partitionBy.mkString(", ")}"
        )
        writer = writer.partitionBy(outputConfig.partitionBy: _*)
      }

      // Apply compression if configured
      if (outputConfig.compression.nonEmpty) {
        logger.info(s"Using compression: ${outputConfig.compression}")
        writer = writer.option("compression", outputConfig.compression)
      }

      // Apply additional options
      outputConfig.options.foreach { case (key, value) =>
        logger.debug(s"Applying option: $key = $value")
        writer = writer.option(key, value)
      }

      // Write to output path
      writer.save(outputPath)

      logger.info(s"Successfully wrote Batch Model to $outputPath")
    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to write Batch Model to $outputPath: ${e.getMessage}"
        )
        throw DataWriteException(
          outputType = "Batch Model",
          outputPath = outputPath,
          details = e.getMessage,
          cause = e
        )
    }
  }
}

/** Companion object for creating BatchModelWriter instances
  */
object BatchModelWriter {

  /** Creates a BatchModelWriter with the given output configuration
    */
  def apply[T: Encoder](
      outputConfig: OutputConfig
  )(implicit spark: SparkSession): BatchModelWriter[T] = {
    new BatchModelWriter[T](outputConfig)
  }
}
