package com.etl.framework.io.writers

import com.etl.framework.config.{FileFormat, OutputConfig} // Import FileFormat
import com.etl.framework.exceptions.DataWriteException
import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/** Writes Dataset[FinalModel] to configured output format
  *
  * This writer supports:
  *   - Multiple output formats (Parquet, JSON, CSV, Avro, etc.)
  *   - Partitioning by specified columns
  *   - Compression options
  *   - Custom output options
  *
  * The Final Model represents the external data model used for export, APIs,
  * and downstream consumption.
  *
  * @tparam F
  *   FinalModel type (must have an Encoder)
  * @param outputConfig
  *   Configuration for output format, path, partitioning, etc.
  * @param spark
  *   Implicit SparkSession
  */
class FinalModelWriter[F: Encoder](
    outputConfig: OutputConfig
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Writes Dataset[FinalModel] to the specified output path
    *
    * The output format, partitioning, and compression are determined by the
    * outputConfig.
    *
    * @param dataset
    *   Dataset containing final model records
    * @param outputPath
    *   Path where the final model should be written
    * @throws DataWriteException
    *   if writing fails
    */
  def write(dataset: Dataset[F], outputPath: String): Unit = {
    logger.info(s"Writing Final Model to: $outputPath")
    logger.info(s"Output format: ${outputConfig.format.name}") // Use .name
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

      logger.info(s"Successfully wrote Final Model to $outputPath")
    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to write Final Model to $outputPath: ${e.getMessage}"
        )
        throw DataWriteException(
          outputType = "Final Model",
          outputPath = outputPath,
          details = e.getMessage,
          cause = e
        )
    }
  }

  /** Writes Dataset[FinalModel] with custom save mode
    *
    * This method allows overriding the default Overwrite mode.
    *
    * @param dataset
    *   Dataset containing final model records
    * @param outputPath
    *   Path where the final model should be written
    * @param saveMode
    *   Spark SaveMode (Overwrite, Append, ErrorIfExists, Ignore)
    * @throws DataWriteException
    *   if writing fails
    */
  def write(
      dataset: Dataset[F],
      outputPath: String,
      saveMode: SaveMode
  ): Unit = {
    logger.info(s"Writing Final Model to: $outputPath with mode: $saveMode")
    logger.info(s"Output format: ${outputConfig.format.name}") // Use .name
    logger.info(s"Record count: ${dataset.count()}")

    try {
      var writer = dataset.write
        .mode(saveMode)
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

      logger.info(s"Successfully wrote Final Model to $outputPath")
    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to write Final Model to $outputPath: ${e.getMessage}"
        )
        throw DataWriteException(
          outputType = "Final Model",
          outputPath = outputPath,
          details = e.getMessage,
          cause = e
        )
    }
  }

  /** Writes Dataset[FinalModel] to multiple output paths
    *
    * This is useful for writing the same final model to different locations or
    * formats (e.g., Parquet for storage, JSON for API).
    *
    * @param dataset
    *   Dataset containing final model records
    * @param outputPaths
    *   Sequence of output paths
    * @throws DataWriteException
    *   if any write operation fails
    */
  def writeMultiple(dataset: Dataset[F], outputPaths: Seq[String]): Unit = {
    logger.info(s"Writing Final Model to ${outputPaths.size} output paths")

    outputPaths.foreach { path =>
      write(dataset, path)
    }

    logger.info(
      s"Successfully wrote Final Model to all ${outputPaths.size} paths"
    )
  }
}

/** Companion object for creating FinalModelWriter instances
  */
object FinalModelWriter {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Creates a FinalModelWriter with the given output configuration
    *
    * @param outputConfig
    *   Configuration for output format, partitioning, etc.
    * @param spark
    *   Implicit SparkSession
    * @tparam F
    *   FinalModel type
    * @return
    *   FinalModelWriter instance
    */
  def apply[F: Encoder](
      outputConfig: OutputConfig
  )(implicit spark: SparkSession): FinalModelWriter[F] = {
    logger.debug(
      s"Creating FinalModelWriter with format: ${outputConfig.format.name}"
    ) // Use .name
    new FinalModelWriter[F](outputConfig)
  }

  /** Creates a FinalModelWriter with default Parquet configuration
    *
    * @param spark
    *   Implicit SparkSession
    * @tparam F
    *   FinalModel type
    * @return
    *   FinalModelWriter instance with Parquet format
    */
  def parquet[F: Encoder](implicit spark: SparkSession): FinalModelWriter[F] = {
    val defaultConfig = OutputConfig(
      path = None,
      rejectedPath = None,
      format = FileFormat.Parquet, // Use Enum
      partitionBy = Seq.empty,
      compression = "snappy",
      options = Map.empty
    )
    new FinalModelWriter[F](defaultConfig)
  }

  /** Creates a FinalModelWriter with JSON configuration
    *
    * @param spark
    *   Implicit SparkSession
    * @tparam F
    *   FinalModel type
    * @return
    *   FinalModelWriter instance with JSON format
    */
  def json[F: Encoder](implicit spark: SparkSession): FinalModelWriter[F] = {
    val jsonConfig = OutputConfig(
      path = None,
      rejectedPath = None,
      format = FileFormat.JSON, // Use Enum
      partitionBy = Seq.empty,
      compression = "gzip",
      options = Map.empty
    )
    new FinalModelWriter[F](jsonConfig)
  }

  /** Creates a FinalModelWriter with CSV configuration
    *
    * @param spark
    *   Implicit SparkSession
    * @tparam F
    *   FinalModel type
    * @return
    *   FinalModelWriter instance with CSV format
    */
  def csv[F: Encoder](implicit spark: SparkSession): FinalModelWriter[F] = {
    val csvConfig = OutputConfig(
      path = None,
      rejectedPath = None,
      format = FileFormat.CSV, // Use Enum
      partitionBy = Seq.empty,
      compression = "gzip",
      options = Map("header" -> "true")
    )
    new FinalModelWriter[F](csvConfig)
  }
}
