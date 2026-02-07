package com.etl.framework.pipeline

import com.etl.framework.config.{FileFormat, OutputConfig}
import com.etl.framework.exceptions.MissingConfigFieldException
import com.etl.framework.io.writers.FinalModelWriter
import com.etl.framework.mapping.FinalModelMapper
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.slf4j.LoggerFactory

/** Fluent API builder for Export (Export) pipeline
  */
class ExportPipeline[B: Encoder, F: Encoder] private (
    batchModelPath: Option[String],
    batchModelDataset: Option[Dataset[B]],
    mapper: FinalModelMapper[B, F],
    outputConfig: OutputConfig,
    outputPath: Option[String]
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Executes Export pipeline Returns Dataset[F] (Final Model)
    */
  def execute(): Dataset[F] = {
    logger.info("Executing Export pipeline")

    // 1. Load Batch Model
    logger.info("Step 1: Loading Batch Model")
    val batchModel = (batchModelDataset, batchModelPath) match {
      case (Some(dataset), _) =>
        logger.info("Using provided Batch Model dataset")
        dataset
      case (None, Some(path)) =>
        logger.info(s"Loading Batch Model from: $path")
        spark.read.parquet(path).as[B]
      case (None, None) =>
        throw MissingConfigFieldException(
          file = "export-pipeline",
          field = "batchModelPath or batchModelDataset",
          section = "batch model source"
        )
    }

    logger.info(s"Batch Model loaded with ${batchModel.count()} records")

    // 2. Apply mapper to convert Batch Model to Final Model
    logger.info("Step 2: Mapping Batch Model to Final Model")
    val finalModel = mapper.map(batchModel)
    logger.info(s"Final Model created with ${finalModel.count()} records")

    // 3. Write Final Model if output path is configured
    outputPath.foreach { path =>
      logger.info(s"Step 3: Writing Final Model to $path")
      val writer = new FinalModelWriter[F](outputConfig)
      writer.write(finalModel, path)
    }

    logger.info("Export pipeline execution completed")
    finalModel
  }

  /** Returns the mapper
    */
  def getMapper: FinalModelMapper[B, F] = mapper

  /** Returns the output configuration
    */
  def getOutputConfig: OutputConfig = outputConfig
}

/** Builder for ExportPipeline
  */
class ExportPipelineBuilder[B: Encoder, F: Encoder](implicit
    spark: SparkSession
) {

  private val logger = LoggerFactory.getLogger(getClass)
  private var batchModelPathOpt: Option[String] = None
  private var batchModelDatasetOpt: Option[Dataset[B]] = None
  private var mapperOpt: Option[FinalModelMapper[B, F]] = None
  private var mapperClassNameOpt: Option[String] = None
  private var outputConfigOpt: Option[OutputConfig] = None
  private var outputPathOpt: Option[String] = None

  /** Sets the path to load the Batch Model from
    *
    * @param path
    *   Path to Batch Model (Parquet format)
    * @return
    *   This builder for chaining
    */
  def withBatchModelPath(path: String): ExportPipelineBuilder[B, F] = {
    logger.info(s"Setting Batch Model path: $path")
    this.batchModelPathOpt = Some(path)
    this
  }

  /** Sets the Batch Model dataset directly Alternative to withBatchModelPath
    * for in-memory datasets
    *
    * @param dataset
    *   Batch Model dataset
    * @return
    *   This builder for chaining
    */
  def withBatchModelDataset(
      dataset: Dataset[B]
  ): ExportPipelineBuilder[B, F] = {
    logger.info("Setting Batch Model dataset directly")
    this.batchModelDatasetOpt = Some(dataset)
    this
  }

  /** Specifies the Batch Model type (for type safety) This is a no-op method
    * that helps with type inference
    *
    * @return
    *   This builder for chaining
    */
  def withBatchModel[BB: Encoder](): ExportPipelineBuilder[BB, F] = {
    logger.info("Setting Batch Model type")
    // Create a new builder with the correct type
    val newBuilder = new ExportPipelineBuilder[BB, F]()
    newBuilder.batchModelPathOpt = this.batchModelPathOpt
    newBuilder.mapperClassNameOpt = this.mapperClassNameOpt
    newBuilder.outputConfigOpt = this.outputConfigOpt
    newBuilder.outputPathOpt = this.outputPathOpt
    newBuilder
  }

  /** Specifies the Final Model type (for type safety) This is a no-op method
    * that helps with type inference
    *
    * @return
    *   This builder for chaining
    */
  def withFinalModel[FF: Encoder](): ExportPipelineBuilder[B, FF] = {
    logger.info("Setting Final Model type")
    // Create a new builder with the correct type
    val newBuilder = new ExportPipelineBuilder[B, FF]()
    newBuilder.batchModelPathOpt = this.batchModelPathOpt
    newBuilder.mapperClassNameOpt = this.mapperClassNameOpt
    newBuilder.outputConfigOpt = this.outputConfigOpt
    newBuilder.outputPathOpt = this.outputPathOpt
    newBuilder
  }

  /** Sets the mapper for converting Batch Model to Final Model
    *
    * @param mapper
    *   FinalModelMapper instance
    * @return
    *   This builder for chaining
    */
  def withMapper(
      mapper: FinalModelMapper[B, F]
  ): ExportPipelineBuilder[B, F] = {
    logger.info("Setting Final Model mapper")
    this.mapperOpt = Some(mapper)
    this
  }

  /** Loads the mapper by class name using reflection The mapper class must
    * implement FinalModelMapper[B, F]
    *
    * @param className
    *   Fully qualified class name of the mapper
    * @return
    *   This builder for chaining
    */
  def withMapperClass(className: String): ExportPipelineBuilder[B, F] = {
    logger.info(s"Setting mapper class: $className")
    this.mapperClassNameOpt = Some(className)
    this
  }

  /** Sets the output configuration for writing the Final Model
    *
    * @param config
    *   Output configuration
    * @return
    *   This builder for chaining
    */
  def withOutputConfig(config: OutputConfig): ExportPipelineBuilder[B, F] = {
    logger.info("Setting output configuration")
    this.outputConfigOpt = Some(config)
    this
  }

  /** Sets the output path for writing the Final Model If not set, the Final
    * Model will not be written to disk
    *
    * @param path
    *   Output path
    * @return
    *   This builder for chaining
    */
  def withOutputPath(path: String): ExportPipelineBuilder[B, F] = {
    logger.info(s"Setting output path: $path")
    this.outputPathOpt = Some(path)
    this
  }

  /** Sets the output format (convenience method) Creates a default OutputConfig
    * with the specified format
    *
    * @param format
    *   Output format (parquet, json, csv, etc.)
    * @return
    *   This builder for chaining
    */
  def withOutputFormat(format: String): ExportPipelineBuilder[B, F] = {
    logger.info(s"Setting output format: $format")

    val formatEnum = FileFormat
      .fromString(format)
      .fold(
        err => throw new IllegalArgumentException(err),
        identity
      )

    val compression = formatEnum match {
      case FileFormat.Parquet               => "snappy"
      case FileFormat.JSON | FileFormat.CSV => "gzip"
      case _                                => ""
    }

    val options = if (formatEnum == FileFormat.CSV) {
      Map("header" -> "true")
    } else {
      Map.empty[String, String]
    }

    this.outputConfigOpt = Some(
      OutputConfig(
        path = None,
        rejectedPath = None,
        format = formatEnum,
        partitionBy = Seq.empty,
        compression = compression,
        options = options
      )
    )

    this
  }

  /** Sets partitioning columns for the output
    *
    * @param columns
    *   Columns to partition by
    * @return
    *   This builder for chaining
    */
  def withPartitioning(columns: String*): ExportPipelineBuilder[B, F] = {
    logger.info(s"Setting partitioning: ${columns.mkString(", ")}")

    val currentConfig = outputConfigOpt.getOrElse(
      OutputConfig(
        path = None,
        rejectedPath = None,
        format = FileFormat.Parquet,
        partitionBy = Seq.empty,
        compression = "snappy",
        options = Map.empty
      )
    )

    this.outputConfigOpt = Some(currentConfig.copy(partitionBy = columns.toSeq))
    this
  }

  /** Builds the ExportPipeline
    *
    * @return
    *   Configured ExportPipeline
    * @throws IllegalStateException
    *   if required configuration is missing
    */
  def build(): ExportPipeline[B, F] = {
    logger.info("Building ExportPipeline")

    // Validate that either path or dataset is provided
    if (batchModelPathOpt.isEmpty && batchModelDatasetOpt.isEmpty) {
      throw MissingConfigFieldException(
        file = "export-pipeline-builder",
        field = "batchModelPath or batchModelDataset",
        section = "batch model source"
      )
    }

    // Load or validate mapper
    val mapper = (mapperOpt, mapperClassNameOpt) match {
      case (Some(m), _) =>
        m
      case (None, Some(className)) =>
        FinalModelMapper.loadMapper[B, F](className)
      case (None, None) =>
        throw MissingConfigFieldException(
          file = "export-pipeline-builder",
          field = "mapper or mapperClass",
          section = "mapper configuration"
        )
    }

    // Use default output config if not provided
    val outputConfig = outputConfigOpt.getOrElse {
      logger.info(
        "No output config provided, using default Parquet configuration"
      )
      OutputConfig(
        path = None,
        rejectedPath = None,
        format = FileFormat.Parquet,
        partitionBy = Seq.empty,
        compression = "snappy",
        options = Map.empty
      )
    }

    logger.info("ExportPipeline built successfully")
    ExportPipeline.create[B, F](
      batchModelPathOpt,
      batchModelDatasetOpt,
      mapper,
      outputConfig,
      outputPathOpt
    )
  }
}

/** Companion object for ExportPipeline
  */
object ExportPipeline {

  /** Internal factory method to create ExportPipeline instances
    */
  private[pipeline] def create[B: Encoder, F: Encoder](
      batchModelPath: Option[String],
      batchModelDataset: Option[Dataset[B]],
      mapper: FinalModelMapper[B, F],
      outputConfig: OutputConfig,
      outputPath: Option[String]
  )(implicit spark: SparkSession): ExportPipeline[B, F] = {
    new ExportPipeline[B, F](
      batchModelPath,
      batchModelDataset,
      mapper,
      outputConfig,
      outputPath
    )
  }

  /** Creates a new ExportPipeline builder
    *
    * @tparam B
    *   Batch Model type
    * @tparam F
    *   Final Model type
    * @param spark
    *   Implicit SparkSession
    * @return
    *   New ExportPipelineBuilder
    */
  def builder[B: Encoder, F: Encoder]()(implicit
      spark: SparkSession
  ): ExportPipelineBuilder[B, F] = {
    new ExportPipelineBuilder[B, F]()
  }
}
