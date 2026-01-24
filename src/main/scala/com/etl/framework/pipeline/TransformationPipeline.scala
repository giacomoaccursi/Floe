package com.etl.framework.pipeline

import com.etl.framework.aggregation.DAGOrchestrator
import com.etl.framework.config.{AggregationConfig, DAGConfigLoader, GlobalConfig, GlobalConfigLoader}
import com.etl.framework.io.writers.BatchModelWriter
import com.etl.framework.mapping.{BatchModelEnricher, BatchModelMapper, MappingConfig}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Fluent API builder for Transformation (Aggregation) pipeline
 */
class TransformationPipeline[T: Encoder] private(
  dagConfig: AggregationConfig,
  globalConfig: GlobalConfig,
  mappingConfig: Option[MappingConfig],
  enrichmentFunctions: Seq[BatchModelEnricher.EnrichmentFunction[T]],
  autoDiscoverAdditionalTables: Boolean,
  outputPath: Option[String]
)(implicit spark: SparkSession) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Executes Transformation pipeline
   * Returns Dataset[T] (Batch Model)
   */
  def execute(): Dataset[T] = {
    logger.info("Executing Transformation pipeline")
    
    // 1. Execute DAG aggregation
    logger.info("Step 1: Executing DAG aggregation")
    val orchestrator = new DAGOrchestrator(dagConfig, globalConfig, autoDiscoverAdditionalTables)
    val aggregatedDF = orchestrator.execute()
    
    // 2. Map DataFrame to Dataset[BatchModel]
    logger.info("Step 2: Mapping DataFrame to Batch Model")
    val batchModel = mappingConfig match {
      case Some(config) =>
        val mapper = new BatchModelMapper[T](config)
        mapper.map(aggregatedDF)
      case None =>
        // No mapping config, try direct conversion
        logger.info("No mapping config provided, attempting direct conversion")
        aggregatedDF.as[T]
    }
    
    // 3. Apply enrichment functions
    val enrichedModel = if (enrichmentFunctions.nonEmpty) {
      logger.info(s"Step 3: Applying ${enrichmentFunctions.size} enrichment functions")
      val enricher = new BatchModelEnricher[T]()
      enricher.enrichMultiple(batchModel, enrichmentFunctions)
    } else {
      logger.info("Step 3: No enrichment functions configured, skipping")
      batchModel
    }
    
    // 4. Write Batch Model if output path is configured
    outputPath.foreach { path =>
      logger.info(s"Step 4: Writing Batch Model to $path")
      val outputConfig = dagConfig.output.batch
      val writer = BatchModelWriter[T](outputConfig)
      writer.write(enrichedModel, path)
    }
    
    logger.info("Transformation pipeline execution completed")
    enrichedModel
  }
  
  /**
   * Returns the DAG configuration
   */
  def getDAGConfig: AggregationConfig = dagConfig
  
  /**
   * Returns the global configuration
   */
  def getGlobalConfig: GlobalConfig = globalConfig
}

/**
 * Builder for TransformationPipeline
 */
class TransformationPipelineBuilder[T: Encoder](implicit spark: SparkSession) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  private var dagConfigOpt: Option[AggregationConfig] = None
  private var globalConfigOpt: Option[GlobalConfig] = None
  private var mappingConfigOpt: Option[MappingConfig] = None
  private var mappingFileOpt: Option[String] = None
  private var enrichmentFunctions: Seq[BatchModelEnricher.EnrichmentFunction[T]] = Seq.empty
  private var autoDiscoverAdditionalTables: Boolean = false
  private var outputPathOpt: Option[String] = None
  
  /**
   * Sets the DAG configuration
   * 
   * @param config DAG configuration
   * @return This builder for chaining
   */
  def withDAGConfig(config: AggregationConfig): TransformationPipelineBuilder[T] = {
    logger.info("Setting DAG configuration")
    this.dagConfigOpt = Some(config)
    this
  }
  
  /**
   * Loads DAG configuration from YAML file
   * 
   * @param path Path to DAG configuration file
   * @return This builder for chaining
   */
  def withDAGConfigFile(path: String): TransformationPipelineBuilder[T] = {
    logger.info(s"Loading DAG configuration from: $path")
    val loader = new DAGConfigLoader()
    this.dagConfigOpt = Some(loader.load(path) match {
      case Right(config) => config
      case Left(error) => throw error
    })
    this
  }
  
  /**
   * Sets the global configuration
   * 
   * @param config Global configuration
   * @return This builder for chaining
   */
  def withGlobalConfig(config: GlobalConfig): TransformationPipelineBuilder[T] = {
    logger.info("Setting global configuration")
    this.globalConfigOpt = Some(config)
    this
  }
  
  /**
   * Loads global configuration from YAML file
   * 
   * @param path Path to global configuration file
   * @return This builder for chaining
   */
  def withGlobalConfigFile(path: String): TransformationPipelineBuilder[T] = {
    logger.info(s"Loading global configuration from: $path")
    val loader = new GlobalConfigLoader()
    this.globalConfigOpt = Some(loader.load(path) match {
      case Right(config) => config
      case Left(error) => throw error
    })
    this
  }
  
  /**
   * Specifies the Batch Model type (for type safety)
   * This is a no-op method that helps with type inference
   * 
   * @return This builder for chaining
   */
  def withBatchModel[B: Encoder](): TransformationPipelineBuilder[B] = {
    logger.info(s"Setting Batch Model type")
    // Create a new builder with the correct type
    val newBuilder = new TransformationPipelineBuilder[B]()
    newBuilder.dagConfigOpt = this.dagConfigOpt
    newBuilder.globalConfigOpt = this.globalConfigOpt
    newBuilder.mappingConfigOpt = this.mappingConfigOpt
    newBuilder.mappingFileOpt = this.mappingFileOpt
    newBuilder.autoDiscoverAdditionalTables = this.autoDiscoverAdditionalTables
    newBuilder.outputPathOpt = this.outputPathOpt
    newBuilder
  }
  
  /**
   * Sets the mapping configuration for DataFrame to Dataset conversion
   * 
   * @param config Mapping configuration
   * @return This builder for chaining
   */
  def withMappingConfig(config: MappingConfig): TransformationPipelineBuilder[T] = {
    logger.info("Setting mapping configuration")
    this.mappingConfigOpt = Some(config)
    this
  }
  
  /**
   * Loads mapping configuration from YAML file
   * 
   * @param path Path to mapping configuration file
   * @return This builder for chaining
   */
  def withMappingFile(path: String): TransformationPipelineBuilder[T] = {
    logger.info(s"Setting mapping file: $path")
    this.mappingFileOpt = Some(path)
    this
  }
  
  /**
   * Adds an enrichment function to be applied to the Batch Model
   * Multiple enrichment functions can be added and will be applied in order
   * 
   * @param enrichmentFn Enrichment function
   * @return This builder for chaining
   */
  def withEnrichment(enrichmentFn: BatchModelEnricher.EnrichmentFunction[T]): TransformationPipelineBuilder[T] = {
    logger.info("Adding enrichment function")
    this.enrichmentFunctions = this.enrichmentFunctions :+ enrichmentFn
    this
  }
  
  /**
   * Adds multiple enrichment functions to be applied to the Batch Model
   * 
   * @param enrichmentFns Sequence of enrichment functions
   * @return This builder for chaining
   */
  def withEnrichments(enrichmentFns: Seq[BatchModelEnricher.EnrichmentFunction[T]]): TransformationPipelineBuilder[T] = {
    logger.info(s"Adding ${enrichmentFns.size} enrichment functions")
    this.enrichmentFunctions = this.enrichmentFunctions ++ enrichmentFns
    this
  }
  
  /**
   * Enables or disables auto-discovery of additional tables from Ingestion
   * When enabled, additional tables created in Ingestion with dagMetadata
   * will be automatically discovered and integrated into the DAG
   * 
   * @param enabled Whether to enable auto-discovery (default: true)
   * @return This builder for chaining
   */
  def withAutoDiscoverAdditionalTables(enabled: Boolean = true): TransformationPipelineBuilder[T] = {
    logger.info(s"Setting auto-discover additional tables: $enabled")
    this.autoDiscoverAdditionalTables = enabled
    this
  }
  
  /**
   * Sets the output path for writing the Batch Model
   * If not set, the Batch Model will not be written to disk
   * 
   * @param path Output path
   * @return This builder for chaining
   */
  def withOutputPath(path: String): TransformationPipelineBuilder[T] = {
    logger.info(s"Setting output path: $path")
    this.outputPathOpt = Some(path)
    this
  }
  
  /**
   * Builds the TransformationPipeline
   * 
   * @return Configured TransformationPipeline
   * @throws IllegalStateException if required configuration is missing
   */
  def build(): TransformationPipeline[T] = {
    logger.info("Building TransformationPipeline")
    
    // Validate required configurations
    val dagConfig = dagConfigOpt.getOrElse {
      throw new IllegalStateException("DAG configuration is required. Use withDAGConfig() or withDAGConfigFile()")
    }
    
    val globalConfig = globalConfigOpt.getOrElse {
      throw new IllegalStateException("Global configuration is required. Use withGlobalConfig() or withGlobalConfigFile()")
    }
    
    // Load mapping config if file was specified
    val mappingConfig = (mappingConfigOpt, mappingFileOpt) match {
      case (Some(config), _) =>
        Some(config)
      case (None, Some(file)) =>
        Some(BatchModelMapper.loadMappingConfig(file))
      case (None, None) =>
        logger.info("No mapping configuration provided, will attempt direct DataFrame to Dataset conversion")
        None
    }
    
    logger.info(s"TransformationPipeline built with ${enrichmentFunctions.size} enrichment functions")
    TransformationPipeline.create[T](
      dagConfig,
      globalConfig,
      mappingConfig,
      enrichmentFunctions.toSeq,
      autoDiscoverAdditionalTables,
      outputPathOpt
    )
  }
}

/**
 * Companion object for TransformationPipeline
 */
object TransformationPipeline {
  
  /**
   * Internal factory method to create TransformationPipeline instances
   */
  private[pipeline] def create[T: Encoder](
    dagConfig: AggregationConfig,
    globalConfig: GlobalConfig,
    mappingConfig: Option[MappingConfig],
    enrichmentFunctions: Seq[BatchModelEnricher.EnrichmentFunction[T]],
    autoDiscoverAdditionalTables: Boolean,
    outputPath: Option[String]
  )(implicit spark: SparkSession): TransformationPipeline[T] = {
    new TransformationPipeline[T](
      dagConfig,
      globalConfig,
      mappingConfig,
      enrichmentFunctions,
      autoDiscoverAdditionalTables,
      outputPath
    )
  }
  
  /**
   * Creates a new TransformationPipeline builder
   * 
   * @tparam T Batch Model type
   * @param spark Implicit SparkSession
   * @return New TransformationPipelineBuilder
   */
  def builder[T: Encoder]()(implicit spark: SparkSession): TransformationPipelineBuilder[T] = {
    new TransformationPipelineBuilder[T]()
  }
}
