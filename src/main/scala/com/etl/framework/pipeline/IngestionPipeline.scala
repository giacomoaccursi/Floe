package com.etl.framework.pipeline

import com.etl.framework.config.{DomainsConfigLoader, FlowConfig, FlowConfigLoader, GlobalConfig, GlobalConfigLoader}
import com.etl.framework.core.FlowTransformation
import com.etl.framework.exceptions.MissingConfigFieldException
import com.etl.framework.orchestration.{FlowOrchestrator, IngestionResult}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Fluent API builder for Ingestion pipeline (Extract, Load & Validation)
 */
class IngestionPipeline private(
                                 globalConfig: GlobalConfig,
                                 flowConfigs: Seq[FlowConfig],
                                 flowTransformations: Map[String, FlowTransformations],
                                 domainsConfig: Option[com.etl.framework.config.DomainsConfig]
                               )(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Executes Ingestion pipeline
   * Returns IngestionResult with batch ID and flow results
   */
  def execute(): IngestionResult = {
    logger.info("Executing Ingestion pipeline")

    // Apply transformations to flow configs
    val enrichedFlowConfigs = flowConfigs.map { flowConfig =>
      flowTransformations.get(flowConfig.name) match {
        case Some(transformations) =>
          flowConfig.copy(
            preValidationTransformation = transformations.preValidation,
            postValidationTransformation = transformations.postValidation
          )
        case None =>
          flowConfig
      }
    }

    // Create and execute orchestrator with DomainsConfig
    val orchestrator = FlowOrchestrator(globalConfig, enrichedFlowConfigs, domainsConfig)
    orchestrator.execute()
  }

  /**
   * Returns the global configuration
   */
  def getGlobalConfig: GlobalConfig = globalConfig

  /**
   * Returns the flow configurations
   */
  def getFlowConfigs: Seq[FlowConfig] = flowConfigs
}

/**
 * Builder for IngestionPipeline
 */
class IngestionPipelineBuilder(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)
  private var configDirectory: Option[String] = None
  private var globalConfigOpt: Option[GlobalConfig] = None
  private var flowConfigsOpt: Option[Seq[FlowConfig]] = None
  private var domainsConfigOpt: Option[com.etl.framework.config.DomainsConfig] = None
  private val flowTransformations = scala.collection.mutable.Map[String, FlowTransformations]()

  /**
   * Sets the configuration directory path
   * Loads global.yaml, domains.yaml, and flows/ *.yaml from this directory
   *
   * @param path Path to configuration directory
   * @return This builder for chaining
   */
  def withConfigDirectory(path: String): IngestionPipelineBuilder = {
    logger.info(s"Setting config directory: $path")
    this.configDirectory = Some(path)
    this
  }

  /**
   * Sets the global configuration directly
   * Alternative to withConfigDirectory for programmatic configuration
   *
   * @param config Global configuration
   * @return This builder for chaining
   */
  def withGlobalConfig(config: GlobalConfig): IngestionPipelineBuilder = {
    logger.info("Setting global config directly")
    this.globalConfigOpt = Some(config)
    this
  }

  /**
   * Sets the domains configuration directly
   * Alternative to withConfigDirectory for programmatic configuration
   *
   * @param config Domains configuration
   * @return This builder for chaining
   */
  def withDomainsConfig(config: com.etl.framework.config.DomainsConfig): IngestionPipelineBuilder = {
    logger.info(s"Setting domains config directly with ${config.domains.size} domains")
    this.domainsConfigOpt = Some(config)
    this
  }

  /**
   * Sets the flow configurations directly
   * Alternative to withConfigDirectory for programmatic configuration
   *
   * @param configs Flow configurations
   * @return This builder for chaining
   */
  def withFlowConfigs(configs: Seq[FlowConfig]): IngestionPipelineBuilder = {
    logger.info(s"Setting ${configs.size} flow configs directly")
    this.flowConfigsOpt = Some(configs)
    this
  }

  /**
   * Adds a transformation for a specific flow
   *
   * @param flowName Name of the flow
   * @param preValidation Optional pre-validation transformation
   * @param postValidation Optional post-validation transformation
   * @return This builder for chaining
   */
  def withFlowTransformation(
                              flowName: String,
                              preValidation: Option[FlowTransformation] = None,
                              postValidation: Option[FlowTransformation] = None
                            ): IngestionPipelineBuilder = {
    logger.info(s"Adding transformation for flow: $flowName")

    val existing = flowTransformations.getOrElse(flowName, FlowTransformations(None, None))
    flowTransformations(flowName) = FlowTransformations(
      preValidation = preValidation.orElse(existing.preValidation),
      postValidation = postValidation.orElse(existing.postValidation)
    )

    this
  }

  /**
   * Adds a pre-validation transformation for a specific flow
   *
   * @param flowName Name of the flow
   * @param transformation Pre-validation transformation function
   * @return This builder for chaining
   */
  def withPreValidationTransformation(
                                       flowName: String,
                                       transformation: FlowTransformation
                                     ): IngestionPipelineBuilder = {
    withFlowTransformation(flowName, preValidation = Some(transformation))
  }

  /**
   * Adds a post-validation transformation for a specific flow
   *
   * @param flowName Name of the flow
   * @param transformation Post-validation transformation function
   * @return This builder for chaining
   */
  def withPostValidationTransformation(
                                        flowName: String,
                                        transformation: FlowTransformation
                                      ): IngestionPipelineBuilder = {
    withFlowTransformation(flowName, postValidation = Some(transformation))
  }

  /**
   * Builds the IngestionPipeline
   *
   * @return Configured IngestionPipeline
   * @throws IllegalStateException if required configuration is missing
   */
  def build(): IngestionPipeline = {
    logger.info("Building IngestionPipeline")

    // Load configurations if directory was specified
    val (globalConfig, flowConfigs) = (globalConfigOpt, flowConfigsOpt) match {
      case (Some(gc), Some(fc)) =>
        // Both provided directly
        (gc, fc)

      case (None, None) if configDirectory.isDefined =>
        // Load from directory
        loadConfigurationsFromDirectory(configDirectory.get)

      case (Some(gc), None) if configDirectory.isDefined =>
        // Global config provided, load flows from directory
        val flows = loadFlowConfigsFromDirectory(configDirectory.get)
        (gc, flows)

      case (None, Some(fc)) if configDirectory.isDefined =>
        // Flow configs provided, load global from directory
        val global = loadGlobalConfigFromDirectory(configDirectory.get)
        (global, fc)

      case _ =>
        throw MissingConfigFieldException(
          file = "pipeline-builder",
          field = "configDirectory or (globalConfig + flowConfigs)",
          section = "pipeline configuration"
        )
    }

    logger.info(s"IngestionPipeline built with ${flowConfigs.size} flows")
    IngestionPipeline.create(globalConfig, flowConfigs, flowTransformations.toMap, domainsConfigOpt)
  }

  /**
   * Loads all configurations from directory
   */
  private def loadConfigurationsFromDirectory(directory: String): (GlobalConfig, Seq[FlowConfig]) = {
    logger.info(s"Loading configurations from directory: $directory")

    val globalConfigLoader = new GlobalConfigLoader()
    val domainsConfigLoader = new DomainsConfigLoader()
    val flowConfigLoader = new FlowConfigLoader()

    val globalConfig = globalConfigLoader.load(s"$directory/global.yaml") match {
      case Right(config) => config
      case Left(error) => throw error
    }

    val domainsConfig = domainsConfigLoader.load(s"$directory/domains.yaml") match {
      case Right(config) => 
        logger.info(s"Loaded ${config.domains.size} domains from domains.yaml")
        config
      case Left(error) => 
        logger.warn(s"Could not load domains.yaml: ${error.getMessage}")
        throw error
    }

    val flowConfigs = flowConfigLoader.loadAll(s"$directory/flows") match {
      case Right(configs) => configs
      case Left(error) => throw error
    }

    // Store domainsConfig in builder for later use
    this.domainsConfigOpt = Some(domainsConfig)

    (globalConfig, flowConfigs)
  }

  /**
   * Loads global config from directory
   */
  private def loadGlobalConfigFromDirectory(directory: String): GlobalConfig = {
    logger.info(s"Loading global config from directory: $directory")
    val loader = new com.etl.framework.config.GlobalConfigLoader()
    loader.load(s"$directory/global.yaml") match {
      case Right(config) => config
      case Left(error) => throw error
    }
  }

  /**
   * Loads flow configs from directory
   */
  private def loadFlowConfigsFromDirectory(directory: String): Seq[FlowConfig] = {
    logger.info(s"Loading flow configs from directory: $directory")
    val domainsLoader = new com.etl.framework.config.DomainsConfigLoader()
    val flowLoader = new com.etl.framework.config.FlowConfigLoader()

    val domainsConfig = domainsLoader.load(s"$directory/domains.yaml") match {
      case Right(config) => 
        logger.info(s"Loaded ${config.domains.size} domains from domains.yaml")
        config
      case Left(error) => 
        logger.warn(s"Could not load domains.yaml: ${error.getMessage}")
        throw error
    }

    // Store domainsConfig in builder for later use
    this.domainsConfigOpt = Some(domainsConfig)

    flowLoader.loadAll(s"$directory/flows") match {
      case Right(configs) => configs
      case Left(error) => throw error
    }
  }
}

/**
 * Companion object for IngestionPipeline
 */
object IngestionPipeline {

  /**
   * Internal factory method to create IngestionPipeline instances
   */
  private[pipeline] def create(
                                globalConfig: GlobalConfig,
                                flowConfigs: Seq[FlowConfig],
                                flowTransformations: Map[String, FlowTransformations],
                                domainsConfig: Option[com.etl.framework.config.DomainsConfig]
                              )(implicit spark: SparkSession): IngestionPipeline = {
    new IngestionPipeline(globalConfig, flowConfigs, flowTransformations, domainsConfig)
  }

  /**
   * Creates a new IngestionPipeline builder
   *
   * @param spark Implicit SparkSession
   * @return New IngestionPipelineBuilder
   */
  def builder()(implicit spark: SparkSession): IngestionPipelineBuilder = {
    new IngestionPipelineBuilder()
  }
}

/**
 * Container for flow transformations
 */
private case class FlowTransformations(
                                        preValidation: Option[FlowTransformation],
                                        postValidation: Option[FlowTransformation]
                                      )
