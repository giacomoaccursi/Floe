package com.etl.framework.pipeline

import com.etl.framework.config.{
  DomainsConfig,
  DomainsConfigLoader,
  FlowConfig,
  FlowConfigLoader,
  GlobalConfig,
  GlobalConfigLoader,
  IcebergConfig
}
import com.etl.framework.core.FlowTransformation
import com.etl.framework.exceptions.MissingConfigFieldException
import com.etl.framework.iceberg.catalog.{CatalogFactory, CatalogProvider}
import com.etl.framework.orchestration.{FlowOrchestrator, IngestionResult}
import com.etl.framework.validation.Validator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import scala.collection._

/** Fluent API builder for Ingestion pipeline (Extract, Load & Validation)
  */
class IngestionPipeline private (
    globalConfig: GlobalConfig,
    flowConfigs: Seq[FlowConfig],
    flowTransformations: Map[String, FlowTransformations],
    domainsConfig: Option[DomainsConfig],
    extraCatalogProviders: Map[String, () => CatalogProvider],
    customValidators: Map[String, () => Validator] = Map.empty,
    derivedTables: Seq[(String, DerivedTableContext => DataFrame)] = Seq.empty
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Executes Ingestion pipeline Returns IngestionResult with batch ID and flow results
    */
  def execute(): IngestionResult = {
    logger.info("Executing Ingestion pipeline")

    // Configure Spark for Iceberg
    configureSparkForIceberg(globalConfig.iceberg, extraCatalogProviders)

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
    val orchestrator = FlowOrchestrator(globalConfig, enrichedFlowConfigs, domainsConfig, customValidators.toMap)
    val result = orchestrator.execute()

    // Execute derived tables after all flows have been written to Iceberg
    if (derivedTables.nonEmpty && result.success) {
      logger.info(s"Executing ${derivedTables.size} derived tables")
      val executor = new DerivedTableExecutor(globalConfig.iceberg)
      val derivedResults = executor.execute(derivedTables, result.batchId)
      val failures = derivedResults.filterNot(_.success)
      if (failures.nonEmpty) {
        logger.warn(s"${failures.size} derived tables failed: ${failures.flatMap(_.error).mkString(", ")}")
      }
      result.copy(derivedTableResults = derivedResults)
    } else {
      result
    }
  }

  private def configureSparkForIceberg(
      config: IcebergConfig,
      extraProviders: Map[String, () => CatalogProvider]
  ): Unit = {
    CatalogFactory.createCatalogProvider(config.catalogType, extraProviders.toMap) match {
      case Right(provider) =>
        provider.validateConfig(config) match {
          case Right(_) =>
            provider.configureCatalog(spark, config)
            logger.info(
              s"Iceberg configured: catalog=${config.catalogName}, " +
                s"type=${config.catalogType}, warehouse=${config.warehouse}"
            )
          case Left(error) =>
            throw new IllegalArgumentException(
              s"Invalid Iceberg catalog config: $error"
            )
        }
      case Left(error) =>
        throw new IllegalArgumentException(error)
    }
  }

  /** Returns the global configuration
    */
  def getGlobalConfig: GlobalConfig = globalConfig

  /** Returns the flow configurations
    */
  def getFlowConfigs: Seq[FlowConfig] = flowConfigs
}

/** Builder for IngestionPipeline
  */
class IngestionPipelineBuilder(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)
  private var configDirectory: Option[String] = None
  private var globalConfigOpt: Option[GlobalConfig] = None
  private var flowConfigsOpt: Option[Seq[FlowConfig]] = None
  private var domainsConfigOpt: Option[DomainsConfig] = None
  private val flowTransformations = mutable.Map[String, FlowTransformations]()
  private val extraCatalogProviders = mutable.Map[String, () => CatalogProvider]()
  private val customValidators = mutable.Map[String, () => Validator]()
  private val derivedTables = mutable.ListBuffer[(String, DerivedTableContext => DataFrame)]()
  private var configVariables: scala.collection.immutable.Map[String, String] = scala.collection.immutable.Map.empty

  /** Sets the configuration directory path Loads global.yaml, domains.yaml, and flows/ *.yaml from this directory
    *
    * @param path
    *   Path to configuration directory
    * @return
    *   This builder for chaining
    */
  def withConfigDirectory(path: String): IngestionPipelineBuilder = {
    logger.info(s"Setting config directory: $path")
    this.configDirectory = Some(path)
    this
  }

  /** Sets the global configuration directly Alternative to withConfigDirectory for programmatic configuration
    *
    * @param config
    *   Global configuration
    * @return
    *   This builder for chaining
    */
  def withGlobalConfig(config: GlobalConfig): IngestionPipelineBuilder = {
    logger.info("Setting global config directly")
    this.globalConfigOpt = Some(config)
    this
  }

  /** Sets the domains configuration directly Alternative to withConfigDirectory for programmatic configuration
    *
    * @param config
    *   Domains configuration
    * @return
    *   This builder for chaining
    */
  def withDomainsConfig(config: DomainsConfig): IngestionPipelineBuilder = {
    logger.info(s"Setting domains config directly with ${config.domains.size} domains")
    this.domainsConfigOpt = Some(config)
    this
  }

  /** Sets the flow configurations directly Alternative to withConfigDirectory for programmatic configuration
    *
    * @param configs
    *   Flow configurations
    * @return
    *   This builder for chaining
    */
  def withFlowConfigs(configs: Seq[FlowConfig]): IngestionPipelineBuilder = {
    logger.info(s"Setting ${configs.size} flow configs directly")
    this.flowConfigsOpt = Some(configs)
    this
  }

  /** Sets variables for YAML config substitution. These take priority over environment variables.
    *
    * @param variables
    *   Map of variable name to value
    * @return
    *   This builder for chaining
    */
  def withVariables(variables: scala.collection.immutable.Map[String, String]): IngestionPipelineBuilder = {
    logger.info(s"Setting ${variables.size} config variables")
    this.configVariables = variables
    this
  }

  /** Adds a transformation for a specific flow
    *
    * @param flowName
    *   Name of the flow
    * @param preValidation
    *   Optional pre-validation transformation
    * @param postValidation
    *   Optional post-validation transformation
    * @return
    *   This builder for chaining
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

  /** Adds a pre-validation transformation for a specific flow
    *
    * @param flowName
    *   Name of the flow
    * @param transformation
    *   Pre-validation transformation function
    * @return
    *   This builder for chaining
    */
  def withPreValidationTransformation(
      flowName: String,
      transformation: FlowTransformation
  ): IngestionPipelineBuilder = {
    withFlowTransformation(flowName, preValidation = Some(transformation))
  }

  /** Adds a post-validation transformation for a specific flow
    *
    * @param flowName
    *   Name of the flow
    * @param transformation
    *   Post-validation transformation function
    * @return
    *   This builder for chaining
    */
  def withPostValidationTransformation(
      flowName: String,
      transformation: FlowTransformation
  ): IngestionPipelineBuilder = {
    withFlowTransformation(flowName, postValidation = Some(transformation))
  }

  /** Registers a custom catalog provider for the given catalog type.
    *
    * Use this when you need a catalog not built into the framework (e.g. a proprietary metastore or a
    * community-contributed Iceberg catalog). Custom providers override built-in ones if the same type key is used.
    *
    * @param catalogType
    *   Identifier used in `iceberg.catalog-type` (e.g. "custom")
    * @param provider
    *   Factory function returning the CatalogProvider instance
    * @return
    *   This builder for chaining
    */
  def withCatalogProvider(
      catalogType: String,
      provider: () => CatalogProvider
  ): IngestionPipelineBuilder = {
    extraCatalogProviders(catalogType) = provider
    this
  }

  /** Registers a custom validator by name.
    * Use the same name in the flow YAML `class` field to reference it.
    *
    * @param name Short name for the validator (e.g. "luhn")
    * @param factory Factory function that creates the validator instance
    * @return This builder for chaining
    */
  def withCustomValidator(
      name: String,
      factory: () => Validator
  ): IngestionPipelineBuilder = {
    customValidators(name) = factory
    this
  }

  /** Registers a derived table that will be computed after all flows are written to Iceberg.
    * The function receives a DerivedTableContext with access to Iceberg tables (full history).
    * The result is written to Iceberg as a full-load table.
    *
    * @param tableName Name of the derived table (becomes the Iceberg table name)
    * @param fn Function that produces the derived DataFrame
    * @return This builder for chaining
    */
  def withDerivedTable(
      tableName: String,
      fn: DerivedTableContext => DataFrame
  ): IngestionPipelineBuilder = {
    if (derivedTables.exists(_._1 == tableName))
      throw new IllegalArgumentException(s"Derived table '$tableName' is already registered")
    logger.info(s"Registering derived table: $tableName")
    derivedTables += ((tableName, fn))
    this
  }

  /** Builds the IngestionPipeline
    *
    * @return
    *   Configured IngestionPipeline
    * @throws IllegalStateException
    *   if required configuration is missing
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
    IngestionPipeline.create(
      globalConfig,
      flowConfigs,
      flowTransformations.toMap,
      domainsConfigOpt,
      extraCatalogProviders.toMap,
      customValidators.toMap,
      derivedTables.toSeq
    )
  }

  /** Loads all configurations from directory
    */
  private def loadConfigurationsFromDirectory(directory: String): (GlobalConfig, Seq[FlowConfig]) = {
    logger.info(s"Loading configurations from directory: $directory")

    val globalConfigLoader = new GlobalConfigLoader()
    val domainsConfigLoader = new DomainsConfigLoader()
    val flowConfigLoader = new FlowConfigLoader()

    val globalConfig = globalConfigLoader.load(s"$directory/global.yaml", configVariables) match {
      case Right(config) => config
      case Left(error)   => throw error
    }

    val domainsConfig = domainsConfigLoader.load(s"$directory/domains.yaml", configVariables) match {
      case Right(config) =>
        logger.info(s"Loaded ${config.domains.size} domains from domains.yaml")
        config
      case Left(_) =>
        logger.info("No domains.yaml found, using empty domains")
        DomainsConfig(Map.empty)
    }

    val flowConfigs = flowConfigLoader.loadAll(s"$directory/flows", configVariables) match {
      case Right(configs) => configs
      case Left(error)    => throw error
    }

    // Store domainsConfig in builder for later use
    this.domainsConfigOpt = Some(domainsConfig)

    (globalConfig, flowConfigs)
  }

  /** Loads global config from directory
    */
  private def loadGlobalConfigFromDirectory(directory: String): GlobalConfig = {
    logger.info(s"Loading global config from directory: $directory")
    val loader = new GlobalConfigLoader()
    loader.load(s"$directory/global.yaml", configVariables) match {
      case Right(config) => config
      case Left(error)   => throw error
    }
  }

  /** Loads flow configs from directory
    */
  private def loadFlowConfigsFromDirectory(directory: String): Seq[FlowConfig] = {
    logger.info(s"Loading flow configs from directory: $directory")
    val domainsLoader = new DomainsConfigLoader()
    val flowLoader = new FlowConfigLoader()

    val domainsConfig = domainsLoader.load(s"$directory/domains.yaml", configVariables) match {
      case Right(config) =>
        logger.info(s"Loaded ${config.domains.size} domains from domains.yaml")
        config
      case Left(_) =>
        logger.info("No domains.yaml found, using empty domains")
        DomainsConfig(Map.empty)
    }

    // Store domainsConfig in builder for later use
    this.domainsConfigOpt = Some(domainsConfig)

    flowLoader.loadAll(s"$directory/flows", configVariables) match {
      case Right(configs) => configs
      case Left(error)    => throw error
    }
  }
}

/** Companion object for IngestionPipeline
  */
object IngestionPipeline {

  /** Internal factory method to create IngestionPipeline instances
    */
  private[pipeline] def create(
      globalConfig: GlobalConfig,
      flowConfigs: Seq[FlowConfig],
      flowTransformations: Map[String, FlowTransformations],
      domainsConfig: Option[DomainsConfig],
      extraCatalogProviders: Map[String, () => CatalogProvider],
      customValidators: Map[String, () => Validator] = Map.empty,
      derivedTables: Seq[(String, DerivedTableContext => DataFrame)] = Seq.empty
  )(implicit spark: SparkSession): IngestionPipeline = {
    new IngestionPipeline(
      globalConfig,
      flowConfigs,
      flowTransformations,
      domainsConfig,
      extraCatalogProviders,
      customValidators,
      derivedTables
    )
  }

  /** Creates a new IngestionPipeline builder
    *
    * @param spark
    *   Implicit SparkSession
    * @return
    *   New IngestionPipelineBuilder
    */
  def builder()(implicit spark: SparkSession): IngestionPipelineBuilder = {
    new IngestionPipelineBuilder()
  }
}

/** Container for flow transformations
  */
private case class FlowTransformations(
    preValidation: Option[FlowTransformation],
    postValidation: Option[FlowTransformation]
)
