package com.etl.framework.pipeline

import com.etl.framework.config.{
  DomainsConfig,
  DomainsConfigLoader,
  FlowConfig,
  FlowConfigLoader,
  FlowConfigValidator,
  GlobalConfig,
  GlobalConfigLoader,
  IcebergConfig
}
import com.etl.framework.exceptions.{BatchFailedException, ConfigFileException, MissingConfigFieldException}
import com.etl.framework.iceberg.catalog.{CatalogFactory, CatalogProvider}
import com.etl.framework.io.readers.DataReaderFactory
import com.etl.framework.orchestration.{FlowOrchestrator, IngestionResult, BatchListener}
import com.etl.framework.orchestration.state.{InMemoryRunStore, RunStore}
import com.etl.framework.validation.Validator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import scala.collection._
import java.util.Locale
import java.io.FileNotFoundException
import java.nio.file.NoSuchFileException

/** Fluent API builder for Ingestion pipeline (Extract, Load & Validation)
  */
class IngestionPipeline private (
    globalConfig: GlobalConfig,
    flowConfigs: Seq[FlowConfig],
    flowTransformations: Map[String, FlowTransformations],
    domainsConfig: Option[DomainsConfig],
    extraCatalogProviders: Map[String, () => CatalogProvider],
    customValidators: Map[String, () => Validator],
    derivedTables: Seq[(String, DerivedTableContext => DataFrame)],
    batchListeners: Seq[BatchListener],
    pipelineVersion: String,
    runStore: RunStore,
    customReaders: Map[String, DataReaderFactory.ReaderFactory]
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Executes Ingestion pipeline Returns IngestionResult with batch ID and flow results
    */
  def execute(): IngestionResult = {
    logger.info("Executing Ingestion pipeline")
    createOrchestrator().execute()
  }

  /** Resumes a previously failed batch after reconciling every Iceberg commit. The original effective timestamp and
    * input fingerprints are preserved; already committed targets are read back and never written twice.
    */
  def resume(batchId: String): IngestionResult = {
    logger.info(s"Resuming Ingestion batch $batchId")
    createOrchestrator().resume(batchId)
  }

  /** Reprocesses the immutable inputs of a terminal batch under a new batch ID. */
  def replay(batchId: String): IngestionResult = {
    logger.info(s"Replaying Ingestion batch $batchId")
    createOrchestrator().replay(batchId)
  }

  /** Executes the pipeline and throws BatchFailedException when synchronous batch publication fails. Use this on
    * managed platforms (Glue, EMR) where a failed pipeline should stop the job.
    */
  def executeOrThrow(): IngestionResult = {
    val result = execute()
    if (!result.success)
      throw BatchFailedException(result.batchId, result.error.getOrElse("unknown error"))
    result
  }

  private def createOrchestrator(): FlowOrchestrator = {
    configureSparkForIceberg(globalConfig.iceberg, extraCatalogProviders)
    val enrichedFlowConfigs = flowConfigs.map { flowConfig =>
      flowTransformations.get(flowConfig.name) match {
        case Some(transformations) =>
          flowConfig.copy(
            preValidationTransformation = transformations.preValidation,
            postValidationTransformation = transformations.postValidation
          )
        case None => flowConfig
      }
    }
    FlowOrchestrator(
      globalConfig,
      enrichedFlowConfigs,
      domainsConfig,
      customValidators.toMap,
      batchListeners,
      customReaders.toMap,
      derivedTables,
      pipelineVersion = pipelineVersion,
      runStore = runStore
    )
  }

  /** Configures the SparkSession with Iceberg catalog settings. Resolves the catalog provider (hadoop, glue, or custom)
    * and applies catalog properties.
    */
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
  private val batchListeners = mutable.ListBuffer[BatchListener]()
  private val customReaders = mutable.Map[String, DataReaderFactory.ReaderFactory]()
  private var pipelineVersion: String = "unversioned"
  private var runStore: RunStore = new InMemoryRunStore()
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

  /** Registers a custom validator by name. Use the same name in the flow YAML `class` field to reference it.
    *
    * @param name
    *   Short name for the validator (e.g. "luhn")
    * @param factory
    *   Factory function that creates the validator instance
    * @return
    *   This builder for chaining
    */
  def withCustomValidator(
      name: String,
      factory: () => Validator
  ): IngestionPipelineBuilder = {
    customValidators(name) = factory
    this
  }

  /** Registers a derived table that will be computed after all flows are written to Iceberg. The function receives a
    * DerivedTableContext with access to the current state of Iceberg tables. The result is written to Iceberg as a
    * full-load table.
    *
    * @param tableName
    *   Name of the derived table (becomes the Iceberg table name)
    * @param fn
    *   Function that produces the derived DataFrame
    * @return
    *   This builder for chaining
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

  /** Registers a BatchListener that receives notifications on batch completion or failure. */
  def withBatchListener(listener: BatchListener): IngestionPipelineBuilder = {
    batchListeners += listener
    this
  }

  /** Sets the durable coordinator used for run state, CAS transitions, leases, and recovery. */
  def withRunStore(store: RunStore): IngestionPipelineBuilder = {
    this.runStore = store
    this
  }

  /** Sets the deployment/config revision used to reject recovery across code changes that cannot be serialized, such as
    * transformation and derived-table functions.
    */
  def withPipelineVersion(version: String): IngestionPipelineBuilder = {
    require(version.trim.nonEmpty, "pipelineVersion must not be blank")
    this.pipelineVersion = version.trim
    this
  }

  /** Registers a custom DataReader factory for a given source type name. Use this to read from sources not supported by
    * the built-in readers (file, jdbc).
    */
  def withDataReader(
      typeName: String,
      factory: DataReaderFactory.ReaderFactory
  ): IngestionPipelineBuilder = {
    customReaders(typeName) = factory
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
    val (globalConfig, flowConfigs) = loadConfigs()
    val configErrors = flowConfigs.flatMap { config =>
      FlowConfigValidator.validate(config).map(error => s"Flow '${config.name}': $error")
    }
    require(configErrors.isEmpty, s"Invalid flow configuration: ${configErrors.mkString("; ")}")
    val collisions = derivedTableCollisions(globalConfig, flowConfigs)
    require(
      collisions.isEmpty,
      s"Derived table names collide with managed tables: ${collisions.mkString(", ")}"
    )

    logger.info(s"IngestionPipeline built with ${flowConfigs.size} flows")
    IngestionPipeline.create(
      globalConfig,
      flowConfigs,
      flowTransformations.toMap,
      domainsConfigOpt,
      extraCatalogProviders.toMap,
      customValidators.toMap,
      derivedTables.toSeq,
      batchListeners.toSeq,
      pipelineVersion,
      runStore,
      customReaders.toMap
    )
  }

  /** Validates pipeline configuration without reading source data or executing Spark jobs. Checks FK references,
    * dependency cycles, and config loading. Returns a list of error messages (empty if valid).
    */
  def validate(): Seq[String] = {
    val errors = scala.collection.mutable.ArrayBuffer[String]()

    val configs = scala.util.Try(loadConfigs())
    configs match {
      case scala.util.Failure(e) =>
        errors += s"Configuration loading failed: ${e.getMessage}"
        return errors.toSeq
      case _ =>
    }

    val (globalConfig, flowConfigs) = configs.get
    derivedTableCollisions(globalConfig, flowConfigs).foreach { name =>
      errors += s"Derived table '$name' collides with a managed table"
    }
    val flowNames = flowConfigs.map(_.name).toSet

    flowConfigs.foreach { config =>
      FlowConfigValidator.validate(config).foreach(error => errors += s"Flow '${config.name}': $error")
    }

    // Check FK references point to existing flows
    flowConfigs.foreach { fc =>
      fc.validation.foreignKeys.foreach { fk =>
        if (!flowNames.contains(fk.references.flow))
          errors += s"Flow '${fc.name}': FK references unknown flow '${fk.references.flow}'"
      }
      fc.dependsOn.foreach { dep =>
        if (!flowNames.contains(dep))
          errors += s"Flow '${fc.name}': dependsOn references unknown flow '$dep'"
      }
    }

    // Check for dependency cycles
    scala.util.Try {
      val graph = com.etl.framework.util.TopologicalSorter.buildGraph[FlowConfig](
        flowConfigs,
        _.name,
        fc => fc.validation.foreignKeys.map(_.references.flow).toSet ++ fc.dependsOn.toSet
      )
      com.etl.framework.util.TopologicalSorter.sort(graph, "flow dependency graph")
    } match {
      case scala.util.Failure(e) => errors += e.getMessage
      case _                     =>
    }

    errors.toSeq
  }

  private def derivedTableCollisions(globalConfig: GlobalConfig, flowConfigs: Seq[FlowConfig]): Seq[String] = {
    val managedNames =
      (flowConfigs.map(_.name) ++ globalConfig.processing.qualityMetricsTable.toSeq)
        .map(_.toLowerCase(Locale.ROOT))
        .toSet
    derivedTables.map(_._1).filter(name => managedNames.contains(name.toLowerCase(Locale.ROOT))).toSeq
  }

  private def loadConfigs(): (GlobalConfig, Seq[FlowConfig]) = {
    (globalConfigOpt, flowConfigsOpt) match {
      case (Some(gc), Some(fc)) =>
        (gc, fc)

      case (None, None) if configDirectory.isDefined =>
        loadConfigurationsFromDirectory(configDirectory.get)

      case (Some(gc), None) if configDirectory.isDefined =>
        val flows = loadFlowConfigsFromDirectory(configDirectory.get)
        (gc, flows)

      case (None, Some(fc)) if configDirectory.isDefined =>
        val global = loadGlobalConfigFromDirectory(configDirectory.get)
        (global, fc)

      case _ =>
        throw MissingConfigFieldException(
          file = "pipeline-builder",
          field = "configDirectory or (globalConfig + flowConfigs)",
          section = "pipeline configuration"
        )
    }
  }

  /** Loads all configurations from directory
    */
  private def loadConfigurationsFromDirectory(directory: String): (GlobalConfig, Seq[FlowConfig]) = {
    logger.info(s"Loading configurations from directory: $directory")

    val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
    val globalConfigLoader = new GlobalConfigLoader(hadoopConfiguration)
    val domainsConfigLoader = new DomainsConfigLoader(hadoopConfiguration)
    val flowConfigLoader = new FlowConfigLoader(hadoopConfiguration)

    val globalConfig = globalConfigLoader.load(s"$directory/global.yaml", configVariables) match {
      case Right(config) => config
      case Left(error)   => throw error
    }

    val domainsConfig = domainsConfigLoader.load(s"$directory/domains.yaml", configVariables) match {
      case Right(config) =>
        logger.info(s"Loaded ${config.domains.size} domains from domains.yaml")
        config
      case Left(error) if isMissingConfigFile(error) =>
        logger.info("No domains.yaml found, using empty domains")
        DomainsConfig(Map.empty)
      case Left(error) => throw error
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
    val loader = new GlobalConfigLoader(spark.sparkContext.hadoopConfiguration)
    loader.load(s"$directory/global.yaml", configVariables) match {
      case Right(config) => config
      case Left(error)   => throw error
    }
  }

  /** Loads flow configs from directory
    */
  private def loadFlowConfigsFromDirectory(directory: String): Seq[FlowConfig] = {
    logger.info(s"Loading flow configs from directory: $directory")
    val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
    val domainsLoader = new DomainsConfigLoader(hadoopConfiguration)
    val flowLoader = new FlowConfigLoader(hadoopConfiguration)

    val domainsConfig = domainsLoader.load(s"$directory/domains.yaml", configVariables) match {
      case Right(config) =>
        logger.info(s"Loaded ${config.domains.size} domains from domains.yaml")
        config
      case Left(error) if isMissingConfigFile(error) =>
        logger.info("No domains.yaml found, using empty domains")
        DomainsConfig(Map.empty)
      case Left(error) => throw error
    }

    // Store domainsConfig in builder for later use
    this.domainsConfigOpt = Some(domainsConfig)

    flowLoader.loadAll(s"$directory/flows", configVariables) match {
      case Right(configs) => configs
      case Left(error)    => throw error
    }
  }

  private def isMissingConfigFile(error: Throwable): Boolean = {
    @scala.annotation.tailrec
    def causedByMissingFile(current: Throwable): Boolean = current match {
      case _: FileNotFoundException | _: NoSuchFileException => true
      case other if other.getCause != null                   => causedByMissingFile(other.getCause)
      case _                                                 => false
    }
    error match {
      case _: ConfigFileException => causedByMissingFile(error)
      case _                      => false
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
      customValidators: Map[String, () => Validator],
      derivedTables: Seq[(String, DerivedTableContext => DataFrame)],
      batchListeners: Seq[BatchListener] = Seq.empty,
      pipelineVersion: String = "unversioned",
      runStore: RunStore = new InMemoryRunStore(),
      customReaders: Map[String, DataReaderFactory.ReaderFactory] = Map.empty
  )(implicit spark: SparkSession): IngestionPipeline = {
    new IngestionPipeline(
      globalConfig,
      flowConfigs,
      flowTransformations,
      domainsConfig,
      extraCatalogProviders,
      customValidators,
      derivedTables,
      batchListeners,
      pipelineVersion,
      runStore,
      customReaders
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
