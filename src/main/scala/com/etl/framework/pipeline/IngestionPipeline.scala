package com.etl.framework.pipeline

import com.etl.framework.config.{
  DomainsConfig,
  DomainsConfigLoader,
  FlowConfig,
  FlowConfigLoader,
  FlowConfigValidator,
  GlobalConfig,
  GlobalConfigLoader
}
import com.etl.framework.exceptions.{BatchFailedException, ConfigFileException, MissingConfigFieldException}
import com.etl.framework.iceberg.catalog.{CatalogProvider, CatalogRuntime}
import com.etl.framework.io.readers.DataReaderFactory
import com.etl.framework.orchestration.{
  BatchListener,
  ExecutionRequest,
  FlowOrchestrator,
  IngestionResult,
  PipelineDefinition,
  PipelineDefinitionBuilder
}
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
    derivedTables: Seq[DerivedTableDefinition],
    batchListeners: Seq[BatchListener],
    pipelineId: String,
    codeVersion: String,
    customReaders: Map[String, DataReaderFactory.ReaderFactory]
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)
  private lazy val resolvedFlowConfigs = flowConfigs.map { flowConfig =>
    flowTransformations.get(flowConfig.name) match {
      case Some(transformations) =>
        flowConfig.copy(
          preValidationTransformation = transformations.preValidation,
          postValidationTransformation = transformations.postValidation
        )
      case None => flowConfig
    }
  }

  lazy val executionDefinition: PipelineDefinition = {
    val semanticSparkConfig = Seq(
      "spark.sql.session.timeZone",
      "spark.sql.ansi.enabled",
      "spark.sql.caseSensitive",
      "spark.sql.legacy.timeParserPolicy"
    ).flatMap(key => spark.conf.getOption(key).map(key -> _)).toMap
    PipelineDefinitionBuilder.build(
      pipelineId,
      codeVersion,
      globalConfig,
      resolvedFlowConfigs,
      domainsConfig,
      derivedTables.map(definition => definition.name -> definition.dependencies),
      semanticSparkConfig
    )
  }

  /** Executes Ingestion pipeline Returns IngestionResult with batch ID and flow results
    */
  def execute(): IngestionResult = {
    logger.info("Executing Ingestion pipeline")
    createOrchestrator().execute()
  }

  /** Executes the immutable request supplied by the hosting platform. */
  def execute(request: ExecutionRequest): IngestionResult = {
    logger.info(s"Executing ingestion attempt ${request.attemptId} for logical run ${request.logicalRunId}")
    createOrchestrator().execute(request)
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

  /** Executes an explicit request and throws when its functional outcome is not successful. */
  def executeOrThrow(request: ExecutionRequest): IngestionResult = {
    val result = execute(request)
    if (!result.success)
      throw BatchFailedException(result.attemptId, result.error.getOrElse("unknown error"))
    result
  }

  private def createOrchestrator(): FlowOrchestrator = {
    CatalogRuntime.prepare(spark, globalConfig.iceberg, extraCatalogProviders.toMap)
    FlowOrchestrator(
      globalConfig,
      resolvedFlowConfigs,
      domainsConfig,
      customValidators.toMap,
      batchListeners,
      customReaders.toMap,
      derivedTables,
      pipelineId = pipelineId,
      codeVersion = codeVersion
    )
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
  private val derivedTables = mutable.ListBuffer[DerivedTableDefinition]()
  private val batchListeners = mutable.ListBuffer[BatchListener]()
  private val customReaders = mutable.Map[String, DataReaderFactory.ReaderFactory]()
  private var pipelineId: String = "local"
  private var codeVersion: String = "unversioned"
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

  /** Registers a derived table that will be computed after all flows are written to Iceberg. Every input must be
    * declared; the context exposes only attempt-pinned dependencies. The result is written as a full-load table.
    *
    * @param tableName
    *   Name of the derived table (becomes the Iceberg table name)
    * @param dependencies
    *   Primary or derived table names read by this transformation
    * @param fn
    *   Function that produces the derived DataFrame
    * @return
    *   This builder for chaining
    */
  def withDerivedTable(
      tableName: String,
      dependencies: Seq[String],
      fn: DerivedTableContext => DataFrame
  ): IngestionPipelineBuilder = {
    if (derivedTables.exists(_.name.equalsIgnoreCase(tableName)))
      throw new IllegalArgumentException(s"Derived table '$tableName' is already registered")
    logger.info(s"Registering derived table: $tableName")
    derivedTables += DerivedTableDefinition(tableName, dependencies, fn)
    this
  }

  /** Registers a BatchListener that receives notifications on batch completion or failure. */
  def withBatchListener(listener: BatchListener): IngestionPipelineBuilder = {
    batchListeners += listener
    this
  }

  /** Sets the stable pipeline identity used by platform-managed execution requests. */
  def withPipelineId(id: String): IngestionPipelineBuilder = {
    com.etl.framework.orchestration.ExecutionRequest.validateIdentifier("pipelineId", id)
    this.pipelineId = id
    this
  }

  /** Identifies the immutable application artifact, including custom transformations, readers and validators. */
  def withCodeVersion(version: String): IngestionPipelineBuilder = {
    require(version != null && version.trim.nonEmpty, "codeVersion must not be blank")
    this.codeVersion = version.trim
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
    val derivedErrors = DerivedTableExecutor.validateDefinitions(derivedTables.toSeq, flowConfigs.map(_.name).toSet)
    require(derivedErrors.isEmpty, s"Invalid derived table configuration: ${derivedErrors.mkString("; ")}")

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
      pipelineId,
      codeVersion,
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
    errors ++= DerivedTableExecutor.validateDefinitions(derivedTables.toSeq, flowConfigs.map(_.name).toSet)
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
    derivedTables.map(_.name).filter(name => managedNames.contains(name.toLowerCase(Locale.ROOT))).toSeq
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
      derivedTables: Seq[DerivedTableDefinition],
      batchListeners: Seq[BatchListener] = Seq.empty,
      pipelineId: String = "local",
      codeVersion: String = "unversioned",
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
      pipelineId,
      codeVersion,
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
