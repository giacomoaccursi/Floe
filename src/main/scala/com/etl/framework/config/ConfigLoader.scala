package com.etl.framework.config

import com.etl.framework.exceptions.{ConfigFileException, ConfigurationException, YAMLSyntaxException}
import pureconfig._
import pureconfig.error.{ConfigReaderFailures, ConvertFailure, KeyNotFound, ThrowableFailure}
import pureconfig.generic.auto._
import pureconfig.module.yaml._
// Import ConfigHints at top level to ensure all derived readers in this file use CamelCase
import ConfigHints._

import java.io.File
import scala.io.Source
import scala.util.Using
import java.util.regex.Matcher

/** Global configuration hints to enforce CamelCase naming strategy (e.g. "outputPath" in YAML matches "outputPath" in
  * case class)
  */
object ConfigHints {
  import pureconfig.ConfigFieldMapping
  import pureconfig.generic.ProductHint
  import pureconfig.CamelCase

  implicit def productHint[T]: ProductHint[T] =
    ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
}

/** Base trait for configuration loaders using PureConfig
  */
trait ConfigLoader[T] {
  def load(path: String): Either[ConfigurationException, T]

  /** Load configuration from a YAML file using PureConfig
    */
  protected def loadFromYamlFile(
      path: String,
      variables: Map[String, String] = Map.empty
  )(implicit reader: ConfigReader[T]): Either[ConfigurationException, T] = {
    for {
      yaml <- loadYamlFile(path)
      substituted <- substituteEnvVars(yaml, path, variables)
      config <- parseYaml(substituted, path)
    } yield config
  }

  /** Read raw YAML content from file
    */
  protected def loadYamlFile(
      path: String
  ): Either[ConfigurationException, String] = {
    Using(Source.fromFile(path)) { source =>
      source.mkString
    }.toEither.left.map { ex =>
      ConfigFileException(
        file = path,
        message = "Failed to read configuration file",
        cause = ex
      )
    }
  }

  /** Parse YAML string to configuration type using PureConfig
    */
  protected def parseYaml(yaml: String, path: String)(implicit
      reader: ConfigReader[T]
  ): Either[ConfigurationException, T] = {
    YamlConfigSource.string(yaml).load[T].left.map { failures =>
      convertPureConfigError(failures, path)
    }
  }

  /** Convert PureConfig errors to our custom exception hierarchy
    */
  protected def convertPureConfigError(
      failures: ConfigReaderFailures,
      path: String
  ): ConfigurationException = {
    failures.head match {
      case ConvertFailure(KeyNotFound(key, _), _, configPath) =>
        val location = if (configPath.isEmpty) "root" else configPath
        ConfigFileException(
          file = path,
          message = s"Missing required field '$key' at '$location'"
        )

      case ThrowableFailure(throwable, _)
          if throwable.getMessage != null &&
            throwable.getMessage.contains("while parsing") =>
        YAMLSyntaxException(
          file = path,
          details = throwable.getMessage,
          cause = throwable
        )

      case other =>
        val location =
          other.origin.map(_.description).getOrElse("unknown location")
        val description = other.description
        ConfigFileException(
          file = path,
          message = s"Configuration Error at $location: $description"
        )
    }
  }

  /** Substitutes environment variables in the format ${VAR_NAME} or $VAR_NAME. Returns Left if any referenced variable
    * is not set.
    */
  protected def substituteEnvVars(
      text: String,
      path: String = "<unknown>",
      variables: Map[String, String] = Map.empty
  ): Either[ConfigurationException, String] = {
    val escapePlaceholder = "\u0000DOLLAR\u0000"
    val escaped = text.replace("$$", escapePlaceholder)

    val pattern = """\$\{([^}]+)}|\$([A-Za-z_][A-Za-z0-9_]*)""".r

    def resolve(name: String): Option[String] =
      variables.get(name).orElse(sys.env.get(name))

    val unresolvedVars = pattern
      .findAllMatchIn(escaped)
      .map(m => Option(m.group(1)).getOrElse(m.group(2)))
      .filterNot(resolve(_).isDefined)
      .toSeq
      .distinct

    if (unresolvedVars.nonEmpty) {
      Left(
        ConfigFileException(
          file = path,
          message = s"Unresolved environment variables: ${unresolvedVars.mkString(", ")}. " +
            "Set these environment variables before running the pipeline."
        )
      )
    } else {
      val substituted = pattern.replaceAllIn(
        escaped,
        m => {
          val varName = Option(m.group(1)).getOrElse(m.group(2))
          Matcher.quoteReplacement(resolve(varName).get)
        }
      )
      Right(substituted.replace(escapePlaceholder, "$"))
    }
  }
}

/** Global configuration loader
  */
class GlobalConfigLoader extends ConfigLoader[GlobalConfig] {

  override def load(
      path: String
  ): Either[ConfigurationException, GlobalConfig] = load(path, Map.empty)

  def load(
      path: String,
      variables: Map[String, String]
  ): Either[ConfigurationException, GlobalConfig] =
    loadFromYamlFile(path, variables)
}

/** Domains configuration loader
  */
class DomainsConfigLoader extends ConfigLoader[DomainsConfig] {

  override def load(
      path: String
  ): Either[ConfigurationException, DomainsConfig] = load(path, Map.empty)

  def load(
      path: String,
      variables: Map[String, String]
  ): Either[ConfigurationException, DomainsConfig] =
    loadFromYamlFile(path, variables)
}

/** Internal case class for parsing FlowConfig from YAML Excludes FlowTransformation fields which cannot be deserialized
  * from YAML
  */
private[config] case class FlowConfigYaml(
    name: String,
    description: String = "",
    version: String = "",
    owner: String = "",
    source: SourceConfig,
    schema: SchemaConfig,
    loadMode: LoadModeConfig,
    validation: ValidationConfig = ValidationConfig(),
    output: OutputConfig = OutputConfig(),
    dependsOn: Seq[String] = Seq.empty,
    maxRejectionRate: Option[Double] = None
) {
  def toFlowConfig: FlowConfig = FlowConfig(
    name = name,
    description = description,
    version = version,
    owner = owner,
    source = source,
    schema = schema,
    loadMode = loadMode,
    validation = validation,
    output = output,
    dependsOn = dependsOn,
    maxRejectionRate = maxRejectionRate,
    preValidationTransformation = None,
    postValidationTransformation = None
  )
}

private[config] object FlowConfigYaml {
  def fromFlowConfig(fc: FlowConfig): FlowConfigYaml = FlowConfigYaml(
    name = fc.name,
    description = fc.description,
    version = fc.version,
    owner = fc.owner,
    source = fc.source,
    schema = fc.schema,
    loadMode = fc.loadMode,
    validation = fc.validation,
    output = fc.output,
    dependsOn = fc.dependsOn,
    maxRejectionRate = fc.maxRejectionRate
  )
}

/** Flow configuration loader
  */
class FlowConfigLoader extends ConfigLoader[FlowConfig] {

  override def load(
      path: String
  ): Either[ConfigurationException, FlowConfig] = load(path, Map.empty)

  def load(
      path: String,
      variables: Map[String, String]
  ): Either[ConfigurationException, FlowConfig] = {
    for {
      rawYaml <- loadYamlFile(path)
      yaml <- substituteEnvVars(rawYaml, path, variables)
      configYaml <- parseYamlToFlowConfigYaml(yaml, path)
      flowConfig = configYaml.toFlowConfig
      _ <- validateFlowConfig(flowConfig, path)
    } yield flowConfig
  }

  private def validateFlowConfig(
      config: FlowConfig,
      path: String
  ): Either[ConfigurationException, Unit] = {
    def err(msg: String): Either[ConfigurationException, Unit] =
      Left(ConfigFileException(file = path, message = s"Flow '${config.name}': $msg"))

    config.loadMode.`type` match {
      case LoadMode.SCD2 =>
        if (config.loadMode.compareColumns.isEmpty)
          err("SCD2 requires compareColumns to be non-empty")
        else if (config.validation.primaryKey.isEmpty)
          err("SCD2 requires non-empty primaryKey for record identification")
        else {
          // SCD2 uses NULL as a sentinel in merge key columns; a nullable PK
          // collides with it and causes duplicate insertions on every run.
          val nullablePkCols = config.validation.primaryKey.filter { pk =>
            config.schema.columns.find(_.name == pk).exists(_.nullable)
          }
          if (nullablePkCols.nonEmpty)
            err(
              s"SCD2 requires all PK columns to be non-nullable; " +
                s"declare nullable=false for: ${nullablePkCols.mkString(", ")}"
            )
          else Right(())
        }
      case _ => Right(())
    }
  }

  private def parseYamlToFlowConfigYaml(
      yaml: String,
      path: String
  ): Either[ConfigurationException, FlowConfigYaml] = {
    import ConfigHints._
    YamlConfigSource.string(yaml).load[FlowConfigYaml].left.map { failures =>
      convertPureConfigError(failures, path)
    }
  }

  /** Loads all flow configurations from a directory. If any files fail to load, all errors are reported together.
    */
  def loadAll(
      directory: String
  ): Either[ConfigurationException, Seq[FlowConfig]] = loadAll(directory, Map.empty)

  def loadAll(
      directory: String,
      variables: Map[String, String]
  ): Either[ConfigurationException, Seq[FlowConfig]] = {
    val dir = new File(directory)
    if (!dir.exists() || !dir.isDirectory)
      Left(ConfigFileException(file = directory, message = "Directory does not exist or is not a directory"))
    else {
      val results = Option(dir.listFiles())
        .getOrElse(Array.empty)
        .filter(f => f.isFile && (f.getName.endsWith(".yaml") || f.getName.endsWith(".yml")))
        .toSeq
        .map(f => load(f.getAbsolutePath, variables))

      val errors = results.collect { case Left(err) => err }
      val configs = results.collect { case Right(cfg) => cfg }

      if (errors.nonEmpty)
        Left(
          ConfigFileException(
            file = directory,
            message = s"${errors.size} flow configuration(s) failed to load:\n" +
              errors.map(e => s"  - ${e.getMessage}").mkString("\n")
          )
        )
      else {
        val duplicates = configs.groupBy(_.name).collect { case (name, dups) if dups.size > 1 => name }.toSeq
        if (duplicates.nonEmpty)
          Left(
            ConfigFileException(
              file = directory,
              message = s"Duplicate flow names: ${duplicates.mkString(", ")}. Each flow must have a unique name."
            )
          )
        else Right(configs)
      }
    }
  }
}

/** DAG configuration loader
  */
class DAGConfigLoader extends ConfigLoader[AggregationConfig] {

  override def load(
      path: String
  ): Either[ConfigurationException, AggregationConfig] = load(path, Map.empty)

  def load(
      path: String,
      variables: Map[String, String]
  ): Either[ConfigurationException, AggregationConfig] = {
    for {
      config <- loadFromYamlFile(path, variables)
      _ <- validateAggregationConfig(config, path)
    } yield config
  }

  private def validateAggregationConfig(
      config: AggregationConfig,
      path: String
  ): Either[ConfigurationException, Unit] = {
    def err(msg: String): Either[ConfigurationException, Unit] =
      Left(ConfigFileException(file = path, message = s"DAG '${config.name}': $msg"))

    val nodeIds = config.nodes.map(_.id).toSet
    val duplicates = config.nodes
      .map(_.id)
      .groupBy(identity)
      .collect { case (id, ids) if ids.size > 1 => id }
      .toSeq

    def validateNode(node: DAGNode): Either[ConfigurationException, Unit] = {
      val sourceErrors =
        if (node.sourceFlow.isEmpty && node.sourceTable.isEmpty)
          Seq(s"node '${node.id}' must have either sourceFlow or sourceTable")
        else Seq.empty
      val joinErrors = node.joins.flatMap { joinConfig =>
        val missingWith =
          if (!nodeIds.contains(joinConfig.`with`))
            Seq(s"node '${node.id}' references missing join target '${joinConfig.`with`}'")
          else Seq.empty
        val emptyOn =
          if (joinConfig.conditions.isEmpty)
            Seq(s"node '${node.id}' has a join with no conditions")
          else Seq.empty
        missingWith ++ emptyOn
      }
      val allErrors = sourceErrors ++ joinErrors
      allErrors.headOption.fold[Either[ConfigurationException, Unit]](Right(()))(err)
    }

    if (config.nodes.isEmpty) err("nodes list must not be empty")
    else if (duplicates.nonEmpty) err(s"duplicate node IDs: ${duplicates.mkString(", ")}")
    else
      config.nodes.foldLeft[Either[ConfigurationException, Unit]](Right(())) { (acc, node) =>
        acc.flatMap(_ => validateNode(node))
      }
  }
}
