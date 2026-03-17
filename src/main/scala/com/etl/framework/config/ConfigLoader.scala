package com.etl.framework.config

import com.etl.framework.exceptions.{
  ConfigFileException,
  ConfigurationException,
  YAMLSyntaxException
}
import pureconfig._
import pureconfig.error.{
  ConfigReaderFailures,
  ConvertFailure,
  KeyNotFound,
  ThrowableFailure
}
import pureconfig.generic.auto._
import pureconfig.module.yaml._
// Import ConfigHints at top level to ensure all derived readers in this file use CamelCase
import ConfigHints._

import java.io.File
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}
import java.util.regex.Matcher

/** Global configuration hints to enforce CamelCase naming strategy (e.g.
  * "outputPath" in YAML matches "outputPath" in case class)
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
      path: String
  )(implicit reader: ConfigReader[T]): Either[ConfigurationException, T] = {
    for {
      yaml <- loadYamlFile(path)
      substituted = substituteEnvVars(yaml)
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
  private def convertPureConfigError(
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

  /** Substitutes environment variables in the format ${VAR_NAME} or $VAR_NAME.
    * Fails fast if any referenced variable is not set.
    */
  protected def substituteEnvVars(text: String): String = {
    val pattern = """\$\{([^}]+)}|\$([A-Za-z_][A-Za-z0-9_]*)""".r

    val unresolvedVars = pattern
      .findAllMatchIn(text)
      .map(m => Option(m.group(1)).getOrElse(m.group(2)))
      .filterNot(sys.env.contains)
      .toSeq
      .distinct

    if (unresolvedVars.nonEmpty) {
      throw new IllegalArgumentException(
        s"Unresolved environment variables: ${unresolvedVars.mkString(", ")}. " +
          "Set these environment variables before running the pipeline."
      )
    }

    pattern.replaceAllIn(
      text,
      m => {
        val varName = Option(m.group(1)).getOrElse(m.group(2))
        Matcher.quoteReplacement(sys.env(varName))
      }
    )
  }
}

/** Global configuration loader
  */
class GlobalConfigLoader extends ConfigLoader[GlobalConfig] {

  override def load(
      path: String
  ): Either[ConfigurationException, GlobalConfig] = {
    for {
      config <- loadFromYamlFile(path)
      _      <- validateIcebergConfig(config, path)
    } yield config
  }

  private def validateIcebergConfig(
      config: GlobalConfig,
      path: String
  ): Either[ConfigurationException, Unit] =
    config.iceberg match {
      case Some(iceberg) if iceberg.formatVersion != 1 && iceberg.formatVersion != 2 =>
        Left(
          ConfigFileException(
            file = path,
            message = s"Invalid formatVersion: ${iceberg.formatVersion}. Must be 1 or 2."
          )
        )
      case _ => Right(())
    }
}

/** Domains configuration loader
  */
class DomainsConfigLoader extends ConfigLoader[DomainsConfig] {

  override def load(
      path: String
  ): Either[ConfigurationException, DomainsConfig] = {
    for {
      yaml <- loadYamlFile(path)
      config <- parseYaml(yaml, path)
    } yield config
  }
}

/** Internal case class for parsing FlowConfig from YAML Excludes
  * FlowTransformation fields which cannot be deserialized from YAML
  */
private[config] case class FlowConfigYaml(
    name: String,
    description: String,
    version: String,
    owner: String,
    source: SourceConfig,
    schema: SchemaConfig,
    loadMode: LoadModeConfig,
    validation: ValidationConfig,
    output: OutputConfig
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
    output = fc.output
  )
}

/** Flow configuration loader
  */
class FlowConfigLoader extends ConfigLoader[FlowConfig] {

  override def load(
      path: String
  ): Either[ConfigurationException, FlowConfig] = {
    for {
      yaml       <- loadYamlFile(path)
      configYaml <- parseYamlToFlowConfigYaml(yaml, path)
      flowConfig  = configYaml.toFlowConfig
      _          <- validateFlowConfig(flowConfig, path)
    } yield flowConfig
  }

  private def validateFlowConfig(
      config: FlowConfig,
      path: String
  ): Either[ConfigurationException, Unit] = {
    if (config.loadMode.`type` == LoadMode.SCD2 && config.loadMode.compareColumns.isEmpty) {
      Left(
        ConfigFileException(
          file = path,
          message =
            s"Flow '${config.name}': SCD2 load mode requires compareColumns to be non-empty"
        )
      )
    } else if (config.loadMode.`type` == LoadMode.SCD2 && config.validation.primaryKey.isEmpty) {
      Left(
        ConfigFileException(
          file = path,
          message =
            s"Flow '${config.name}': SCD2 load mode requires non-empty primaryKey for record identification"
        )
      )
    } else {
      Right(())
    }
  }

  private def parseYamlToFlowConfigYaml(
      yaml: String,
      path: String
  ): Either[ConfigurationException, FlowConfigYaml] = {
    import ConfigHints._
    YamlConfigSource.string(yaml).load[FlowConfigYaml].left.map { failures =>
      val firstFailure = failures.head
      firstFailure match {
        case ConvertFailure(KeyNotFound(key, _), _, configPath) =>
          val location = if (configPath.isEmpty) "root" else configPath
          ConfigFileException(
            file = path,
            message = s"Missing required field '$key' at '$location'"
          )
        case other =>
          val location =
            other.origin.map(_.description).getOrElse("unknown location")
          ConfigFileException(
            file = path,
            message = s"Configuration Error at $location: ${other.description}"
          )
      }
    }
  }

  /** Loads all flow configurations from a directory
    */
  def loadAll(
      directory: String
  ): Either[ConfigurationException, Seq[FlowConfig]] = {
    Try {
      val dir = new File(directory)
      if (!dir.exists() || !dir.isDirectory) {
        throw ConfigFileException(
          file = directory,
          message = "Directory does not exist or is not a directory"
        )
      }

      val yamlFiles = dir
        .listFiles()
        .filter(_.isFile)
        .filter(f => f.getName.endsWith(".yaml") || f.getName.endsWith(".yml"))
        .toSeq

      yamlFiles.map(f => load(f.getAbsolutePath))
    } match {
      case Success(results) =>
        val (errors, configs) = results.partition(_.isLeft)
        errors.collectFirst { case Left(err) => err } match {
          case Some(err) => Left(err)
          case None      => Right(configs.collect { case Right(c) => c })
        }
      case Failure(ex) =>
        Left(
          ConfigFileException(
            file = directory,
            message = "Failed to load flow configurations from directory",
            cause = ex
          )
        )
    }
  }
}

/** DAG configuration loader
  */
class DAGConfigLoader extends ConfigLoader[AggregationConfig] {

  override def load(
      path: String
  ): Either[ConfigurationException, AggregationConfig] = {
    for {
      yaml   <- loadYamlFile(path)
      config <- parseYaml(yaml, path)
      _      <- validateAggregationConfig(config, path)
    } yield config
  }

  private def validateAggregationConfig(
      config: AggregationConfig,
      path: String
  ): Either[ConfigurationException, Unit] = {
    if (config.nodes.isEmpty) {
      return Left(ConfigFileException(file = path, message = s"DAG '${config.name}': nodes list must not be empty"))
    }

    val nodeIds = config.nodes.map(_.id).toSet
    val duplicates = config.nodes.map(_.id).groupBy(identity).collect { case (id, ids) if ids.size > 1 => id }
    if (duplicates.nonEmpty) {
      return Left(ConfigFileException(file = path, message = s"DAG '${config.name}': duplicate node IDs: ${duplicates.mkString(", ")}"))
    }

    config.nodes.foreach { node =>
      val missingDeps = node.dependencies.filterNot(nodeIds.contains)
      if (missingDeps.nonEmpty) {
        return Left(ConfigFileException(file = path, message = s"DAG '${config.name}': node '${node.id}' references missing dependencies: ${missingDeps.mkString(", ")}"))
      }
      node.join.foreach { joinConfig =>
        if (!nodeIds.contains(joinConfig.parent)) {
          return Left(ConfigFileException(file = path, message = s"DAG '${config.name}': node '${node.id}' references missing join parent '${joinConfig.parent}'"))
        }
        if (joinConfig.on.isEmpty) {
          return Left(ConfigFileException(file = path, message = s"DAG '${config.name}': node '${node.id}' has a join with no conditions"))
        }
      }
    }

    Right(())
  }
}
