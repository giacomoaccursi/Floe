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
  * "validatedPath" in YAML matches "validatedPath" in case class)
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

  /** Substitutes environment variables in the format ${VAR_NAME} or $VAR_NAME
    */
  protected def substituteEnvVars(text: String): String = {
    val pattern = """\$\{([^}]+)}|\$([A-Za-z_][A-Za-z0-9_]*)""".r

    pattern.replaceAllIn(
      text,
      m => {
        val varName = Option(m.group(1)).getOrElse(m.group(2))
        // Escape the replacement string if we are returning the original match
        // to avoid issues with $ and {} being interpreted as groups
        val replacement = sys.env.getOrElse(varName, m.matched)
        Matcher.quoteReplacement(replacement)
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
    loadFromYamlFile(path)
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
      yaml <- loadYamlFile(path)
      configYaml <- parseYamlToFlowConfigYaml(yaml, path)
    } yield configYaml.toFlowConfig
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
          case None      => Right(configs.map(_.toOption.get))
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
      yaml <- loadYamlFile(path)
      config <- parseYaml(yaml, path)
    } yield config
  }
}
