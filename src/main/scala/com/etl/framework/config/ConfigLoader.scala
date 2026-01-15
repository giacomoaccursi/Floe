package com.etl.framework.config

import io.circe.{Decoder, HCursor}
import io.circe.yaml.parser
import io.circe.generic.semiauto._

import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Base trait for configuration loaders
 */
trait ConfigLoader[T] {
  def load(path: String): Either[ConfigurationException, T]
  
  protected def loadYamlFile(path: String): Either[ConfigurationException, String] = {
    Try {
      val source = Source.fromFile(path)
      try source.mkString finally source.close()
    } match {
      case Success(content) => Right(content)
      case Failure(ex) => Left(ConfigurationException(
        s"Failed to read configuration file: $path",
        Some(ex)
      ))
    }
  }
  
  protected def parseYaml[A: Decoder](yaml: String, path: String): Either[ConfigurationException, A] = {
    parser.parse(yaml) match {
      case Right(json) =>
        json.as[A] match {
          case Right(config) => Right(config)
          case Left(error) => Left(ConfigurationException(
            s"Failed to parse configuration from $path: ${error.getMessage}",
            Some(error)
          ))
        }
      case Left(error) => Left(ConfigurationException(
        s"Invalid YAML syntax in $path: ${error.getMessage}",
        Some(error)
      ))
    }
  }
  
  /**
   * Substitutes environment variables in the format ${VAR_NAME} or $VAR_NAME
   */
  protected def substituteEnvVars(text: String): String = {
    // Pattern to match ${VAR_NAME} or $VAR_NAME
    val pattern = """\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)""".r
    
    pattern.replaceAllIn(text, m => {
      val varName = Option(m.group(1)).getOrElse(m.group(2))
      sys.env.getOrElse(varName, m.matched)
    })
  }
}

/**
 * Configuration exception
 */
case class ConfigurationException(
  message: String,
  cause: Option[Throwable] = None
) extends Exception(message, cause.orNull)

/**
 * Circe decoders for configuration classes
 */
object ConfigDecoders {
  implicit val sparkConfigDecoder: Decoder[SparkConfig] = deriveDecoder[SparkConfig]
  implicit val pathsConfigDecoder: Decoder[PathsConfig] = deriveDecoder[PathsConfig]
  implicit val processingConfigDecoder: Decoder[ProcessingConfig] = deriveDecoder[ProcessingConfig]
  implicit val performanceConfigDecoder: Decoder[PerformanceConfig] = deriveDecoder[PerformanceConfig]
  implicit val monitoringConfigDecoder: Decoder[MonitoringConfig] = deriveDecoder[MonitoringConfig]
  implicit val securityConfigDecoder: Decoder[SecurityConfig] = deriveDecoder[SecurityConfig]
  implicit val globalConfigDecoder: Decoder[GlobalConfig] = deriveDecoder[GlobalConfig]

  
  implicit val sourceConfigDecoder: Decoder[SourceConfig] = deriveDecoder[SourceConfig]
  implicit val columnConfigDecoder: Decoder[ColumnConfig] = deriveDecoder[ColumnConfig]
  implicit val schemaConfigDecoder: Decoder[SchemaConfig] = deriveDecoder[SchemaConfig]
  implicit val loadModeConfigDecoder: Decoder[LoadModeConfig] = deriveDecoder[LoadModeConfig]
  implicit val referenceConfigDecoder: Decoder[ReferenceConfig] = deriveDecoder[ReferenceConfig]
  implicit val foreignKeyConfigDecoder: Decoder[ForeignKeyConfig] = deriveDecoder[ForeignKeyConfig]
  implicit val validationRuleDecoder: Decoder[ValidationRule] = deriveDecoder[ValidationRule]
  implicit val validationConfigDecoder: Decoder[ValidationConfig] = deriveDecoder[ValidationConfig]
  implicit val outputConfigDecoder: Decoder[OutputConfig] = deriveDecoder[OutputConfig]
  
  // Custom decoder for FlowConfig that excludes transformation fields (they are set programmatically, not from YAML)
  implicit val flowConfigDecoder: Decoder[FlowConfig] = (c: HCursor) => {
    for {
      name <- c.downField("name").as[String]
      description <- c.downField("description").as[String]
      version <- c.downField("version").as[String]
      owner <- c.downField("owner").as[String]
      source <- c.downField("source").as[SourceConfig]
      schema <- c.downField("schema").as[SchemaConfig]
      loadMode <- c.downField("loadMode").as[LoadModeConfig]
      validation <- c.downField("validation").as[ValidationConfig]
      output <- c.downField("output").as[OutputConfig]
    } yield FlowConfig(
      name = name,
      description = description,
      version = version,
      owner = owner,
      source = source,
      schema = schema,
      loadMode = loadMode,
      validation = validation,
      output = output,
      preValidationTransformation = None,  // Set programmatically
      postValidationTransformation = None  // Set programmatically
    )
  }
  
  implicit val modelConfigDecoder: Decoder[ModelConfig] = deriveDecoder[ModelConfig]
  implicit val dagOutputConfigDecoder: Decoder[DAGOutputConfig] = deriveDecoder[DAGOutputConfig]
  implicit val joinConditionDecoder: Decoder[JoinCondition] = deriveDecoder[JoinCondition]
  implicit val aggregationSpecDecoder: Decoder[AggregationSpec] = deriveDecoder[AggregationSpec]
  implicit val joinConfigDecoder: Decoder[JoinConfig] = deriveDecoder[JoinConfig]
  implicit val dagNodeDecoder: Decoder[DAGNode] = deriveDecoder[DAGNode]
  implicit val aggregationConfigDecoder: Decoder[AggregationConfig] = deriveDecoder[AggregationConfig]
}

/**
 * Global configuration loader
 */
class GlobalConfigLoader extends ConfigLoader[GlobalConfig] {
  import ConfigDecoders._
  
  override def load(path: String): Either[ConfigurationException, GlobalConfig] = {
    for {
      yaml <- loadYamlFile(path)
      substituted = substituteEnvVars(yaml)
      config <- parseYaml[GlobalConfig](substituted, path)
    } yield config
  }
}


/**
 * Flow configuration loader
 */
class FlowConfigLoader extends ConfigLoader[FlowConfig] {
  import ConfigDecoders._
  import java.io.File
  
  override def load(path: String): Either[ConfigurationException, FlowConfig] = {
    for {
      yaml <- loadYamlFile(path)
      config <- parseYaml[FlowConfig](yaml, path)
    } yield config
  }
  
  /**
   * Loads all flow configurations from a directory
   */
  def loadAll(directory: String): Either[ConfigurationException, Seq[FlowConfig]] = {
    Try {
      val dir = new File(directory)
      if (!dir.exists() || !dir.isDirectory) {
        throw new IllegalArgumentException(s"Directory does not exist: $directory")
      }
      
      val yamlFiles = dir.listFiles()
        .filter(_.isFile)
        .filter(f => f.getName.endsWith(".yaml") || f.getName.endsWith(".yml"))
        .toSeq
      
      yamlFiles.map(f => load(f.getAbsolutePath))
    } match {
      case Success(results) =>
        // Collect all errors or return all configs
        val (errors, configs) = results.partition(_.isLeft)
        if (errors.nonEmpty) {
          Left(errors.head.left.get)
        } else {
          Right(configs.map(_.right.get))
        }
      case Failure(ex) =>
        Left(ConfigurationException(
          s"Failed to load flow configurations from directory: $directory",
          Some(ex)
        ))
    }
  }
}

/**
 * DAG configuration loader
 */
class DAGConfigLoader extends ConfigLoader[AggregationConfig] {
  import ConfigDecoders._
  
  override def load(path: String): Either[ConfigurationException, AggregationConfig] = {
    for {
      yaml <- loadYamlFile(path)
      config <- parseYaml[AggregationConfig](yaml, path)
    } yield config
  }
}
