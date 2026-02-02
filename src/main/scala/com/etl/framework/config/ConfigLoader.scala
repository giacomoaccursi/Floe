package com.etl.framework.config

import com.etl.framework.exceptions.DataSourceException
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
          case Left(error) => 
            // Extract detailed error information from Circe's DecodingFailure
            val detailedMessage = error match {
              case df: io.circe.DecodingFailure =>
                val missingField = extractMissingField(df.history)
                val parentPath = formatParentPath(df.history)
                
                // Build a clear error message with context
                missingField match {
                  case Some(field) =>
                    val location = if (parentPath.nonEmpty) s"$parentPath" else "root"
                    val yamlContext = extractYamlContext(yaml, location, field)
                    
                    val contextInfo = if (yamlContext.nonEmpty) {
                      s"\n\nContext from YAML file:\n$yamlContext\n"
                    } else ""
                    
                    s"Missing required field '$field' in '$location'$contextInfo" +
                    s"\nHint: Add the field to your YAML configuration.\n" +
                    s"File: $path"
                    
                  case None =>
                    // Fallback to original message if we can't extract field name
                    val readablePath = formatFullPath(df.history)
                    val pathInfo = if (readablePath.nonEmpty) s" at '$readablePath'" else ""
                    s"${df.message}$pathInfo\nFile: $path"
                }
                
              case other => s"${other.getMessage}\nFile: $path"
            }
            Left(ConfigurationException(
              s"Failed to parse configuration from $path:\n$detailedMessage",
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
   * Extracts the missing field name from Circe's cursor history
   * The first DownField in the history is usually the missing field
   */
  private def extractMissingField(history: List[io.circe.CursorOp]): Option[String] = {
    history.collectFirst {
      case io.circe.CursorOp.DownField(field) => field
    }
  }

  /**
   * Formats the parent path (excluding the missing field) like "validation.rules[*]"
   */
  private def formatParentPath(history: List[io.circe.CursorOp]): String = {
    // Skip the first DownField which is the missing field itself
    val withoutMissingField = history.reverse.dropWhile {
      case io.circe.CursorOp.DownField(_) => true
      case _ => false
    }
    
    val pathParts = withoutMissingField.map {
      case io.circe.CursorOp.DownField(field) => s".$field"
      case io.circe.CursorOp.DownArray => "[*]"
      case io.circe.CursorOp.DownN(n) => s"[$n]"
      case _ => ""
    }.mkString("").stripPrefix(".")
    
    pathParts
  }

  /**
   * Formats the full path including all fields
   */
  private def formatFullPath(history: List[io.circe.CursorOp]): String = {
    history.reverse.map {
      case io.circe.CursorOp.DownField(field) => s".$field"
      case io.circe.CursorOp.DownArray => "[*]"
      case io.circe.CursorOp.DownN(n) => s"[$n]"
      case _ => ""
    }.mkString("").stripPrefix(".")
  }

  /**
   * Extracts relevant YAML context around the error location
   * Shows the parent section where the field is missing
   */
  private def extractYamlContext(yaml: String, parentPath: String, missingField: String): String = {
    val lines = yaml.split("\n")
    
    // Try to find the parent section in YAML
    val pathParts = parentPath.split("\\.").filter(_.nonEmpty).filterNot(_.contains("["))
    
    if (pathParts.isEmpty) {
      // Root level - show first few lines
      val preview = lines.take(10).mkString("\n")
      s"$preview\n... (missing field '$missingField' at root level)"
    } else {
      // Find the section
      var currentIndent = 0
      var sectionStart = -1
      var sectionEnd = -1
      var foundSection = false
      
      // Look for the last path part (the immediate parent)
      val targetSection = pathParts.last
      
      for (i <- lines.indices if !foundSection) {
        val line = lines(i)
        val trimmed = line.trim
        
        if (trimmed.startsWith(s"$targetSection:") || trimmed.startsWith(s"- $targetSection:")) {
          sectionStart = i
          currentIndent = line.takeWhile(_.isWhitespace).length
          foundSection = true
          
          // Find end of section (next line with same or less indentation)
          var j = i + 1
          while (j < lines.length && sectionEnd == -1) {
            val nextLine = lines(j)
            if (nextLine.trim.nonEmpty) {
              val nextIndent = nextLine.takeWhile(_.isWhitespace).length
              if (nextIndent <= currentIndent && !nextLine.trim.startsWith("-")) {
                sectionEnd = j - 1
              }
            }
            j += 1
          }
          if (sectionEnd == -1) sectionEnd = lines.length - 1
        }
      }
      
      if (foundSection && sectionStart >= 0) {
        val contextLines = lines.slice(sectionStart, Math.min(sectionEnd + 1, sectionStart + 15))
        val lineNumbers = (sectionStart + 1) to Math.min(sectionEnd + 1, sectionStart + 15)
        val numbered = contextLines.zip(lineNumbers).map { case (line, num) =>
          f"  $num%4d | $line"
        }.mkString("\n")
        
        s"$numbered\n  ^^^^ Missing field '$missingField' should be added here"
      } else {
        s"Could not locate section '$parentPath' in YAML (missing field: '$missingField')"
      }
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

  implicit val domainConfigDecoder: Decoder[DomainConfig] = deriveDecoder[DomainConfig]
  implicit val domainsConfigDecoder: Decoder[DomainsConfig] = deriveDecoder[DomainsConfig]

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
 * Domains configuration loader
 */
class DomainsConfigLoader extends ConfigLoader[DomainsConfig] {
  import ConfigDecoders._

  override def load(path: String): Either[ConfigurationException, DomainsConfig] = {
    for {
      yaml <- loadYamlFile(path)
      config <- parseYaml[DomainsConfig](yaml, path)
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
        throw DataSourceException(
          sourceType = "directory",
          sourcePath = directory,
          details = "Directory does not exist or is not a directory"
        )
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
