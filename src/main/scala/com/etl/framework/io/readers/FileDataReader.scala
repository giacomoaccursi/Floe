package com.etl.framework.io.readers

import com.etl.framework.config.{SchemaConfig, SourceConfig}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * DataReader implementation for file-based sources
 * Supports CSV, Parquet, JSON formats with file pattern matching
 * Can optionally enforce schema during read for type safety
 */
class FileDataReader(
  sourceConfig: SourceConfig,
  schemaConfig: Option[SchemaConfig] = None
)(implicit spark: SparkSession) extends DataReader {

  /**
   * Reads data from file source
   * @return DataFrame containing the file data
   * @throws UnsupportedFileFormatException if file format is not supported
   */
  override def read(): DataFrame = {
    // Validate format
    validateFormat(sourceConfig.format)

    // Create reader with format
    var reader = spark.read.format(sourceConfig.format)
    
    // Apply schema if provided and enforceSchema is true
    schemaConfig.foreach { schema =>
      if (schema.enforceSchema) {
        val sparkSchema = convertToSparkSchema(schema)
        reader = reader.schema(sparkSchema)
      }
    }
    
    // Apply options
    reader = reader.options(sourceConfig.options)

    // Determine path with optional file pattern
    val fullPath = sourceConfig.filePattern match {
      case Some(pattern) => s"${sourceConfig.path}/$pattern"
      case None => sourceConfig.path
    }

    // Load data
    reader.load(fullPath)
  }

  /**
   * Converts SchemaConfig to Spark StructType
   */
  private def convertToSparkSchema(schema: SchemaConfig): StructType = {
    StructType(schema.columns.map { col =>
      StructField(
        col.name,
        mapTypeToSparkType(col.`type`),
        col.nullable
      )
    })
  }
  
  /**
   * Maps string type names to Spark DataTypes
   */
  private def mapTypeToSparkType(typeName: String): DataType = {
    typeName.toLowerCase match {
      case "string" | "varchar" | "text" => StringType
      case "int" | "integer" => IntegerType
      case "long" | "bigint" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "boolean" | "bool" => BooleanType
      case "date" => DateType
      case "timestamp" | "datetime" => TimestampType
      case "decimal" => DecimalType(38, 18) // Default precision
      case "binary" => BinaryType
      case "byte" | "tinyint" => ByteType
      case "short" | "smallint" => ShortType
      case other => 
        throw new UnsupportedTypeException(
          s"Unsupported type: $other. Supported types: string, int, long, float, double, boolean, date, timestamp, decimal, binary, byte, short"
        )
    }
  }

  /**
   * Validates that the file format is supported
   * @param format File format to validate
   * @throws UnsupportedFileFormatException if format is not supported
   */
  private def validateFormat(format: String): Unit = {
    val supportedFormats = Set("csv", "parquet", "json")
    if (!supportedFormats.contains(format.toLowerCase)) {
      throw new UnsupportedFileFormatException(
        s"Unsupported file format: $format. Supported formats: ${supportedFormats.mkString(", ")}"
      )
    }
  }
}

/**
 * Exception thrown when an unsupported file format is encountered
 */
class UnsupportedFileFormatException(message: String) extends RuntimeException(message)

/**
 * Exception thrown when an unsupported type is encountered
 */
class UnsupportedTypeException(message: String) extends RuntimeException(message)
