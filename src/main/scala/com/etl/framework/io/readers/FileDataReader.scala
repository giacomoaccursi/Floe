package com.etl.framework.io.readers

import com.etl.framework.config.{SchemaConfig, SourceConfig}
import com.etl.framework.exceptions.UnsupportedOperationException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** DataReader implementation for file-based sources Supports CSV, Parquet, JSON formats with file pattern matching Can
  * optionally enforce schema during read for type safety
  */
class FileDataReader(
    sourceConfig: SourceConfig,
    schemaConfig: Option[SchemaConfig] = None
)(implicit spark: SparkSession)
    extends DataReader {

  /** Reads data from file source
    * @return
    *   DataFrame containing the file data
    * @throws UnsupportedFileFormatException
    *   if file format is not supported
    */
  override def read(): DataFrame = {
    // Validate format
    validateFormat(sourceConfig.format.name) // Use .name

    // Create reader with format
    var reader = spark.read.format(sourceConfig.format.name) // Use .name

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
      case None          => sourceConfig.path
    }

    // Load data
    reader.load(fullPath)
  }

  /** Converts SchemaConfig to Spark StructType
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

  /** Maps string type names to Spark DataTypes
    */
  private def mapTypeToSparkType(typeName: String): DataType = {
    typeName.toLowerCase match {
      case "string" | "varchar" | "text" => StringType
      case "int" | "integer"             => IntegerType
      case "long" | "bigint"             => LongType
      case "float"                       => FloatType
      case "double"                      => DoubleType
      case "boolean" | "bool"            => BooleanType
      case "date"                        => DateType
      case "timestamp" | "datetime"      => TimestampType
      case d if d.startsWith("decimal") =>
        val paramPattern = """decimal\((\d+),\s*(\d+)\)""".r
        d match {
          case paramPattern(p, s) => DecimalType(p.toInt, s.toInt)
          case "decimal"          => DecimalType(38, 18)
          case _ =>
            throw UnsupportedOperationException(
              operation = s"type '$d'",
              details = "Decimal format must be 'decimal' or 'decimal(precision, scale)', e.g. 'decimal(10, 2)'"
            )
        }
      case "binary"             => BinaryType
      case "byte" | "tinyint"   => ByteType
      case "short" | "smallint" => ShortType
      case other =>
        throw UnsupportedOperationException(
          operation = s"type '$other'",
          details =
            "Supported types: string, int, long, float, double, boolean, date, timestamp, decimal, binary, byte, short"
        )
    }
  }

  /** Validates that the file format is supported
    * @param format
    *   File format to validate
    * @throws UnsupportedFileFormatException
    *   if format is not supported
    */
  private def validateFormat(format: String): Unit = {
    val supportedFormats = Set("csv", "parquet", "json")
    if (!supportedFormats.contains(format.toLowerCase)) {
      throw UnsupportedOperationException(
        operation = s"file format '$format'",
        details = s"Supported formats: ${supportedFormats.mkString(", ")}"
      )
    }
  }
}
