package com.etl.framework.io.readers

import com.etl.framework.config.SourceConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * DataReader implementation for file-based sources
 * Supports CSV, Parquet, JSON formats with file pattern matching
 */
class FileDataReader(sourceConfig: SourceConfig)(implicit spark: SparkSession) extends DataReader {

  /**
   * Reads data from file source
   * @return DataFrame containing the file data
   * @throws UnsupportedFileFormatException if file format is not supported
   */
  override def read(): DataFrame = {
    // Validate format
    validateFormat(sourceConfig.format)

    // Create reader with format
    val reader = spark.read
      .format(sourceConfig.format)
      .options(sourceConfig.options)

    // Determine path with optional file pattern
    val fullPath = sourceConfig.filePattern match {
      case Some(pattern) => s"${sourceConfig.path}/$pattern"
      case None => sourceConfig.path
    }

    // Load data
    reader.load(fullPath)
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
