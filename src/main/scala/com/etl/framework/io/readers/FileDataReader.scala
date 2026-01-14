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
   *
   * @return DataFrame containing the file data
   */
  override def read(): DataFrame = {

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
}