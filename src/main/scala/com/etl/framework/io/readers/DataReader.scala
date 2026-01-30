package com.etl.framework.io.readers

import com.etl.framework.config.SourceConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Trait for reading data from various sources
 */
trait DataReader {
  /**
   * Reads data from the configured source
   * @return DataFrame containing the source data
   */
  def read(): DataFrame
}

/**
 * Factory for creating DataReader instances based on source configuration
 */
object DataReaderFactory {

  /**
   * Creates a DataReader instance based on the source type
   * @param sourceConfig Source configuration
   * @param spark Implicit SparkSession
   * @return DataReader instance
   * @throws UnsupportedSourceTypeException if source type is not supported
   */
  def create(sourceConfig: SourceConfig)(implicit spark: SparkSession): DataReader = {
    sourceConfig.`type` match {
      case "file" => new FileDataReader(sourceConfig)
      case "jdbc" => new JDBCDataReader(sourceConfig)
      case unsupported =>
        throw new UnsupportedSourceTypeException(
          s"Unsupported source type: $unsupported. Supported types: file ('csv', 'parquet', 'json'), jdbc"
        )
    }
  }
}

/**
 * Exception thrown when an unsupported source type is encountered
 */
class UnsupportedSourceTypeException(message: String) extends RuntimeException(message)
