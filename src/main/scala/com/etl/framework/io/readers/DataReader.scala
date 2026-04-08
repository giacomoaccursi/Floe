package com.etl.framework.io.readers

import com.etl.framework.config.{SchemaConfig, SourceConfig, SourceType}
import com.etl.framework.exceptions.UnsupportedOperationException
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Trait for reading data from various sources
  */
trait DataReader {

  /** Reads data from the configured source
    * @return
    *   DataFrame containing the source data
    */
  def read(): DataFrame
}

/** Factory for creating DataReader instances based on source configuration
  */
object DataReaderFactory {

  /** Creates a DataReader instance based on the source type
    * @param sourceConfig
    *   Source configuration
    * @param schemaConfig
    *   Optional schema configuration for type enforcement
    * @param spark
    *   Implicit SparkSession
    * @return
    *   DataReader instance
    * @throws UnsupportedSourceTypeException
    *   if source type is not supported
    */
  def create(
      sourceConfig: SourceConfig,
      schemaConfig: Option[SchemaConfig] = None
  )(implicit spark: SparkSession): DataReader = {
    sourceConfig.`type` match {
      case SourceType.File => new FileDataReader(sourceConfig, schemaConfig)
      case SourceType.JDBC => new JDBCDataReader(sourceConfig, schemaConfig)
      case unsupported =>
        throw UnsupportedOperationException(
          operation = s"source type '${unsupported.name}'",
          details = "Supported types: file, jdbc"
        )
    }
  }
}
