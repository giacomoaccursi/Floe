package com.etl.framework.io.readers

import com.etl.framework.config.{SchemaConfig, SourceConfig, SourceType}
import com.etl.framework.exceptions.UnsupportedOperationException
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataReader {
  def read(): DataFrame
}

object DataReaderFactory {

  type ReaderFactory = (SourceConfig, Option[SchemaConfig], SparkSession) => DataReader

  def create(
      sourceConfig: SourceConfig,
      schemaConfig: Option[SchemaConfig] = None,
      extraReaders: Map[String, ReaderFactory] = Map.empty
  )(implicit spark: SparkSession): DataReader = {
    extraReaders.get(sourceConfig.`type`.name) match {
      case Some(factory) => factory(sourceConfig, schemaConfig, spark)
      case None =>
        sourceConfig.`type` match {
          case SourceType.File => new FileDataReader(sourceConfig, schemaConfig)
          case SourceType.JDBC => new JDBCDataReader(sourceConfig, schemaConfig)
          case unsupported =>
            val extra = if (extraReaders.nonEmpty) s", ${extraReaders.keys.mkString(", ")}" else ""
            throw UnsupportedOperationException(
              operation = s"source type '${unsupported.name}'",
              details = s"Supported types: file, jdbc$extra"
            )
        }
    }
  }
}
