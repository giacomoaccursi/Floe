package com.etl.framework.io.readers

import com.etl.framework.config.SourceConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * DataReader implementation for JDBC database sources
 * Supports reading from relational databases via JDBC connections
 */
class JDBCDataReader(sourceConfig: SourceConfig)(implicit spark: SparkSession) extends DataReader {
  
  /**
   * Reads data from JDBC source
   * @return DataFrame containing the database data
   */
  override def read(): DataFrame = {
    spark.read
      .format("jdbc")
      .options(sourceConfig.options)
      .load()
  }
}
