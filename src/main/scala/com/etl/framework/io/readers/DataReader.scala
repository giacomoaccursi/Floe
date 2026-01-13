package com.etl.framework.io.readers

import org.apache.spark.sql.DataFrame

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