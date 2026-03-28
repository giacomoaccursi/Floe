package com.etl.framework.iceberg.catalog

import com.etl.framework.config.IcebergConfig
import org.apache.spark.sql.SparkSession

trait CatalogProvider {

  def catalogType: String

  /** Returns Spark properties that must be set on the SparkSession builder, before the session is created (e.g.
    * spark.sql.extensions).
    */
  def sparkSessionConfig(config: IcebergConfig): Map[String, String]

  def configureCatalog(
      spark: SparkSession,
      config: IcebergConfig
  ): Unit

  def validateConfig(config: IcebergConfig): Either[String, Unit]
}
