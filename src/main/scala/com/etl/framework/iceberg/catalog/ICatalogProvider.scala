package com.etl.framework.iceberg.catalog

import com.etl.framework.config.IcebergConfig
import org.apache.spark.sql.SparkSession

trait ICatalogProvider {

  def catalogType: String

  def configureCatalog(
      spark: SparkSession,
      config: IcebergConfig
  ): Unit

  def validateConfig(config: IcebergConfig): Either[String, Unit]
}
