package com.etl.framework.iceberg.catalog

import com.etl.framework.config.IcebergConfig
import org.apache.spark.sql.SparkSession

class HadoopCatalogProvider extends ICatalogProvider {

  override def catalogType: String = "hadoop"

  override def configureCatalog(
      spark: SparkSession,
      config: IcebergConfig
  ): Unit = {
    val catalogPrefix = s"spark.sql.catalog.${config.catalogName}"
    spark.conf.set(
      catalogPrefix,
      "org.apache.iceberg.spark.SparkCatalog"
    )
    spark.conf.set(s"$catalogPrefix.type", "hadoop")
    spark.conf.set(s"$catalogPrefix.warehouse", config.warehouse)
    try {
      spark.conf.set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
    } catch {
      case _: org.apache.spark.sql.AnalysisException =>
      // spark.sql.extensions is a static config, already set at session creation
    }

    config.catalogProperties.foreach { case (key, value) =>
      spark.conf.set(s"$catalogPrefix.$key", value)
    }
  }

  override def validateConfig(
      config: IcebergConfig
  ): Either[String, Unit] = {
    if (config.warehouse.isEmpty) {
      Left("warehouse path is required for hadoop catalog")
    } else {
      Right(())
    }
  }
}
