package com.etl.framework.iceberg.catalog

import com.etl.framework.config.{CatalogMode, IcebergConfig}
import org.apache.spark.sql.SparkSession

/** Applies the explicit catalog ownership contract without silently replacing platform configuration. */
object CatalogRuntime {
  private val IcebergExtensions = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

  def prepare(
      spark: SparkSession,
      config: IcebergConfig,
      extraProviders: Map[String, () => CatalogProvider] = Map.empty
  ): Unit = {
    validateExtensions(spark)
    config.catalogMode match {
      case CatalogMode.Existing  => validateExisting(spark, config)
      case CatalogMode.Configure => configure(spark, config, extraProviders)
    }
  }

  private def validateExtensions(spark: SparkSession): Unit = {
    val registered = spark.conf.getOption("spark.sql.extensions").getOrElse("")
    require(
      registered.split(",").map(_.trim).contains(IcebergExtensions),
      s"Iceberg SQL extensions are not registered. Configure spark.sql.extensions=$IcebergExtensions " +
        "before creating the SparkSession"
    )
  }

  private def validateExisting(spark: SparkSession, config: IcebergConfig): Unit = {
    require(config.catalogName != null && config.catalogName.trim.nonEmpty, "catalogName is required")
    val key = s"spark.sql.catalog.${config.catalogName}"
    require(
      spark.conf.getOption(key).exists(_.trim.nonEmpty),
      s"Catalog '${config.catalogName}' is not configured in the supplied SparkSession. " +
        s"Set $key before session creation or opt into catalogMode=configure"
    )
  }

  private def configure(
      spark: SparkSession,
      config: IcebergConfig,
      extraProviders: Map[String, () => CatalogProvider]
  ): Unit =
    CatalogFactory.createCatalogProvider(config.catalogType, extraProviders) match {
      case Left(error) => throw new IllegalArgumentException(error)
      case Right(provider) =>
        provider.validateConfig(config) match {
          case Left(error) => throw new IllegalArgumentException(s"Invalid Iceberg catalog config: $error")
          case Right(_)    => provider.configureCatalog(spark, config)
        }
    }
}
