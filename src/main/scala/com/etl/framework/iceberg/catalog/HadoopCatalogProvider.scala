package com.etl.framework.iceberg.catalog

import com.etl.framework.config.IcebergConfig
import org.apache.spark.sql.SparkSession

class HadoopCatalogProvider extends ICatalogProvider {

  private val icebergExtensions =
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

  override def catalogType: String = "hadoop"

  override def sparkSessionConfig(config: IcebergConfig): Map[String, String] =
    Map("spark.sql.extensions" -> icebergExtensions)

  override def configureCatalog(
      spark: SparkSession,
      config: IcebergConfig
  ): Unit = {
    val registeredExtensions =
      spark.conf.getOption("spark.sql.extensions").getOrElse("")
    if (!registeredExtensions.contains(icebergExtensions)) {
      throw new IllegalStateException(
        s"Iceberg SQL extensions not registered. " +
          s"Add 'spark.sql.extensions=$icebergExtensions' before creating the SparkSession: " +
          s"via SparkSession.builder().config(...), spark-submit --conf, or cluster-level config."
      )
    }

    val catalogPrefix = s"spark.sql.catalog.${config.catalogName}"
    spark.conf.set(catalogPrefix, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(s"$catalogPrefix.type", "hadoop")
    spark.conf.set(s"$catalogPrefix.warehouse", config.warehouse)

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
