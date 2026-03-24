package com.etl.framework.iceberg.catalog

import com.etl.framework.config.IcebergConfig

object CatalogFactory {

  private val builtinProviders: Map[String, () => CatalogProvider] = Map(
    "hadoop" -> (() => new HadoopCatalogProvider()),
    "glue"   -> (() => new GlueCatalogProvider())
  )

  def createCatalogProvider(
      catalogType: String,
      extraProviders: Map[String, () => CatalogProvider] = Map.empty
  ): Either[String, CatalogProvider] = {
    val providers = builtinProviders ++ extraProviders
    providers
      .get(catalogType)
      .map(factory => Right(factory()))
      .getOrElse(
        Left(
          s"Unsupported catalog type: '$catalogType'. " +
            s"Supported: ${providers.keys.mkString(", ")}"
        )
      )
  }

  /** Returns Spark properties that must be passed to the SparkSession builder
    * before session creation (e.g. spark.sql.extensions).
    */
  def sparkSessionConfig(
      catalogType: String,
      config: IcebergConfig,
      extraProviders: Map[String, () => CatalogProvider] = Map.empty
  ): Either[String, Map[String, String]] = {
    val providers = builtinProviders ++ extraProviders
    providers
      .get(catalogType)
      .map(factory => Right(factory().sparkSessionConfig(config)))
      .getOrElse(
        Left(
          s"Unsupported catalog type: '$catalogType'. " +
            s"Supported: ${providers.keys.mkString(", ")}"
        )
      )
  }
}
