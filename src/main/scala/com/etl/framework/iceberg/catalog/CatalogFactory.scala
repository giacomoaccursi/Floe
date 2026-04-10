package com.etl.framework.iceberg.catalog

object CatalogFactory {

  private val builtinProviders: Map[String, () => CatalogProvider] = Map(
    "hadoop" -> (() => new HadoopCatalogProvider()),
    "glue" -> (() => new GlueCatalogProvider())
  )

  /** Resolves the catalog provider for the given type and returns it, or an error message. */
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
}
