package com.etl.framework.iceberg.catalog

object CatalogFactory {

  private val providerFactories: Map[String, () => ICatalogProvider] = Map(
    "hadoop" -> (() => new HadoopCatalogProvider())
  )

  def createCatalogProvider(
      catalogType: String
  ): Either[String, ICatalogProvider] = {
    providerFactories
      .get(catalogType)
      .map(factory => Right(factory()))
      .getOrElse(
        Left(
          s"Unsupported catalog type: '$catalogType'. " +
            s"Supported: ${providerFactories.keys.mkString(", ")}"
        )
      )
  }
}
