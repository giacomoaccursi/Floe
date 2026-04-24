package com.etl.framework.iceberg.catalog

import com.etl.framework.config.IcebergConfig
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CatalogFactoryTest extends AnyFlatSpec with Matchers {

  private class CustomCatalogProvider extends CatalogProvider {
    override def catalogType: String = "custom"
    override def configureCatalog(spark: SparkSession, config: IcebergConfig): Unit = ()
    override def validateConfig(config: IcebergConfig): Either[String, Unit] = Right(())
  }

  "CatalogFactory" should "create HadoopCatalogProvider for hadoop type" in {
    val result = CatalogFactory.createCatalogProvider("hadoop")
    result shouldBe a[Right[_, _]]
    result.toOption.get shouldBe a[HadoopCatalogProvider]
  }

  it should "return Left for unsupported catalog type" in {
    val result = CatalogFactory.createCatalogProvider("unsupported")
    result shouldBe a[Left[_, _]]
    result.left.toOption.get should include("unsupported")
  }

  it should "create GlueCatalogProvider for glue type" in {
    val result = CatalogFactory.createCatalogProvider("glue")
    result shouldBe a[Right[_, _]]
    result.toOption.get shouldBe a[GlueCatalogProvider]
  }

  it should "list supported types in error message" in {
    val result = CatalogFactory.createCatalogProvider("unsupported")
    val msg = result.left.toOption.get
    msg should include("hadoop")
    msg should include("glue")
  }

  it should "resolve a custom provider registered via extraProviders" in {
    val extra = Map("custom" -> (() => new CustomCatalogProvider()))
    val result = CatalogFactory.createCatalogProvider("custom", extra)
    result shouldBe a[Right[_, _]]
    result.toOption.get shouldBe a[CustomCatalogProvider]
  }

  it should "let a custom provider override a built-in one" in {
    val customHadoop = new CustomCatalogProvider()
    val extra = Map("hadoop" -> (() => customHadoop))
    val result = CatalogFactory.createCatalogProvider("hadoop", extra)
    result.toOption.get shouldBe a[CustomCatalogProvider]
  }

  it should "create new instances on each call" in {
    val result1 = CatalogFactory.createCatalogProvider("hadoop")
    val result2 = CatalogFactory.createCatalogProvider("hadoop")
    result1.toOption.get should not be theSameInstanceAs(result2.toOption.get)
  }

  "GlueCatalogProvider.validateConfig" should "accept config with non-empty catalogName" in {
    val provider = new GlueCatalogProvider()
    val config = IcebergConfig(catalogName = "glue_catalog", warehouse = "s3://bucket/warehouse")
    provider.validateConfig(config) shouldBe Right(())
  }

  it should "reject config with empty catalogName" in {
    val provider = new GlueCatalogProvider()
    val config = IcebergConfig(catalogName = "", warehouse = "s3://bucket/warehouse")
    val result = provider.validateConfig(config)
    result shouldBe a[Left[_, _]]
    result.left.toOption.get should include("catalogName")
  }
}
