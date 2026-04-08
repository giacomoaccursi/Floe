package com.etl.framework.iceberg.catalog

import com.etl.framework.config.IcebergConfig
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HadoopCatalogProviderTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = {
    // Stop any existing session to ensure Iceberg extensions are loaded
    SparkSession.getActiveSession.foreach(_.stop())

    SparkSession
      .builder()
      .appName("HadoopCatalogProviderTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .getOrCreate()
  }

  private val provider = new HadoopCatalogProvider()

  "HadoopCatalogProvider" should "have catalogType hadoop" in {
    provider.catalogType shouldBe "hadoop"
  }

  it should "validate config with non-empty warehouse" in {
    val config = IcebergConfig(warehouse = "/tmp/warehouse")
    provider.validateConfig(config) shouldBe Right(())
  }

  it should "reject config with empty warehouse" in {
    val config = IcebergConfig(warehouse = "")
    val result = provider.validateConfig(config)
    result shouldBe a[Left[_, _]]
    result.left.toOption.get should include("warehouse")
  }

  it should "configure spark session with catalog settings" in {
    val config = IcebergConfig(
      catalogName = "hadoop_provider_test",
      warehouse = "/tmp/test_warehouse"
    )
    provider.configureCatalog(spark, config)

    spark.conf.get("spark.sql.catalog.hadoop_provider_test") shouldBe
      "org.apache.iceberg.spark.SparkCatalog"
    spark.conf.get("spark.sql.catalog.hadoop_provider_test.type") shouldBe
      "hadoop"
    spark.conf.get("spark.sql.catalog.hadoop_provider_test.warehouse") shouldBe
      "/tmp/test_warehouse"
  }

  it should "apply custom catalog properties" in {
    val config = IcebergConfig(
      catalogName = "custom_props_test",
      warehouse = "/tmp/props_warehouse",
      catalogProperties = Map("custom.key" -> "custom.value")
    )
    provider.configureCatalog(spark, config)

    spark.conf.get("spark.sql.catalog.custom_props_test.custom.key") shouldBe
      "custom.value"
  }
}
