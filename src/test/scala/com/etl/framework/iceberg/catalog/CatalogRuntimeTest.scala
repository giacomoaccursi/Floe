package com.etl.framework.iceberg.catalog

import com.etl.framework.config.{CatalogMode, IcebergConfig}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CatalogRuntimeTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val spark = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession
      .builder()
      .appName("CatalogRuntimeTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  "CatalogRuntime" should "validate an existing catalog without overwriting platform settings" in {
    val prefix = "spark.sql.catalog.platform_catalog"
    spark.conf.set(prefix, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(s"$prefix.type", "hadoop")
    spark.conf.set(s"$prefix.warehouse", "/platform/warehouse")

    CatalogRuntime.prepare(
      spark,
      IcebergConfig(
        catalogMode = CatalogMode.Existing,
        catalogName = "platform_catalog",
        warehouse = "/ignored/by/existing-mode",
        catalogProperties = Map("type" -> "glue")
      )
    )

    spark.conf.get(s"$prefix.type") shouldBe "hadoop"
    spark.conf.get(s"$prefix.warehouse") shouldBe "/platform/warehouse"
  }

  it should "reject an existing catalog that the platform did not configure" in {
    val error = intercept[IllegalArgumentException] {
      CatalogRuntime.prepare(
        spark,
        IcebergConfig(catalogMode = CatalogMode.Existing, catalogName = "missing_catalog")
      )
    }

    error.getMessage should include("missing_catalog")
    error.getMessage should include("not configured")
  }

  it should "invoke a provider only when configuration is explicitly requested" in {
    var configured = false
    val provider = new CatalogProvider {
      override val catalogType: String = "recording"
      override def validateConfig(config: IcebergConfig): Either[String, Unit] = Right(())
      override def configureCatalog(spark: SparkSession, config: IcebergConfig): Unit = configured = true
    }

    CatalogRuntime.prepare(
      spark,
      IcebergConfig(catalogMode = CatalogMode.Configure, catalogType = "recording", warehouse = "/tmp/warehouse"),
      Map("recording" -> (() => provider))
    )

    configured shouldBe true
  }
}
