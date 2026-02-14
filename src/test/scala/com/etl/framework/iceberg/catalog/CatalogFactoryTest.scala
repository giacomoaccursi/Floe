package com.etl.framework.iceberg.catalog

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CatalogFactoryTest extends AnyFlatSpec with Matchers {

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

  it should "list supported types in error message" in {
    val result = CatalogFactory.createCatalogProvider("glue")
    result.left.toOption.get should include("hadoop")
  }

  it should "create new instances on each call" in {
    val result1 = CatalogFactory.createCatalogProvider("hadoop")
    val result2 = CatalogFactory.createCatalogProvider("hadoop")
    result1.toOption.get should not be theSameInstanceAs(result2.toOption.get)
  }
}
