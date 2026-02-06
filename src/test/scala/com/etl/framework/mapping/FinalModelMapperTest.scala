package com.etl.framework.mapping

import com.etl.framework.exceptions.FinalModelMapperLoadException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{Dataset, SparkSession}

case class TestBatchModel(id: Long, firstName: String, lastName: String)
case class TestFinalModel(id: Long, fullName: String)

class TestMapper(implicit spark: SparkSession) extends FinalModelMapper[TestBatchModel, TestFinalModel] {
  import spark.implicits._
  override def map(batchModel: Dataset[TestBatchModel]): Dataset[TestFinalModel] = {
    batchModel.map(b => TestFinalModel(b.id, s"${b.firstName} ${b.lastName}"))
  }
}

class FinalModelMapperTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("FinalModelMapperTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  "FinalModelMapper" should "map batch model to final model with custom mapper" in {
    val mapper = new TestMapper()
    val batchData = Seq(
      TestBatchModel(1L, "Alice", "Smith"),
      TestBatchModel(2L, "Bob", "Jones")
    ).toDS()

    val result = mapper.map(batchData)

    result.count() shouldBe 2
    result.filter($"id" === 1L).head().fullName shouldBe "Alice Smith"
    result.filter($"id" === 2L).head().fullName shouldBe "Bob Jones"
  }

  it should "handle empty dataset" in {
    val mapper = new TestMapper()
    val emptyData = spark.emptyDataset[TestBatchModel]

    val result = mapper.map(emptyData)

    result.count() shouldBe 0
  }

  "FinalModelMapper.loadMapper" should "load mapper by class name with SparkSession constructor" in {
    val mapper = FinalModelMapper.loadMapper[TestBatchModel, TestFinalModel](
      "com.etl.framework.mapping.TestMapper"
    )

    mapper should not be null

    val batchData = Seq(TestBatchModel(1L, "Test", "User")).toDS()
    val result = mapper.map(batchData)

    result.count() shouldBe 1
    result.head().fullName shouldBe "Test User"
  }

  it should "throw FinalModelMapperLoadException for non-existent class" in {
    val exception = intercept[FinalModelMapperLoadException] {
      FinalModelMapper.loadMapper[TestBatchModel, TestFinalModel](
        "com.etl.framework.mapping.NonExistentMapper"
      )
    }

    exception.getMessage should include("not found")
  }

  it should "throw FinalModelMapperLoadException for class that is not a mapper" in {
    val exception = intercept[FinalModelMapperLoadException] {
      FinalModelMapper.loadMapper[TestBatchModel, TestFinalModel](
        "java.lang.String"
      )
    }

    exception.getMessage should include("does not implement FinalModelMapper")
  }
}
