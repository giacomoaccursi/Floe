package com.etl.framework.mapping

import com.etl.framework.exceptions.{DataFrameToDatasetMappingException, MappingExpressionException}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class SimpleBatchModel(id: String, name: String, value: Double)
case class ComplexBatchModel(customerId: String, fullName: String, totalAmount: Double, recordCount: Long)
case class RenamedSimpleModel(customerId: String, fullName: String, totalAmount: Double)

class DataFrameToDatasetMappingTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DataFrameToDatasetMappingTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  private val simpleMapping = MappingConfig(
    mappings = Seq(
      FieldMapping("id", "id", None),
      FieldMapping("name", "name", None),
      FieldMapping("value", "value", None)
    )
  )

  private val simpleDf = Seq(
    ("id1", "Alice", 10.0),
    ("id2", "Bob", 20.5),
    ("id3", "Charlie", 30.75)
  ).toDF("id", "name", "value")

  private val complexDf = Seq(
    ("c1", "John", "Doe", 100.0, 1L),
    ("c2", "Jane", "Smith", 200.0, 2L)
  ).toDF("customer_id", "first_name", "last_name", "amount", "count")

  "DataFrameToDatasetMapping" should "map simple DataFrame to typed Dataset" in {
    val mapper = new BatchModelMapper[SimpleBatchModel](simpleMapping)
    val dataset = mapper.map(simpleDf)

    dataset.count() shouldBe 3
    val records = dataset.collect()
    records.map(_.id).toSet shouldBe Set("id1", "id2", "id3")
    records.map(_.name).toSet shouldBe Set("Alice", "Bob", "Charlie")
  }

  it should "apply SQL expressions during mapping" in {
    val mapping = MappingConfig(
      mappings = Seq(
        FieldMapping("customer_id", "customerId", None),
        FieldMapping("first_name", "fullName", Some("concat(first_name, ' ', last_name)")),
        FieldMapping("amount", "totalAmount", None),
        FieldMapping("count", "recordCount", None)
      )
    )

    val mapper = new BatchModelMapper[ComplexBatchModel](mapping)
    val dataset = mapper.map(complexDf)

    dataset.count() shouldBe 2
    val records = dataset.collect()
    records.map(_.fullName).toSet shouldBe Set("John Doe", "Jane Smith")
  }

  it should "rename columns correctly" in {
    val mapping = MappingConfig(
      mappings = Seq(
        FieldMapping("id", "customerId", None),
        FieldMapping("name", "fullName", None),
        FieldMapping("value", "totalAmount", None)
      )
    )

    val mapper = new BatchModelMapper[RenamedSimpleModel](mapping)
    val dataset = mapper.map(simpleDf)

    dataset.count() shouldBe 3
    dataset.columns.toSet shouldBe Set("customerId", "fullName", "totalAmount")
    val records = dataset.collect()
    records.map(_.customerId).toSet shouldBe Set("id1", "id2", "id3")
  }

  it should "produce empty Dataset from empty DataFrame" in {
    val emptyDf = Seq.empty[(String, String, Double)].toDF("id", "name", "value")
    val mapper = new BatchModelMapper[SimpleBatchModel](simpleMapping)
    val dataset = mapper.map(emptyDf)

    dataset.count() shouldBe 0
  }

  it should "throw exception on schema mismatch" in {
    val wrongDf = Seq(("id1", "name1"), ("id2", "name2")).toDF("id", "name") // missing "value"
    val mapper = new BatchModelMapper[SimpleBatchModel](simpleMapping)

    intercept[Exception] {
      mapper.map(wrongDf)
    }
  }

  it should "throw exception on invalid SQL expression" in {
    val invalidMapping = MappingConfig(
      mappings = Seq(
        FieldMapping("id", "id", None),
        FieldMapping("name", "name", None),
        FieldMapping("value", "value", Some("invalid_function(value)"))
      )
    )

    val mapper = new BatchModelMapper[SimpleBatchModel](invalidMapping)

    intercept[Exception] {
      mapper.map(simpleDf)
    }
  }

  it should "preserve all data values after mapping" in {
    val mapper = new BatchModelMapper[SimpleBatchModel](simpleMapping)
    val dataset = mapper.map(simpleDf)

    val originalData = simpleDf.collect().map { row =>
      (row.getString(0), row.getString(1), row.getDouble(2))
    }.toSet

    val mappedData = dataset.collect().map { model =>
      (model.id, model.name, model.value)
    }.toSet

    originalData shouldBe mappedData
  }
}
