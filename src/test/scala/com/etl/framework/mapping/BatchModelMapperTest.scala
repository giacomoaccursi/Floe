package com.etl.framework.mapping

import com.etl.framework.exceptions.{MappingConfigLoadException, MappingExpressionException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Path}

case class SimpleModel(id: Long, name: String)
case class MappedModel(user_id: Long, user_name: String)
case class ExprModel(id: Long, name: String, name_upper: String)

class BatchModelMapperTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("BatchModelMapperTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  "BatchModelMapper" should "map DataFrame with simple column rename" in {
    val df = Seq((1L, "Alice"), (2L, "Bob")).toDF("id", "name")

    val config = MappingConfig(Seq(
      FieldMapping("id", "user_id"),
      FieldMapping("name", "user_name")
    ))

    val mapper = new BatchModelMapper[MappedModel](config)
    val result = mapper.map(df)

    result.count() shouldBe 2
    result.collect().map(_.user_id) should contain allOf (1L, 2L)
    result.collect().map(_.user_name) should contain allOf ("Alice", "Bob")
  }

  it should "map DataFrame with expression" in {
    val df = Seq((1L, "Alice"), (2L, "Bob")).toDF("id", "name")

    val config = MappingConfig(Seq(
      FieldMapping("id", "id"),
      FieldMapping("name", "name"),
      FieldMapping("name", "name_upper", expression = Some("upper(name)"))
    ))

    val mapper = new BatchModelMapper[ExprModel](config)
    val result = mapper.map(df)

    result.count() shouldBe 2
    result.filter($"id" === 1L).select("name_upper").head().getString(0) shouldBe "ALICE"
    result.filter($"id" === 2L).select("name_upper").head().getString(0) shouldBe "BOB"
  }

  it should "skip rename when source field does not exist" in {
    val df = Seq((1L, "Alice")).toDF("id", "name")

    val config = MappingConfig(Seq(
      FieldMapping("nonexistent_field", "target_field"),
      FieldMapping("id", "id"),
      FieldMapping("name", "name")
    ))

    val mapper = new BatchModelMapper[SimpleModel](config)
    val result = mapper.map(df)

    result.count() shouldBe 1
    result.head().id shouldBe 1L
  }

  it should "keep column as-is when source equals target" in {
    val df = Seq((1L, "Alice")).toDF("id", "name")

    val config = MappingConfig(Seq(
      FieldMapping("id", "id"),
      FieldMapping("name", "name")
    ))

    val mapper = new BatchModelMapper[SimpleModel](config)
    val result = mapper.map(df)

    result.count() shouldBe 1
    result.head().id shouldBe 1L
    result.head().name shouldBe "Alice"
  }

  it should "throw MappingExpressionException for invalid expression" in {
    val df = Seq((1L, "Alice")).toDF("id", "name")

    val config = MappingConfig(Seq(
      FieldMapping("id", "bad_field", expression = Some("invalid_function(name)"))
    ))

    val mapper = new BatchModelMapper[SimpleModel](config)

    intercept[MappingExpressionException] {
      mapper.map(df)
    }
  }

  it should "handle empty mapping config" in {
    val df = Seq((1L, "Alice")).toDF("id", "name")

    val config = MappingConfig(Seq.empty)

    val mapper = new BatchModelMapper[SimpleModel](config)
    val result = mapper.map(df)

    result.count() shouldBe 1
    result.head().id shouldBe 1L
  }

  it should "create mapper from inline config" in {
    val config = MappingConfig(Seq(
      FieldMapping("id", "id"),
      FieldMapping("name", "name")
    ))

    val mapper = BatchModelMapper.fromConfig[SimpleModel](config)
    val df = Seq((1L, "Alice")).toDF("id", "name")
    val result = mapper.map(df)

    result.count() shouldBe 1
  }

  "BatchModelMapper.loadMappingConfig" should "load valid YAML mapping file" in {
    val tempFile = createTempYaml(
      """mappings:
        |  - sourceField: id
        |    targetField: user_id
        |  - sourceField: name
        |    targetField: user_name
        |    expression: "upper(name)"
        |""".stripMargin
    )

    try {
      val config = BatchModelMapper.loadMappingConfig(tempFile.toString)

      config.mappings should have size 2
      config.mappings.head.sourceField shouldBe "id"
      config.mappings.head.targetField shouldBe "user_id"
      config.mappings(1).expression shouldBe Some("upper(name)")
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  it should "throw MappingConfigLoadException for non-existent file" in {
    intercept[MappingConfigLoadException] {
      BatchModelMapper.loadMappingConfig("/tmp/nonexistent_mapping_file_12345.yaml")
    }
  }

  it should "throw MappingConfigLoadException for malformed YAML" in {
    val tempFile = createTempYaml("invalid: yaml: content: [[[")

    try {
      intercept[MappingConfigLoadException] {
        BatchModelMapper.loadMappingConfig(tempFile.toString)
      }
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  private def createTempYaml(content: String): Path = {
    val tempFile = Files.createTempFile("mapping_test_", ".yaml")
    Files.write(tempFile, content.getBytes)
    tempFile
  }
}
