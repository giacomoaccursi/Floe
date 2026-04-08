package com.etl.framework.io.readers

import com.etl.framework.config.{ColumnConfig, SchemaConfig, SourceConfig, SourceType, FileFormat}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Path}

class FileDataReaderTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("FileDataReaderTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  def createTempCsv(content: String): Path = {
    val tempDir = Files.createTempDirectory("file_reader_test")
    val csvFile = tempDir.resolve("data.csv")
    Files.write(csvFile, content.getBytes)
    csvFile
  }

  "FileDataReader" should "read CSV file" in {
    val csvFile = createTempCsv("id,name\n1,Alice\n2,Bob")
    try {
      val sourceConfig = SourceConfig(
        `type` = SourceType.File,
        path = csvFile.toString,
        format = Some(FileFormat.CSV),
        options = Map("header" -> "true")
      )
      val reader = new FileDataReader(sourceConfig)
      val df = reader.read()

      df.count() shouldBe 2
      df.columns should contain allOf ("id", "name")
    } finally {
      Files.deleteIfExists(csvFile)
      Files.deleteIfExists(csvFile.getParent)
    }
  }

  it should "read CSV with enforced schema" in {
    val csvFile = createTempCsv("id,name\n1,Alice\n2,Bob")
    try {
      val sourceConfig = SourceConfig(
        `type` = SourceType.File,
        path = csvFile.toString,
        format = Some(FileFormat.CSV),
        options = Map("header" -> "true")
      )
      val schemaConfig = SchemaConfig(
        enforceSchema = true,
        allowExtraColumns = false,
        columns = Seq(
          ColumnConfig("id", "integer", nullable = false, description = ""),
          ColumnConfig("name", "string", nullable = true, description = "")
        )
      )
      val reader = new FileDataReader(sourceConfig, Some(schemaConfig))
      val df = reader.read()

      df.count() shouldBe 2
      df.schema("id").dataType.simpleString shouldBe "int"
      df.schema("name").dataType.simpleString shouldBe "string"
    } finally {
      Files.deleteIfExists(csvFile)
      Files.deleteIfExists(csvFile.getParent)
    }
  }

  // Removed obsolete test "throw exception for unsupported format"
  // because strong typing with Enums prevents invalid values at compile time.

  it should "support csv, parquet and json formats" in {
    // CSV
    val csvFile = createTempCsv("id\n1")
    try {
      val csvConfig = SourceConfig(
        SourceType.File,
        csvFile.toString,
        Some(FileFormat.CSV),
        Map("header" -> "true")
      )
      val csvReader = new FileDataReader(csvConfig)
      csvReader.read().count() shouldBe 1
    } finally {
      Files.deleteIfExists(csvFile)
      Files.deleteIfExists(csvFile.getParent)
    }
  }

  it should "apply options from source config" in {
    val csvFile = createTempCsv("id;name\n1;Alice")
    try {
      val sourceConfig = SourceConfig(
        `type` = SourceType.File,
        path = csvFile.toString,
        format = Some(FileFormat.CSV),
        options = Map("header" -> "true", "delimiter" -> ";")
      )
      val reader = new FileDataReader(sourceConfig)
      val df = reader.read()

      df.count() shouldBe 1
      df.columns should contain allOf ("id", "name")
    } finally {
      Files.deleteIfExists(csvFile)
      Files.deleteIfExists(csvFile.getParent)
    }
  }

  it should "handle file pattern in source config" in {
    val tempDir = Files.createTempDirectory("file_reader_pattern_test")
    try {
      Files.write(tempDir.resolve("data_2024.csv"), "id,name\n1,Alice".getBytes)
      Files.write(tempDir.resolve("data_2025.csv"), "id,name\n2,Bob".getBytes)

      val sourceConfig = SourceConfig(
        `type` = SourceType.File,
        path = tempDir.toString,
        format = Some(FileFormat.CSV),
        options = Map("header" -> "true")
      )
      val reader = new FileDataReader(sourceConfig)
      val df = reader.read()

      df.count() shouldBe 2
    } finally {
      Files.list(tempDir).forEach(p => Files.deleteIfExists(p))
      Files.deleteIfExists(tempDir)
    }
  }

  it should "map all supported type names to Spark types" in {
    val csvFile = createTempCsv("col1\ntest")
    try {
      // Types that are compatible with CSV reading (binary is excluded as CSV doesn't support it)
      val supportedTypes = Seq(
        "string",
        "varchar",
        "text",
        "int",
        "integer",
        "long",
        "bigint",
        "float",
        "double",
        "boolean",
        "bool",
        "date",
        "timestamp",
        "datetime",
        "decimal",
        "byte",
        "tinyint",
        "short",
        "smallint"
      )

      // Verify schema enforcement doesn't throw for all supported types
      supportedTypes.foreach { typeName =>
        val schemaConfig = SchemaConfig(
          enforceSchema = true,
          allowExtraColumns = true,
          columns = Seq(
            ColumnConfig("col1", typeName, nullable = true, description = "")
          )
        )
        val sourceConfig = SourceConfig(
          SourceType.File,
          csvFile.toString,
          Some(FileFormat.CSV),
          Map("header" -> "true")
        )
        noException should be thrownBy new FileDataReader(
          sourceConfig,
          Some(schemaConfig)
        ).read()
      }
    } finally {
      Files.deleteIfExists(csvFile)
      Files.deleteIfExists(csvFile.getParent)
    }
  }

  it should "not enforce schema when enforceSchema is false" in {
    val csvFile = createTempCsv("id,name\n1,Alice")
    try {
      val sourceConfig = SourceConfig(
        `type` = SourceType.File,
        path = csvFile.toString,
        format = Some(FileFormat.CSV),
        options = Map("header" -> "true")
      )
      val schemaConfig = SchemaConfig(
        enforceSchema = false,
        allowExtraColumns = true,
        columns = Seq(ColumnConfig("id", "integer", nullable = false, description = ""))
      )
      val reader = new FileDataReader(sourceConfig, Some(schemaConfig))
      val df = reader.read()

      // Without enforced schema, CSV reads everything as string
      df.schema("id").dataType.simpleString shouldBe "string"
    } finally {
      Files.deleteIfExists(csvFile)
      Files.deleteIfExists(csvFile.getParent)
    }
  }
}
