package com.etl.framework.io

import com.etl.framework.config.{SourceConfig, SourceType, FileFormat}
import com.etl.framework.io.readers.DataReaderFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

case class DataReaderTestRow(id: Int, name: String, value: Double, active: Boolean)

class DataReaderTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DataReaderTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  import spark.implicits._

  private val testData = Seq(
    DataReaderTestRow(1, "Alice", 100.0, true),
    DataReaderTestRow(2, "Bob", 200.5, false),
    DataReaderTestRow(3, "Charlie", 300.75, true)
  )

  private val expectedColumns = Set("id", "name", "value", "active")

  private def deleteDirectory(dir: File): Unit = {
    if (dir.exists()) {
      if (dir.isDirectory) dir.listFiles().foreach(deleteDirectory)
      dir.delete()
    }
  }

  private def writeTestData(format: FileFormat, path: String, options: Map[String, String] = Map.empty): Unit = {
    val df = testData.toDF()
    format match {
      case FileFormat.CSV =>
        df.write.format("csv").options(options + ("header" -> "true")).mode("overwrite").save(path)
      case FileFormat.Parquet =>
        df.write.format("parquet").mode("overwrite").save(path)
      case FileFormat.JSON =>
        df.write.format("json").mode("overwrite").save(path)
      case _ =>
    }
  }

  "DataReader" should "read CSV files with comma delimiter" in {
    val tempDir = Files.createTempDirectory("csv-comma-test-")
    val tempPath = tempDir.toAbsolutePath.toString
    try {
      writeTestData(FileFormat.CSV, tempPath, Map("delimiter" -> ","))

      val config = SourceConfig(
        SourceType.File,
        tempPath,
        FileFormat.CSV,
        Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true")
      )
      val reader = DataReaderFactory.create(config)
      val df = reader.read()

      expectedColumns.subsetOf(df.columns.toSet) shouldBe true
      df.count() shouldBe testData.size
    } finally {
      deleteDirectory(tempDir.toFile)
    }
  }

  it should "read CSV files with semicolon delimiter" in {
    val tempDir = Files.createTempDirectory("csv-semicolon-test-")
    val tempPath = tempDir.toAbsolutePath.toString
    try {
      writeTestData(FileFormat.CSV, tempPath, Map("delimiter" -> ";"))

      val config = SourceConfig(
        SourceType.File,
        tempPath,
        FileFormat.CSV,
        Map("delimiter" -> ";", "header" -> "true", "inferSchema" -> "true")
      )
      val reader = DataReaderFactory.create(config)
      val df = reader.read()

      expectedColumns.subsetOf(df.columns.toSet) shouldBe true
      df.count() shouldBe testData.size
    } finally {
      deleteDirectory(tempDir.toFile)
    }
  }

  it should "read Parquet files" in {
    val tempDir = Files.createTempDirectory("parquet-test-")
    val tempPath = tempDir.toAbsolutePath.toString
    try {
      writeTestData(FileFormat.Parquet, tempPath)

      val config = SourceConfig(SourceType.File, tempPath, FileFormat.Parquet, Map.empty)
      val reader = DataReaderFactory.create(config)
      val df = reader.read()

      expectedColumns.subsetOf(df.columns.toSet) shouldBe true
      df.count() shouldBe testData.size
    } finally {
      deleteDirectory(tempDir.toFile)
    }
  }

  it should "read JSON files" in {
    val tempDir = Files.createTempDirectory("json-test-")
    val tempPath = tempDir.toAbsolutePath.toString
    try {
      writeTestData(FileFormat.JSON, tempPath)

      val config = SourceConfig(SourceType.File, tempPath, FileFormat.JSON, Map.empty)
      val reader = DataReaderFactory.create(config)
      val df = reader.read()

      expectedColumns.subsetOf(df.columns.toSet) shouldBe true
      df.count() shouldBe testData.size
    } finally {
      deleteDirectory(tempDir.toFile)
    }
  }

  it should "read files matching a file pattern" in {
    val tempDir = Files.createTempDirectory("pattern-test-")
    val tempPath = tempDir.toAbsolutePath.toString
    try {
      val df = testData.toDF()
      df.repartition(3)
        .write
        .format("csv")
        .option("header", "true")
        .mode("overwrite")
        .save(tempPath)

      val config = SourceConfig(
        SourceType.File,
        s"$tempPath/*.csv",
        FileFormat.CSV,
        Map("header" -> "true", "inferSchema" -> "true")
      )
      val reader = DataReaderFactory.create(config)
      val readDf = reader.read()

      expectedColumns.subsetOf(readDf.columns.toSet) shouldBe true
      readDf.count() shouldBe testData.size
    } finally {
      deleteDirectory(tempDir.toFile)
    }
  }
}
