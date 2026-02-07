package com.etl.framework.io.writers

import com.etl.framework.config.FileFormat.Parquet
import com.etl.framework.config.{FileFormat, OutputConfig}
import com.etl.framework.exceptions.DataWriteException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}

case class WriterTestModel(id: Long, name: String, category: String)

class BatchModelWriterTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("BatchModelWriterTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  def createTempDir(): Path = {
    Files.createTempDirectory("batch_writer_test")
  }

  "BatchModelWriter" should "write dataset to parquet" in {
    val tempDir = createTempDir()
    val outputPath = tempDir.resolve("output").toString
    try {
      val outputConfig = OutputConfig(format = FileFormat.Parquet)
      val writer = new BatchModelWriter[WriterTestModel](outputConfig)

      val data = Seq(
        WriterTestModel(1L, "Alice", "A"),
        WriterTestModel(2L, "Bob", "B")
      ).toDS()

      writer.write(data, outputPath)

      // Verify data was written
      val readBack = spark.read.parquet(outputPath)
      readBack.count() shouldBe 2
    } finally {
      deleteRecursively(tempDir)
    }
  }

  it should "write with partitioning" in {
    val tempDir = createTempDir()
    val outputPath = tempDir.resolve("partitioned").toString
    try {
      val outputConfig = OutputConfig(
        format = Parquet,
        partitionBy = Seq("category")
      )
      val writer = new BatchModelWriter[WriterTestModel](outputConfig)

      val data = Seq(
        WriterTestModel(1L, "Alice", "A"),
        WriterTestModel(2L, "Bob", "B"),
        WriterTestModel(3L, "Charlie", "A")
      ).toDS()

      writer.write(data, outputPath)

      // Verify partitioned structure
      val readBack = spark.read.parquet(outputPath)
      readBack.count() shouldBe 3

      // Check partition directories exist
      val outputDir = java.nio.file.Paths.get(outputPath)
      Files.exists(outputDir.resolve("category=A")) shouldBe true
      Files.exists(outputDir.resolve("category=B")) shouldBe true
    } finally {
      deleteRecursively(tempDir)
    }
  }

  it should "write with compression" in {
    val tempDir = createTempDir()
    val outputPath = tempDir.resolve("compressed").toString
    try {
      val outputConfig = OutputConfig(
        format = Parquet,
        compression = "snappy"
      )
      val writer = new BatchModelWriter[WriterTestModel](outputConfig)

      val data = Seq(WriterTestModel(1L, "Alice", "A")).toDS()
      writer.write(data, outputPath)

      val readBack = spark.read.parquet(outputPath)
      readBack.count() shouldBe 1
    } finally {
      deleteRecursively(tempDir)
    }
  }

  it should "apply additional options" in {
    val tempDir = createTempDir()
    val outputPath = tempDir.resolve("with_options").toString
    try {
      val outputConfig = OutputConfig(
        format = Parquet,
        options = Map("mergeSchema" -> "true")
      )
      val writer = new BatchModelWriter[WriterTestModel](outputConfig)

      val data = Seq(WriterTestModel(1L, "Alice", "A")).toDS()
      writer.write(data, outputPath)

      val readBack = spark.read.parquet(outputPath)
      readBack.count() shouldBe 1
    } finally {
      deleteRecursively(tempDir)
    }
  }

  it should "overwrite existing data" in {
    val tempDir = createTempDir()
    val outputPath = tempDir.resolve("overwrite").toString
    try {
      val outputConfig = OutputConfig(format = FileFormat.Parquet)
      val writer = new BatchModelWriter[WriterTestModel](outputConfig)

      // Write first batch
      val data1 = Seq(WriterTestModel(1L, "Alice", "A")).toDS()
      writer.write(data1, outputPath)

      // Write second batch (should overwrite)
      val data2 = Seq(
        WriterTestModel(2L, "Bob", "B"),
        WriterTestModel(3L, "Charlie", "C")
      ).toDS()
      writer.write(data2, outputPath)

      val readBack = spark.read.parquet(outputPath)
      readBack.count() shouldBe 2
    } finally {
      deleteRecursively(tempDir)
    }
  }

  it should "create writer using companion object" in {
    val outputConfig = OutputConfig(format = FileFormat.Parquet)
    val writer = BatchModelWriter[WriterTestModel](outputConfig)
    writer should not be null
  }

  it should "handle empty dataset" in {
    val tempDir = createTempDir()
    val outputPath = tempDir.resolve("empty").toString
    try {
      val outputConfig = OutputConfig(format = FileFormat.Parquet)
      val writer = new BatchModelWriter[WriterTestModel](outputConfig)

      val emptyData = spark.emptyDataset[WriterTestModel]
      writer.write(emptyData, outputPath)

      val readBack = spark.read.parquet(outputPath)
      readBack.count() shouldBe 0
    } finally {
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      if (Files.isDirectory(path)) {
        val stream = Files.list(path)
        try {
          stream.forEach(p => deleteRecursively(p))
        } finally {
          stream.close()
        }
      }
      Files.deleteIfExists(path)
    }
  }
}
