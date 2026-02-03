package com.etl.framework.io

import com.etl.framework.config.SourceConfig
import com.etl.framework.exceptions.UnsupportedOperationException
import com.etl.framework.TestConfig
import com.etl.framework.io.readers.DataReaderFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.scalacheck.{Arbitrary, Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Path}
import scala.util.Try

/**
 * Property-based tests for data source reading
 * Feature: spark-etl-framework, Property 7: Data Source Reading
 * Validates: Requirements 4.1, 4.2
 * 
 * Test configuration: Uses expensiveTestCount (10 cases) due to file I/O operations
 */
object DataReaderProperties extends Properties("DataReader") {
  
  // Configure test parameters for expensive I/O operations
  override def overrideParameters(p: Parameters): Parameters = TestConfig.expensiveParams
  
  // Create a local Spark session for testing
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("DataReaderPropertiesTest")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  
  // Generator for test data rows
  case class TestRow(id: Int, name: String, value: Double, active: Boolean)
  
  val testRowGen: Gen[TestRow] = for {
    id <- Gen.choose(1, 1000)
    name <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    value <- Gen.choose(0.0, 1000.0)
    active <- Gen.oneOf(true, false)
  } yield TestRow(id, name, value, active)
  
  val testDataGen: Gen[Seq[TestRow]] = for {
    size <- Gen.choose(1, 100)
    rows <- Gen.listOfN(size, testRowGen)
  } yield rows
  
  // Generator for CSV source configurations
  val csvSourceConfigGen: Gen[SourceConfig] = for {
    delimiter <- Gen.oneOf(",", ";", "|", "\t")
    encoding <- Gen.oneOf("UTF-8", "ISO-8859-1")
  } yield SourceConfig(
    `type` = "file",
    path = "", // Will be set dynamically
    format = "csv",
    options = Map(
      "delimiter" -> delimiter,
      "header" -> "true",  // Always use header for testing
      "encoding" -> encoding,
      "inferSchema" -> "true"
    ),
    filePattern = None
  )
  
  // Generator for Parquet source configurations
  val parquetSourceConfigGen: Gen[SourceConfig] = Gen.const(
    SourceConfig(
      `type` = "file",
      path = "", // Will be set dynamically
      format = "parquet",
      options = Map.empty,
      filePattern = None
    )
  )
  
  // Generator for JSON source configurations
  val jsonSourceConfigGen: Gen[SourceConfig] = Gen.const(
    SourceConfig(
      `type` = "file",
      path = "", // Will be set dynamically
      format = "json",
      options = Map.empty,
      filePattern = None
    )
  )
  
  // Combined generator for all file source configurations
  val fileSourceConfigGen: Gen[SourceConfig] = Gen.oneOf(
    csvSourceConfigGen,
    parquetSourceConfigGen,
    jsonSourceConfigGen
  )
  
  // Helper function to write test data to file
  def writeTestDataToFile(data: Seq[TestRow], format: String, path: String, options: Map[String, String]): Unit = {
    import spark.implicits._
    
    val df = data.toDF()
    
    format match {
      case "csv" =>
        // Always write with header=true for testing, so we can verify column names
        val writeOptions = options + ("header" -> "true")
        df.write
          .format("csv")
          .options(writeOptions)
          .mode("overwrite")
          .save(path)
      
      case "parquet" =>
        df.write
          .format("parquet")
          .mode("overwrite")
          .save(path)
      
      case "json" =>
        df.write
          .format("json")
          .mode("overwrite")
          .save(path)
    }
  }
  
  // Helper function to verify DataFrame schema matches expected schema
  def verifySchema(df: DataFrame, expectedColumns: Set[String]): Boolean = {
    val actualColumns = df.columns.toSet
    // Check that all expected columns are present (may have additional columns depending on format)
    expectedColumns.subsetOf(actualColumns)
  }
  
  // Helper function to verify DataFrame has expected row count
  def verifyRowCount(df: DataFrame, expectedCount: Long): Boolean = {
    df.count() == expectedCount
  }
  
  /**
   * Property 7: Data Source Reading for CSV files
   * For any valid CSV source configuration, the Framework should successfully read the data
   * and return a DataFrame with the expected schema
   */
  property("csv_file_reading") = forAll(testDataGen, csvSourceConfigGen) { (data, sourceConfig) =>
    Try {
      // Create temp directory for test data
      val tempDir = Files.createTempDirectory("csv-test-")
      val tempPath = tempDir.toAbsolutePath.toString
      
      try {
        // Write test data to CSV
        writeTestDataToFile(data, "csv", tempPath, sourceConfig.options)
        
        // Update source config with actual path
        val configWithPath = sourceConfig.copy(path = tempPath)
        
        // Create reader and read data
        val reader = DataReaderFactory.create(configWithPath)
        val df = reader.read()
        
        // Verify schema contains expected columns
        val expectedColumns = Set("id", "name", "value", "active")
        val schemaValid = verifySchema(df, expectedColumns)
        
        // Verify row count matches
        val rowCountValid = verifyRowCount(df, data.size)
        
        schemaValid && rowCountValid
      } finally {
        // Clean up temp directory
        deleteDirectory(tempDir.toFile)
      }
    }.getOrElse(false)
  }
  
  /**
   * Property 7: Data Source Reading for Parquet files
   * For any valid Parquet source configuration, the Framework should successfully read the data
   * and return a DataFrame with the expected schema
   */
  property("parquet_file_reading") = forAll(testDataGen, parquetSourceConfigGen) { (data, sourceConfig) =>
    Try {
      // Create temp directory for test data
      val tempDir = Files.createTempDirectory("parquet-test-")
      val tempPath = tempDir.toAbsolutePath.toString
      
      try {
        // Write test data to Parquet
        writeTestDataToFile(data, "parquet", tempPath, sourceConfig.options)
        
        // Update source config with actual path
        val configWithPath = sourceConfig.copy(path = tempPath)
        
        // Create reader and read data
        val reader = DataReaderFactory.create(configWithPath)
        val df = reader.read()
        
        // Verify schema contains expected columns
        val expectedColumns = Set("id", "name", "value", "active")
        val schemaValid = verifySchema(df, expectedColumns)
        
        // Verify row count matches
        val rowCountValid = verifyRowCount(df, data.size)
        
        schemaValid && rowCountValid
      } finally {
        // Clean up temp directory
        deleteDirectory(tempDir.toFile)
      }
    }.getOrElse(false)
  }
  
  /**
   * Property 7: Data Source Reading for JSON files
   * For any valid JSON source configuration, the Framework should successfully read the data
   * and return a DataFrame with the expected schema
   */
  property("json_file_reading") = forAll(testDataGen, jsonSourceConfigGen) { (data, sourceConfig) =>
    Try {
      // Create temp directory for test data
      val tempDir = Files.createTempDirectory("json-test-")
      val tempPath = tempDir.toAbsolutePath.toString
      
      try {
        // Write test data to JSON
        writeTestDataToFile(data, "json", tempPath, sourceConfig.options)
        
        // Update source config with actual path
        val configWithPath = sourceConfig.copy(path = tempPath)
        
        // Create reader and read data
        val reader = DataReaderFactory.create(configWithPath)
        val df = reader.read()
        
        // Verify schema contains expected columns
        val expectedColumns = Set("id", "name", "value", "active")
        val schemaValid = verifySchema(df, expectedColumns)
        
        // Verify row count matches
        val rowCountValid = verifyRowCount(df, data.size)
        
        schemaValid && rowCountValid
      } finally {
        // Clean up temp directory
        deleteDirectory(tempDir.toFile)
      }
    }.getOrElse(false)
  }
  
  /**
   * Property 7: Data Source Reading with file patterns
   * For any valid source configuration with file pattern, the Framework should successfully
   * read matching files and return a DataFrame with the expected schema
   */
  property("file_pattern_reading") = forAll(testDataGen) { data =>
    Try {
      // Create temp directory for test data
      val tempDir = Files.createTempDirectory("pattern-test-")
      val tempPath = tempDir.toAbsolutePath.toString
      
      try {
        // Write test data to multiple CSV files
        import spark.implicits._
        val df = data.toDF()
        
        // Split data into multiple files
        df.repartition(3).write
          .format("csv")
          .option("header", "true")
          .mode("overwrite")
          .save(tempPath)
        
        // Create source config with file pattern
        val sourceConfig = SourceConfig(
          `type` = "file",
          path = tempPath,
          format = "csv",
          options = Map("header" -> "true", "inferSchema" -> "true"),
          filePattern = Some("*.csv")
        )
        
        // Create reader and read data
        val reader = DataReaderFactory.create(sourceConfig)
        val readDf = reader.read()
        
        // Verify schema contains expected columns
        val expectedColumns = Set("id", "name", "value", "active")
        val schemaValid = verifySchema(readDf, expectedColumns)
        
        // Verify row count matches (all files should be read)
        val rowCountValid = verifyRowCount(readDf, data.size)
        
        schemaValid && rowCountValid
      } finally {
        // Clean up temp directory
        deleteDirectory(tempDir.toFile)
      }
    }.getOrElse(false)
  }
  
  /**
   * Property 7: Unsupported source type throws exception
   * For any unsupported source type, the Framework should throw UnsupportedOperationException
   */
  property("unsupported_source_type_error") = forAll(Gen.alphaNumStr.suchThat(s => s.nonEmpty && s != "file" && s != "jdbc")) { unsupportedType =>
    Try {
      val sourceConfig = SourceConfig(
        `type` = unsupportedType,
        path = "/tmp/test",
        format = "csv",
        options = Map.empty,
        filePattern = None
      )
      
      // Should throw UnsupportedOperationException
      try {
        DataReaderFactory.create(sourceConfig)
        false // Should not reach here
      } catch {
        case _: UnsupportedOperationException => true
        case _: Throwable => false
      }
    }.getOrElse(false)
  }
  
  /**
   * Property 7: Unsupported file format throws exception
   * For any unsupported file format, the Framework should throw UnsupportedFileFormatException
   */
  property("unsupported_file_format_error") = forAll(Gen.alphaNumStr.suchThat(s => s.nonEmpty && !Set("csv", "parquet", "json").contains(s.toLowerCase))) { unsupportedFormat =>
    Try {
      val sourceConfig = SourceConfig(
        `type` = "file",
        path = "/tmp/test",
        format = unsupportedFormat,
        options = Map.empty,
        filePattern = None
      )
      
      // Should throw UnsupportedOperationException
      try {
        val reader = DataReaderFactory.create(sourceConfig)
        reader.read()
        false // Should not reach here
      } catch {
        case _: UnsupportedOperationException => true
        case _: Throwable => false
      }
    }.getOrElse(false)
  }
  
  // Helper function to recursively delete directory
  private def deleteDirectory(dir: File): Unit = {
    if (dir.exists()) {
      if (dir.isDirectory) {
        dir.listFiles().foreach(deleteDirectory)
      }
      dir.delete()
    }
  }
}

