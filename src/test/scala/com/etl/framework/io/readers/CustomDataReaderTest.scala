package com.etl.framework.io.readers

import com.etl.framework.config.{SchemaConfig, SourceConfig, SourceType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CustomDataReaderTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("CustomDataReaderTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  import spark.implicits._

  "DataReaderFactory" should "use custom reader for registered source type" in {
    val customReader: DataReaderFactory.ReaderFactory = (config, _, spark) => {
      new DataReader {
        override def read(): DataFrame = {
          import spark.implicits._
          Seq(("custom_1", config.path), ("custom_2", config.path)).toDF("id", "source")
        }
      }
    }

    val config = SourceConfig(`type` = SourceType.Custom("myformat"), path = "test/path")
    val reader = DataReaderFactory.create(config, extraReaders = Map("myformat" -> customReader))
    val df = reader.read()

    df.count() shouldBe 2
    df.columns should contain allOf ("id", "source")
    df.collect().head.getAs[String]("source") shouldBe "test/path"
  }

  it should "fall back to built-in readers when no custom match" in {
    val config = SourceConfig(
      `type` = SourceType.File,
      path = "/nonexistent",
      format = Some(com.etl.framework.config.FileFormat.CSV)
    )
    noException should be thrownBy DataReaderFactory.create(
      config,
      extraReaders = Map("myformat" -> ((_, _, _) => null))
    )
  }

  it should "include custom type names in error message for unknown types" in {
    val config = SourceConfig(`type` = SourceType.Custom("unknown"), path = "/test")
    val ex = intercept[com.etl.framework.exceptions.UnsupportedOperationException] {
      DataReaderFactory.create(config, extraReaders = Map("myformat" -> ((_, _, _) => null)))
    }
    ex.getMessage should include("myformat")
  }
}
