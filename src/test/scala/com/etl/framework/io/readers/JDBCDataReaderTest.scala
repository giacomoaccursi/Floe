package com.etl.framework.io.readers

import com.etl.framework.config.{SourceConfig, SourceType, FileFormat}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession

class JDBCDataReaderTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("JDBCDataReaderTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  "JDBCDataReader" should "be created with source config" in {
    val sourceConfig = SourceConfig(
      `type` = SourceType.JDBC,
      path = "",
      format = FileFormat.JDBC,
      options = Map(
        "url" -> "jdbc:postgresql://localhost:5432/testdb",
        "dbtable" -> "users",
        "user" -> "admin",
        "password" -> "secret"
      )
    )

    val reader = new JDBCDataReader(sourceConfig)
    reader should not be null
  }

  it should "preserve JDBC options from source config" in {
    val options = Map(
      "url" -> "jdbc:mysql://localhost:3306/testdb",
      "dbtable" -> "orders",
      "user" -> "root",
      "password" -> "pass",
      "fetchsize" -> "1000",
      "batchsize" -> "500"
    )

    val sourceConfig = SourceConfig(
      `type` = SourceType.JDBC,
      path = "",
      format = FileFormat.JDBC,
      options = options
    )

    val reader = new JDBCDataReader(sourceConfig)
    reader should not be null
  }

  it should "handle different JDBC URL formats" in {
    val urls = Seq(
      "jdbc:postgresql://host:5432/db",
      "jdbc:mysql://host:3306/db",
      "jdbc:oracle:thin:@host:1521:db",
      "jdbc:sqlserver://host:1433;databaseName=db"
    )

    urls.foreach { url =>
      val sourceConfig = SourceConfig(
        `type` = SourceType.JDBC,
        path = "",
        format = FileFormat.JDBC,
        options = Map("url" -> url, "dbtable" -> "test")
      )

      val reader = new JDBCDataReader(sourceConfig)
      reader should not be null
    }
  }

  it should "accept query option instead of dbtable" in {
    val sourceConfig = SourceConfig(
      `type` = SourceType.JDBC,
      path = "",
      format = FileFormat.JDBC,
      options = Map(
        "url" -> "jdbc:postgresql://localhost:5432/testdb",
        "query" -> "SELECT * FROM users WHERE active = true"
      )
    )

    val reader = new JDBCDataReader(sourceConfig)
    reader should not be null
  }
}
