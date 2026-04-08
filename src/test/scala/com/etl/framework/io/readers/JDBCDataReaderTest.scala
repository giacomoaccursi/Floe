package com.etl.framework.io.readers

import com.etl.framework.config.{SourceConfig, SourceType}
import com.etl.framework.exceptions.DataSourceException
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.DriverManager

class JDBCDataReaderTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val h2Url = "jdbc:h2:mem:jdbc_reader_test;DB_CLOSE_DELAY=-1"

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("JDBCDataReaderTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        """CREATE TABLE customers (
          |  id INT PRIMARY KEY,
          |  name VARCHAR(100),
          |  email VARCHAR(200),
          |  active BOOLEAN
          |)""".stripMargin
      )
      stmt.execute("INSERT INTO customers VALUES (1, 'Alice', 'alice@test.com', true)")
      stmt.execute("INSERT INTO customers VALUES (2, 'Bob', 'bob@test.com', false)")
      stmt.execute("INSERT INTO customers VALUES (3, 'Charlie', 'charlie@test.com', true)")

      stmt.execute(
        """CREATE TABLE orders (
          |  id INT PRIMARY KEY,
          |  customer_id INT,
          |  amount DECIMAL(10,2)
          |)""".stripMargin
      )
      stmt.execute("INSERT INTO orders VALUES (100, 1, 50.00)")
      stmt.execute("INSERT INTO orders VALUES (101, 2, 75.50)")
      stmt.execute("INSERT INTO orders VALUES (102, 1, 120.00)")
      stmt.execute("INSERT INTO orders VALUES (103, 3, 30.25)")
    } finally {
      conn.close()
    }
  }

  "JDBCDataReader" should "read all records from a table" in {
    val config = SourceConfig(
      `type` = SourceType.JDBC,
      path = "customers",
      options = Map("url" -> h2Url, "user" -> "sa", "password" -> "")
    )
    val reader = new JDBCDataReader(config)
    val df = reader.read()

    df.count() shouldBe 3
    df.columns should contain allOf ("ID", "NAME", "EMAIL", "ACTIVE")
  }

  it should "read from a custom query" in {
    val config = SourceConfig(
      `type` = SourceType.JDBC,
      path = "customers",
      options = Map(
        "url" -> h2Url,
        "user" -> "sa",
        "password" -> "",
        "query" -> "SELECT id, name FROM customers WHERE active = true"
      )
    )
    val reader = new JDBCDataReader(config)
    val df = reader.read()

    df.count() shouldBe 2
    df.columns.map(_.toUpperCase) should contain allOf ("ID", "NAME")
    df.columns.map(_.toUpperCase) should not contain "EMAIL"
  }

  it should "pass through JDBC options like fetchSize" in {
    val config = SourceConfig(
      `type` = SourceType.JDBC,
      path = "customers",
      options = Map(
        "url" -> h2Url,
        "user" -> "sa",
        "password" -> "",
        "fetchSize" -> "100"
      )
    )
    val reader = new JDBCDataReader(config)
    val df = reader.read()

    df.count() shouldBe 3
  }

  it should "read from a different table" in {
    val config = SourceConfig(
      `type` = SourceType.JDBC,
      path = "orders",
      options = Map("url" -> h2Url, "user" -> "sa", "password" -> "")
    )
    val reader = new JDBCDataReader(config)
    val df = reader.read()

    df.count() shouldBe 4
    df.columns.map(_.toUpperCase) should contain allOf ("ID", "CUSTOMER_ID", "AMOUNT")
  }

  it should "throw DataSourceException when url is missing" in {
    val config = SourceConfig(
      `type` = SourceType.JDBC,
      path = "customers",
      options = Map("user" -> "sa")
    )
    val reader = new JDBCDataReader(config)

    a[DataSourceException] should be thrownBy reader.read()
  }

  it should "be created by DataReaderFactory for JDBC source type" in {
    val config = SourceConfig(
      `type` = SourceType.JDBC,
      path = "customers",
      options = Map("url" -> h2Url, "user" -> "sa", "password" -> "")
    )
    val reader = DataReaderFactory.create(config)

    reader shouldBe a[JDBCDataReader]
    reader.read().count() shouldBe 3
  }

  override def afterAll(): Unit = {
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    try {
      val stmt = conn.createStatement()
      stmt.execute("DROP TABLE IF EXISTS customers")
      stmt.execute("DROP TABLE IF EXISTS orders")
    } finally {
      conn.close()
    }
    super.afterAll()
  }
}
