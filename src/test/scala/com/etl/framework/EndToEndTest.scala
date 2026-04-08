package com.etl.framework

import com.etl.framework.orchestration.{BatchListener, IngestionResult}
import com.etl.framework.pipeline.IngestionPipeline
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.PrintWriter
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable

class EndToEndTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var tempDir: Path = _
  private var warehousePath: String = _

  implicit val spark: SparkSession = {
    val tmp = Files.createTempDirectory("e2e-test")
    tempDir = tmp
    warehousePath = tmp.resolve("warehouse").toString

    SparkSession.getActiveSession.foreach(_.stop())

    SparkSession
      .builder()
      .appName("EndToEndTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", warehousePath)
      .config("spark.sql.defaultCatalog", "spark_catalog")
      .getOrCreate()
  }

  private def writeFile(path: Path, content: String): Unit = {
    Files.createDirectories(path.getParent)
    val pw = new PrintWriter(path.toFile)
    try pw.write(content)
    finally pw.close()
  }

  private def setupConfig(configDir: Path): Unit = {
    writeFile(
      configDir.resolve("global.yaml"),
      s"""
         |paths:
         |  outputPath: "${tempDir.resolve("output")}"
         |  rejectedPath: "${tempDir.resolve("rejected")}"
         |  metadataPath: "${tempDir.resolve("metadata")}"
         |processing:
         |  batchIdFormat: "timestamp"
         |performance:
         |  parallelFlows: false
         |iceberg:
         |  catalogType: "hadoop"
         |  warehouse: "$warehousePath"
         |  enableSnapshotTagging: true
         |""".stripMargin
    )
  }

  private def setupCustomersFlow(configDir: Path, dataDir: Path): Unit = {
    writeFile(
      configDir.resolve("flows").resolve("customers.yaml"),
      s"""
         |name: customers
         |description: "Customer master data"
         |source:
         |  path: "${dataDir.resolve("customers")}"
         |  format: csv
         |  options:
         |    header: "true"
         |schema:
         |  enforceSchema: false
         |loadMode:
         |  type: full
         |validation:
         |  primaryKey: [customer_id]
         |  rules:
         |    - type: regex
         |      column: email
         |      pattern: "^[^@]+@[^@]+\\\\.[^@]+$$"
         |      onFailure: reject
         |""".stripMargin
    )
  }

  private def setupOrdersFlow(configDir: Path, dataDir: Path): Unit = {
    writeFile(
      configDir.resolve("flows").resolve("orders.yaml"),
      s"""
         |name: orders
         |description: "Order data"
         |source:
         |  path: "${dataDir.resolve("orders")}"
         |  format: csv
         |  options:
         |    header: "true"
         |schema:
         |  enforceSchema: false
         |loadMode:
         |  type: full
         |validation:
         |  primaryKey: [order_id]
         |  foreignKeys:
         |    - columns: [customer_id]
         |      references:
         |        flow: customers
         |        columns: [customer_id]
         |      onOrphan: warn
         |""".stripMargin
    )
  }

  private def writeCustomersCsv(dataDir: Path, rows: Seq[(String, String, String)]): Unit = {
    import spark.implicits._
    val df = rows.toDF("customer_id", "name", "email")
    df.write.mode("overwrite").format("csv").option("header", "true").save(dataDir.resolve("customers").toString)
  }

  private def writeOrdersCsv(dataDir: Path, rows: Seq[(String, String, String)]): Unit = {
    import spark.implicits._
    val df = rows.toDF("order_id", "customer_id", "amount")
    df.write.mode("overwrite").format("csv").option("header", "true").save(dataDir.resolve("orders").toString)
  }

  "End-to-end pipeline" should "load, validate, and write to Iceberg" in {
    val configDir = tempDir.resolve("e2e_basic").resolve("config")
    val dataDir = tempDir.resolve("e2e_basic").resolve("data")

    setupConfig(configDir)
    setupCustomersFlow(configDir, dataDir)
    setupOrdersFlow(configDir, dataDir)

    writeCustomersCsv(
      dataDir,
      Seq(
        ("1", "Alice", "alice@test.com"),
        ("2", "Bob", "bob@test.com"),
        ("3", "Charlie", "invalid-email")
      )
    )
    writeOrdersCsv(
      dataDir,
      Seq(
        ("100", "1", "50.00"),
        ("101", "2", "75.00"),
        ("102", "3", "30.00")
      )
    )

    val result = IngestionPipeline
      .builder()
      .withConfigDirectory(configDir.toString)
      .build()
      .execute()

    // Batch should succeed
    result.success shouldBe true
    result.flowResults should have size 2

    // Customers: 3 input, 1 rejected (invalid email), 2 valid
    val custResult = result.flowResults.find(_.flowName == "customers").get
    custResult.success shouldBe true
    custResult.inputRecords shouldBe 3
    custResult.rejectedRecords shouldBe 1
    custResult.validRecords shouldBe 2

    // Orders: 3 input, 1 rejected (FK to customer 3 which was rejected), 2 valid
    val ordResult = result.flowResults.find(_.flowName == "orders").get
    ordResult.success shouldBe true

    // Verify Iceberg tables exist and have data
    val customers = spark.sql("SELECT * FROM spark_catalog.default.customers")
    customers.count() shouldBe 2

    val orders = spark.sql("SELECT * FROM spark_catalog.default.orders")
    orders.count() should be >= 2L

    // Verify metadata JSON was written
    val metadataDir = Paths.get(s"${tempDir.resolve("metadata")}/${result.batchId}")
    Files.exists(metadataDir.resolve("summary.json")) shouldBe true
  }

  it should "detect orphans when parent removes records" in {
    val configDir = tempDir.resolve("e2e_orphan").resolve("config")
    val dataDir = tempDir.resolve("e2e_orphan").resolve("data")

    setupConfig(configDir)
    setupCustomersFlow(configDir, dataDir)
    setupOrdersFlow(configDir, dataDir)

    // Batch 1: all customers present
    writeCustomersCsv(
      dataDir,
      Seq(
        ("1", "Alice", "alice@test.com"),
        ("2", "Bob", "bob@test.com")
      )
    )
    writeOrdersCsv(
      dataDir,
      Seq(
        ("100", "1", "50.00"),
        ("101", "2", "75.00")
      )
    )

    val result1 = IngestionPipeline
      .builder()
      .withConfigDirectory(configDir.toString)
      .build()
      .execute()

    result1.success shouldBe true

    // Batch 2: customer 2 removed from source
    writeCustomersCsv(
      dataDir,
      Seq(
        ("1", "Alice", "alice@test.com")
      )
    )
    writeOrdersCsv(
      dataDir,
      Seq(
        ("100", "1", "50.00"),
        ("101", "1", "75.00")
      )
    )

    val result2 = IngestionPipeline
      .builder()
      .withConfigDirectory(configDir.toString)
      .build()
      .execute()

    result2.success shouldBe true

    // Customers table should have 1 record
    spark.sql("SELECT * FROM spark_catalog.default.customers").count() shouldBe 1
  }

  it should "call batch listeners" in {
    val configDir = tempDir.resolve("e2e_listener").resolve("config")
    val dataDir = tempDir.resolve("e2e_listener").resolve("data")

    setupConfig(configDir)
    setupCustomersFlow(configDir, dataDir)

    writeCustomersCsv(dataDir, Seq(("1", "Alice", "alice@test.com")))

    val completed = mutable.ListBuffer[IngestionResult]()
    val listener = new BatchListener {
      override def onBatchCompleted(result: IngestionResult): Unit = completed += result
      override def onBatchFailed(result: IngestionResult): Unit = ()
    }

    val result = IngestionPipeline
      .builder()
      .withConfigDirectory(configDir.toString)
      .withBatchListener(listener)
      .build()
      .execute()

    result.success shouldBe true
    completed should have size 1
    completed.head.batchId shouldBe result.batchId
  }

  override def afterAll(): Unit = {
    Seq("customers", "orders").foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS spark_catalog.default.$t")
    }
    super.afterAll()
  }
}
