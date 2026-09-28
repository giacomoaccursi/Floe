package com.etl.framework

import com.etl.framework.orchestration.{BatchListener, IngestionResult}
import com.etl.framework.orchestration.state.RunStatus
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
      .config("spark.sql.catalog.floe", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.floe.type", "hadoop")
      .config("spark.sql.catalog.floe.warehouse", warehousePath)
      .getOrCreate()
  }

  private def writeFile(path: Path, content: String): Unit = {
    Files.createDirectories(path.getParent)
    val pw = new PrintWriter(path.toFile)
    try pw.write(content)
    finally pw.close()
  }

  private def setupConfig(
      configDir: Path,
      maxRejectionRate: Option[Double] = None,
      maxRetries: Int = 0,
      metadataPath: Option[Path] = None,
      qualityMetricsTable: Option[String] = None
  ): Unit = {
    val thresholdSetting = maxRejectionRate.map(rate => s"  maxRejectionRate: $rate").getOrElse("")
    val retrySetting = if (maxRetries > 0) s"  maxRetries: $maxRetries\n  retryBackoffMs: 1" else ""
    val qualityMetricsSetting = qualityMetricsTable.map(name => s"  qualityMetricsTable: $name").getOrElse("")
    writeFile(
      configDir.resolve("global.yaml"),
      s"""
         |paths:
         |  outputPath: "${tempDir.resolve("output")}"
         |  rejectedPath: "${tempDir.resolve("rejected")}"
         |  metadataPath: "${metadataPath.getOrElse(tempDir.resolve("metadata"))}"
         |processing:
         |  batchIdFormat: "timestamp"
         |$thresholdSetting
         |$retrySetting
         |$qualityMetricsSetting
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
    val customers = spark.sql("SELECT * FROM floe.default.customers")
    customers.count() shouldBe 2

    val orders = spark.sql("SELECT * FROM floe.default.orders")
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
    spark.sql("SELECT * FROM floe.default.customers").count() shouldBe 1
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

  it should "throw BatchFailedException from executeOrThrow on failure" in {
    val configDir = tempDir.resolve("e2e_throw").resolve("config")

    setupConfig(configDir)

    // Flow pointing to non-existent source → will fail
    writeFile(
      configDir.resolve("flows").resolve("bad_flow.yaml"),
      s"""
         |name: bad_flow
         |source:
         |  path: "/nonexistent/path/that/does/not/exist"
         |  format: csv
         |schema:
         |  enforceSchema: false
         |loadMode:
         |  type: full
         |validation:
         |  primaryKey: [id]
         |""".stripMargin
    )

    val ex = intercept[com.etl.framework.exceptions.BatchFailedException] {
      IngestionPipeline
        .builder()
        .withConfigDirectory(configDir.toString)
        .build()
        .executeOrThrow()
    }

    ex.batchId should not be empty
    ex.getMessage should include("bad_flow")
  }

  it should "expose a partial multi-table batch when a dependent flow fails" in {
    val configDir = tempDir.resolve("e2e_partial_batch").resolve("config")
    val dataDir = tempDir.resolve("e2e_partial_batch").resolve("data")
    setupConfig(configDir)
    writeFile(
      configDir.resolve("flows").resolve("partial_parent.yaml"),
      s"""
         |name: partial_parent
         |source:
         |  path: "${dataDir.resolve("parent")}"
         |  format: csv
         |  options:
         |    header: "true"
         |schema:
         |  enforceSchema: false
         |loadMode:
         |  type: full
         |validation:
         |  primaryKey: [id]
         |""".stripMargin
    )
    writeFile(
      configDir.resolve("flows").resolve("partial_child.yaml"),
      s"""
         |name: partial_child
         |dependsOn: [partial_parent]
         |source:
         |  path: "${dataDir.resolve("missing_child")}"
         |  format: csv
         |schema:
         |  enforceSchema: false
         |loadMode:
         |  type: full
         |validation:
         |  primaryKey: [id]
         |""".stripMargin
    )
    import spark.implicits._
    Seq(("1", "Alice"))
      .toDF("id", "name")
      .write
      .mode("overwrite")
      .format("csv")
      .option("header", "true")
      .save(dataDir.resolve("parent").toString)

    val result = IngestionPipeline.builder().withConfigDirectory(configDir.toString).build().execute()

    result.success shouldBe false
    result.flowResults.map(_.flowName) shouldBe Seq("partial_parent", "partial_child")
    result.flowResults.head.icebergMetadata shouldBe defined
    spark.table("floe.default.partial_parent").count() shouldBe 1L
  }

  it should "rename columns using sourceColumn mapping" in {
    val configDir = tempDir.resolve("e2e_rename").resolve("config")
    val dataDir = tempDir.resolve("e2e_rename").resolve("data")

    setupConfig(configDir)

    // Flow with sourceColumn rename: CustID → customer_id
    writeFile(
      configDir.resolve("flows").resolve("renamed_customers.yaml"),
      s"""
         |name: renamed_customers
         |source:
         |  path: "${dataDir.resolve("renamed_customers")}"
         |  format: csv
         |  options:
         |    header: "true"
         |schema:
         |  enforceSchema: true
         |  columns:
         |    - name: customer_id
         |      type: string
         |      nullable: false
         |      sourceColumn: CustID
         |    - name: full_name
         |      type: string
         |      nullable: true
         |      sourceColumn: customer_id
         |loadMode:
         |  type: full
         |validation:
         |  primaryKey: [customer_id]
         |""".stripMargin
    )

    // Write CSV with source column names
    import spark.implicits._
    Seq(("1", "Alice"), ("2", "Bob"))
      .toDF("CustID", "customer_id")
      .write
      .mode("overwrite")
      .format("csv")
      .option("header", "true")
      .save(dataDir.resolve("renamed_customers").toString)

    val result = IngestionPipeline
      .builder()
      .withConfigDirectory(configDir.toString)
      .build()
      .execute()

    result.success shouldBe true

    // Verify columns were renamed in Iceberg
    val df = spark.sql("SELECT * FROM floe.default.renamed_customers")
    df.columns should contain allOf ("customer_id", "full_name")
    df.columns should not contain "CustID"
    df.count() shouldBe 2
  }

  it should "leave a full-load table unchanged when the rejection threshold fails" in {
    val configDir = tempDir.resolve("e2e_threshold").resolve("config")
    val dataDir = tempDir.resolve("e2e_threshold").resolve("data")

    setupConfig(
      configDir,
      maxRejectionRate = Some(0.1),
      qualityMetricsTable = Some("quality_metrics_threshold_failure")
    )
    writeFile(
      configDir.resolve("flows").resolve("threshold_guard.yaml"),
      s"""
         |name: threshold_guard
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
         |      pattern: "^ok$$"
         |      onFailure: reject
         |""".stripMargin
    )

    writeCustomersCsv(dataDir, Seq(("1", "Existing", "ok")))
    val pipeline = IngestionPipeline.builder().withConfigDirectory(configDir.toString).build()
    pipeline.execute().success shouldBe true

    writeCustomersCsv(dataDir, Seq(("2", "New", "ok"), ("3", "Invalid", "bad")))
    val failed = pipeline.execute()

    failed.success shouldBe false
    failed.error.getOrElse("").toLowerCase should include("rejection rate exceeded")
    failed.flowResults.head.rejectedRecords shouldBe 1L
    failed.flowResults.head.mergedRecords shouldBe 0L
    Files.exists(tempDir.resolve("metadata").resolve(failed.batchId).resolve("summary.json")) shouldBe true
    spark
      .sql(
        s"SELECT batch_success FROM floe.default.quality_metrics_threshold_failure " +
          s"WHERE batch_id = '${failed.batchId}' AND flow_name = 'threshold_guard'"
      )
      .head()
      .getBoolean(0) shouldBe false
    val ids = spark
      .sql("SELECT customer_id FROM floe.default.threshold_guard")
      .collect()
      .map(_.getString(0))
      .toSeq
    ids shouldBe Seq("1")
  }

  it should "publish a delta append with warnings after post-commit metadata failure" in {
    val configDir = tempDir.resolve("e2e_post_commit_retry").resolve("config")
    val dataDir = tempDir.resolve("e2e_post_commit_retry").resolve("data")
    val blockedMetadataPath = tempDir.resolve("e2e_post_commit_retry").resolve("blocked-metadata")
    writeFile(blockedMetadataPath, "This is a file, not a directory")
    setupConfig(configDir, maxRetries = 1, metadataPath = Some(blockedMetadataPath))
    writeFile(
      configDir.resolve("flows").resolve("retry_append.yaml"),
      s"""
         |name: retry_append
         |source:
         |  path: "${dataDir.resolve("rows")}"
         |  format: csv
         |  options:
         |    header: "true"
         |schema:
         |  enforceSchema: false
         |loadMode:
         |  type: delta
         |""".stripMargin
    )

    import spark.implicits._
    Seq(("1", "original"))
      .toDF("id", "value")
      .write
      .mode("overwrite")
      .format("csv")
      .option("header", "true")
      .save(dataDir.resolve("rows").toString)

    val result = IngestionPipeline.builder().withConfigDirectory(configDir.toString).build().execute()

    result.success shouldBe true
    result.status shouldBe RunStatus.SucceededWithWarnings
    result.flowResults.head.warnings.mkString(" ") should include("Flow metadata write failed")
    spark.sql("SELECT * FROM floe.default.retry_append").count() shouldBe 1L
  }

  it should "protect a full-load table when input is below minInputRecords" in {
    val configDir = tempDir.resolve("e2e_min_input").resolve("config")
    val dataDir = tempDir.resolve("e2e_min_input").resolve("data")
    setupConfig(configDir)
    writeFile(
      configDir.resolve("flows").resolve("min_input_guard.yaml"),
      s"""
         |name: min_input_guard
         |source:
         |  path: "${dataDir.resolve("input")}"
         |  format: csv
         |  options:
         |    header: "true"
         |schema:
         |  enforceSchema: false
         |loadMode:
         |  type: full
         |validation:
         |  primaryKey: [id]
         |minInputRecords: 1
         |""".stripMargin
    )
    import spark.implicits._
    Seq(("1", "kept"))
      .toDF("id", "value")
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(dataDir.resolve("input").toString)

    val pipeline = IngestionPipeline.builder().withConfigDirectory(configDir.toString).build()
    pipeline.execute().success shouldBe true

    Seq
      .empty[(String, String)]
      .toDF("id", "value")
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(dataDir.resolve("input").toString)
    val failed = pipeline.execute()

    failed.success shouldBe false
    failed.error.getOrElse("") should include("minInputRecords=1")
    spark.table("floe.default.min_input_guard").count() shouldBe 1L
  }

  it should "report the records produced by a post-validation transformation" in {
    val configDir = tempDir.resolve("e2e_post_metrics").resolve("config")
    val dataDir = tempDir.resolve("e2e_post_metrics").resolve("data")
    setupConfig(configDir)
    writeFile(
      configDir.resolve("flows").resolve("post_metrics.yaml"),
      s"""
         |name: post_metrics
         |source:
         |  path: "${dataDir.resolve("input")}"
         |  format: csv
         |  options:
         |    header: "true"
         |schema:
         |  enforceSchema: false
         |loadMode:
         |  type: full
         |validation:
         |  primaryKey: [id]
         |""".stripMargin
    )
    import spark.implicits._
    Seq(("1", "keep"), ("2", "drop"))
      .toDF("id", "action")
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(dataDir.resolve("input").toString)

    val result = IngestionPipeline
      .builder()
      .withConfigDirectory(configDir.toString)
      .withPostValidationTransformation("post_metrics", ctx => ctx.withData(ctx.currentData.filter("action = 'keep'")))
      .build()
      .execute()

    result.success shouldBe true
    result.flowResults.head.inputRecords shouldBe 2L
    result.flowResults.head.validRecords shouldBe 2L
    result.flowResults.head.mergedRecords shouldBe 1L
    spark.table("floe.default.post_metrics").count() shouldBe 1L
  }

  it should "report a failed derived table as a failed batch to listeners and monitoring" in {
    val configDir = tempDir.resolve("e2e_derived_failure").resolve("config")
    val dataDir = tempDir.resolve("e2e_derived_failure").resolve("data")
    setupConfig(configDir, qualityMetricsTable = Some("quality_metrics_derived_failure"))
    setupCustomersFlow(configDir, dataDir)
    writeCustomersCsv(dataDir, Seq(("1", "Alice", "alice@test.com")))

    val completed = mutable.ListBuffer[IngestionResult]()
    val failed = mutable.ListBuffer[IngestionResult]()
    val listener = new BatchListener {
      override def onBatchCompleted(result: IngestionResult): Unit = completed += result
      override def onBatchFailed(result: IngestionResult): Unit = failed += result
    }

    val result = IngestionPipeline
      .builder()
      .withConfigDirectory(configDir.toString)
      .withDerivedTable("broken_derived", _ => throw new IllegalStateException("derived boom"))
      .withBatchListener(listener)
      .build()
      .execute()

    result.success shouldBe false
    result.derivedTableResults should have size 1
    result.derivedTableResults.head.success shouldBe false
    completed shouldBe empty
    failed should have size 1
    failed.head.derivedTableResults should have size 1

    val summary = Files.readString(tempDir.resolve("metadata").resolve(result.batchId).resolve("summary.json"))
    summary should include("\"success\":false")
    val metric = spark
      .sql(
        s"SELECT batch_success FROM floe.default.quality_metrics_derived_failure " +
          s"WHERE batch_id = '${result.batchId}' AND flow_name = 'customers'"
      )
      .first()
    metric.getBoolean(0) shouldBe false
  }

  it should "fail the batch when orphan cleanup cannot inspect the child table" in {
    val configDir = tempDir.resolve("e2e_orphan_failure").resolve("config")
    val dataDir = tempDir.resolve("e2e_orphan_failure").resolve("data")
    setupConfig(configDir, qualityMetricsTable = Some("quality_metrics_orphan_failure"))

    writeFile(
      configDir.resolve("flows").resolve("orphan_parent_fail.yaml"),
      s"""
         |name: orphan_parent_fail
         |source:
         |  path: "${dataDir.resolve("parents")}"
         |  format: csv
         |  options:
         |    header: "true"
         |schema:
         |  enforceSchema: false
         |loadMode:
         |  type: full
         |validation:
         |  primaryKey: [id]
         |""".stripMargin
    )
    writeFile(
      configDir.resolve("flows").resolve("orphan_child_fail.yaml"),
      s"""
         |name: orphan_child_fail
         |source:
         |  path: "${dataDir.resolve("children")}"
         |  format: csv
         |  options:
         |    header: "true"
         |schema:
         |  enforceSchema: false
         |loadMode:
         |  type: full
         |validation:
         |  primaryKey: [child_id]
         |  foreignKeys:
         |    - columns: [parent_id]
         |      references:
         |        flow: orphan_parent_fail
         |        columns: [id]
         |      onOrphan: delete
         |""".stripMargin
    )

    import spark.implicits._
    def writeParents(ids: Seq[Int]): Unit =
      ids
        .toDF("id")
        .write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(dataDir.resolve("parents").toString)
    def writeChildren(): Unit =
      Seq((10, 1), (11, 2))
        .toDF("child_id", "parent_id")
        .write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(dataDir.resolve("children").toString)

    val pipeline = IngestionPipeline
      .builder()
      .withConfigDirectory(configDir.toString)
      .withPostValidationTransformation(
        "orphan_child_fail",
        ctx => ctx.withData(ctx.currentData.drop("parent_id"))
      )
      .build()

    writeParents(Seq(1, 2))
    writeChildren()
    pipeline.execute().success shouldBe true

    writeParents(Seq(1))
    writeChildren()
    val result = pipeline.execute()

    result.success shouldBe false
    result.error.getOrElse("").toLowerCase should include("orphan")
    val summary = Files.readString(tempDir.resolve("metadata").resolve(result.batchId).resolve("summary.json"))
    summary should include("\"success\":false")
    val metric = spark
      .sql(
        s"SELECT batch_success FROM floe.default.quality_metrics_orphan_failure " +
          s"WHERE batch_id = '${result.batchId}' AND flow_name = 'orphan_parent_fail'"
      )
      .first()
    metric.getBoolean(0) shouldBe false
  }

  it should "reject a child FK when the referenced SCD2 key is no longer current" in {
    val configDir = tempDir.resolve("e2e_scd2_fk").resolve("config")
    val dataDir = tempDir.resolve("e2e_scd2_fk").resolve("data")
    setupConfig(configDir)

    writeFile(
      configDir.resolve("flows").resolve("scd2_fk_parent.yaml"),
      s"""
         |name: scd2_fk_parent
         |source:
         |  path: "${dataDir.resolve("parents")}"
         |  format: csv
         |  options:
         |    header: "true"
         |schema:
         |  enforceSchema: false
         |  columns:
         |    - name: id
         |      type: string
         |      nullable: false
         |    - name: name
         |      type: string
         |      nullable: true
         |loadMode:
         |  type: scd2
         |  compareColumns: [name]
         |  detectDeletes: true
         |validation:
         |  primaryKey: [id]
         |""".stripMargin
    )
    writeFile(
      configDir.resolve("flows").resolve("scd2_fk_child.yaml"),
      s"""
         |name: scd2_fk_child
         |source:
         |  path: "${dataDir.resolve("children")}"
         |  format: csv
         |  options:
         |    header: "true"
         |schema:
         |  enforceSchema: false
         |loadMode:
         |  type: full
         |validation:
         |  primaryKey: [child_id]
         |  foreignKeys:
         |    - columns: [parent_id]
         |      references:
         |        flow: scd2_fk_parent
         |        columns: [id]
         |      onOrphan: ignore
         |""".stripMargin
    )

    import spark.implicits._
    def writeParents(rows: Seq[(String, String)]): Unit =
      rows
        .toDF("id", "name")
        .write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(dataDir.resolve("parents").toString)
    def writeChild(): Unit =
      Seq(("10", "1"))
        .toDF("child_id", "parent_id")
        .write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(dataDir.resolve("children").toString)

    val pipeline = IngestionPipeline.builder().withConfigDirectory(configDir.toString).build()
    writeParents(Seq(("1", "Alice"), ("2", "Bob")))
    writeChild()
    pipeline.execute().success shouldBe true

    writeParents(Seq(("2", "Bob")))
    writeChild()
    val result = pipeline.execute()

    result.success shouldBe true
    result.flowResults.find(_.flowName == "scd2_fk_child").get.rejectedRecords shouldBe 1L
    spark.sql("SELECT * FROM floe.default.scd2_fk_child").count() shouldBe 0L
  }

  override def afterAll(): Unit = {
    Seq(
      "customers",
      "orders",
      "renamed_customers",
      "threshold_guard",
      "retry_append",
      "quality_metrics_derived_failure",
      "orphan_parent_fail",
      "orphan_child_fail",
      "quality_metrics_orphan_failure",
      "scd2_fk_parent",
      "scd2_fk_child",
      "partial_parent",
      "partial_child",
      "quality_metrics_threshold_failure",
      "min_input_guard",
      "post_metrics"
    )
      .foreach { t =>
        spark.sql(s"DROP TABLE IF EXISTS floe.default.$t")
      }
    super.afterAll()
  }
}
