package com.etl.framework.pipeline

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.PrintWriter
import java.nio.file.{Files, Path}

class ValidateConfigTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("ValidateConfigTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  private def writeFile(path: Path, content: String): Unit = {
    Files.createDirectories(path.getParent)
    val pw = new PrintWriter(path.toFile)
    try pw.write(content)
    finally pw.close()
  }

  private def setupValidConfig(dir: Path): Unit = {
    writeFile(
      dir.resolve("global.yaml"),
      """
        |paths:
        |  outputPath: "/tmp/out"
        |  rejectedPath: "/tmp/rej"
        |  metadataPath: "/tmp/meta"
        |performance:
        |  parallelFlows: false
        |iceberg:
        |  warehouse: "/tmp/wh"
        |""".stripMargin
    )

    writeFile(
      dir.resolve("flows/customers.yaml"),
      """
        |name: customers
        |source:
        |  path: "/data/customers"
        |  format: csv
        |schema:
        |  enforceSchema: false
        |loadMode:
        |  type: full
        |validation:
        |  primaryKey: [id]
        |""".stripMargin
    )

    writeFile(
      dir.resolve("flows/orders.yaml"),
      """
        |name: orders
        |source:
        |  path: "/data/orders"
        |  format: csv
        |schema:
        |  enforceSchema: false
        |loadMode:
        |  type: full
        |validation:
        |  primaryKey: [id]
        |  foreignKeys:
        |    - columns: [customer_id]
        |      references:
        |        flow: customers
        |        columns: [id]
        |""".stripMargin
    )
  }

  "validate" should "return empty for valid config" in {
    val dir = Files.createTempDirectory("validate-ok")
    setupValidConfig(dir)

    val issues = IngestionPipeline.builder().withConfigDirectory(dir.toString).validate()
    issues shouldBe empty
  }

  it should "detect FK reference to unknown flow" in {
    val dir = Files.createTempDirectory("validate-bad-fk")
    writeFile(
      dir.resolve("global.yaml"),
      """
        |paths:
        |  outputPath: "/tmp/out"
        |  rejectedPath: "/tmp/rej"
        |  metadataPath: "/tmp/meta"
        |performance:
        |  parallelFlows: false
        |iceberg:
        |  warehouse: "/tmp/wh"
        |""".stripMargin
    )

    writeFile(
      dir.resolve("flows/orders.yaml"),
      """
        |name: orders
        |source:
        |  path: "/data/orders"
        |  format: csv
        |schema:
        |  enforceSchema: false
        |loadMode:
        |  type: full
        |validation:
        |  primaryKey: [id]
        |  foreignKeys:
        |    - columns: [customer_id]
        |      references:
        |        flow: nonexistent
        |        columns: [id]
        |""".stripMargin
    )

    val issues = IngestionPipeline.builder().withConfigDirectory(dir.toString).validate()
    issues should have size 1
    issues.head should include("nonexistent")

    val buildError = intercept[IllegalArgumentException] {
      IngestionPipeline.builder().withConfigDirectory(dir.toString).build()
    }
    buildError.getMessage should include("nonexistent")
  }

  it should "detect dependsOn reference to unknown flow" in {
    val dir = Files.createTempDirectory("validate-bad-dep")
    writeFile(
      dir.resolve("global.yaml"),
      """
        |paths:
        |  outputPath: "/tmp/out"
        |  rejectedPath: "/tmp/rej"
        |  metadataPath: "/tmp/meta"
        |performance:
        |  parallelFlows: false
        |iceberg:
        |  warehouse: "/tmp/wh"
        |""".stripMargin
    )

    writeFile(
      dir.resolve("flows/orders.yaml"),
      """
        |name: orders
        |source:
        |  path: "/data/orders"
        |  format: csv
        |schema:
        |  enforceSchema: false
        |loadMode:
        |  type: full
        |validation:
        |  primaryKey: [id]
        |dependsOn: [missing_flow]
        |""".stripMargin
    )

    val issues = IngestionPipeline.builder().withConfigDirectory(dir.toString).validate()
    issues should have size 1
    issues.head should include("missing_flow")

    val buildError = intercept[IllegalArgumentException] {
      IngestionPipeline.builder().withConfigDirectory(dir.toString).build()
    }
    buildError.getMessage should include("missing_flow")
  }

  it should "detect invalid YAML" in {
    val dir = Files.createTempDirectory("validate-bad-yaml")
    writeFile(dir.resolve("global.yaml"), "invalid: [yaml: broken")

    val issues = IngestionPipeline.builder().withConfigDirectory(dir.toString).validate()
    issues should not be empty
  }

  it should "work with programmatic config" in {
    val global = TestFixtures.globalConfig()
    val flows = Seq(
      TestFixtures.flowConfig("a"),
      TestFixtures.flowConfig(
        "b",
        foreignKeys = Seq(
          ForeignKeyConfig(Seq("a_id"), ReferenceConfig("a", Seq("id")))
        )
      )
    )

    val issues = IngestionPipeline
      .builder()
      .withGlobalConfig(global)
      .withFlowConfigs(flows)
      .validate()

    issues shouldBe empty
  }

  it should "reject unsafe maintenance settings during static validation" in {
    val maintenance = MaintenanceConfig(
      enabled = true,
      snapshotRetentionDays = Some(0),
      targetFileSizeMb = Some(0),
      orphanRetentionMinutes = Some(1439),
      enableManifestRewrite = false
    )
    val global = TestFixtures.globalConfig(
      iceberg = IcebergConfig(ddlMode = DdlMode.Automatic, warehouse = "/tmp/test/warehouse", maintenance = maintenance)
    )
    val builder = IngestionPipeline
      .builder()
      .withGlobalConfig(global)
      .withFlowConfigs(Seq(TestFixtures.flowConfig("orders")))

    val issues = builder.validate().mkString(" ")
    issues should include("snapshotRetentionDays")
    issues should include("targetFileSizeMb")
    issues should include("orphanRetentionMinutes")
    intercept[IllegalArgumentException](builder.build()).getMessage should include("Iceberg maintenance")
  }

  it should "reject an invalid programmatic FK during build" in {
    val parent = TestFixtures.flowConfig("parent")
    val child = TestFixtures.flowConfig(
      "child",
      foreignKeys = Seq(
        ForeignKeyConfig(Seq("parent_id", "tenant_id"), ReferenceConfig("parent", Seq("id")))
      )
    )

    val error = intercept[IllegalArgumentException] {
      IngestionPipeline
        .builder()
        .withGlobalConfig(TestFixtures.globalConfig())
        .withFlowConfigs(Seq(parent, child))
        .build()
    }

    error.getMessage should include("different local/reference arity")
  }

  it should "reject destructive orphan deletion before building a pipeline" in {
    val parent = TestFixtures.flowConfig("parent")
    val child = TestFixtures.flowConfig(
      "child",
      foreignKeys = Seq(
        ForeignKeyConfig(
          Seq("parent_id"),
          ReferenceConfig("parent", Seq("id")),
          onOrphan = OrphanAction.Delete
        )
      )
    )
    val builder = IngestionPipeline
      .builder()
      .withGlobalConfig(TestFixtures.globalConfig())
      .withFlowConfigs(Seq(parent, child))

    builder.validate().mkString(" ") should include("unsupported onOrphan=delete")
    intercept[IllegalArgumentException](builder.build()).getMessage should include("unsupported onOrphan=delete")
  }

  it should "report missing SCD2 columns in a programmatic schema" in {
    val flow = TestFixtures
      .flowConfig(
        "scd2_invalid",
        loadMode = LoadMode.SCD2,
        primaryKey = Seq("missing_id"),
        columns = Seq(ColumnConfig("id", "string", nullable = false)),
        enforceSchema = true,
        compareColumns = Seq("missing_value")
      )

    val issues =
      IngestionPipeline.builder().withGlobalConfig(TestFixtures.globalConfig()).withFlowConfigs(Seq(flow)).validate()

    issues.exists(_.contains("primaryKey columns are not declared")) shouldBe true
    issues.exists(_.contains("compareColumns are not declared")) shouldBe true
  }

  it should "fail when domains.yaml exists but is malformed" in {
    val dir = Files.createTempDirectory("validate-bad-domains")
    setupValidConfig(dir)
    writeFile(dir.resolve("domains.yaml"), "domains: [broken: yaml")

    val issues = IngestionPipeline.builder().withConfigDirectory(dir.toString).validate()

    issues should not be empty
    issues.head should include("domains.yaml")
  }

  it should "reject programmatic flow names that collide case-insensitively" in {
    val builder = IngestionPipeline
      .builder()
      .withGlobalConfig(TestFixtures.globalConfig())
      .withFlowConfigs(Seq(TestFixtures.flowConfig("Orders"), TestFixtures.flowConfig("orders")))

    val issues = builder.validate().mkString(" ")
    issues should include("collide case-insensitively")
    intercept[IllegalArgumentException](builder.build()).getMessage should include("collide case-insensitively")
  }

  it should "reject non-finite rejection thresholds" in {
    val flow = TestFixtures.flowConfig("orders", maxRejectionRate = Some(Double.NaN))
    val global = TestFixtures.globalConfig(maxRejectionRate = Some(Double.PositiveInfinity))
    val builder = IngestionPipeline.builder().withGlobalConfig(global).withFlowConfigs(Seq(flow))

    val issues = builder.validate().mkString(" ")
    issues should include("processing.maxRejectionRate must be finite")
    issues should include("Flow 'orders': maxRejectionRate must be finite")
    intercept[IllegalArgumentException](builder.build()).getMessage should include("maxRejectionRate must be finite")
  }

  it should "reject SCD2 metadata columns that collide with source columns" in {
    val flow = TestFixtures.flowConfig(
      "customers",
      loadMode = LoadMode.SCD2,
      columns = Seq(
        ColumnConfig("id", "string", nullable = false),
        ColumnConfig("name", "string", nullable = true),
        ColumnConfig("valid_from", "timestamp", nullable = true)
      ),
      enforceSchema = true
    )
    val builder = IngestionPipeline
      .builder()
      .withGlobalConfig(TestFixtures.globalConfig())
      .withFlowConfigs(Seq(flow))

    val issues = builder.validate().mkString(" ")
    issues should include("SCD2 metadata columns collide with schema columns: valid_from")
    intercept[IllegalArgumentException](builder.build()).getMessage should include("valid_from")
  }

  it should "reject a quality metrics target that collides with a flow" in {
    val base = TestFixtures.globalConfig()
    val global = base.copy(processing = base.processing.copy(qualityMetricsTable = Some("ORDERS")))
    val builder = IngestionPipeline
      .builder()
      .withGlobalConfig(global)
      .withFlowConfigs(Seq(TestFixtures.flowConfig("orders")))

    val issues = builder.validate().mkString(" ")
    issues should include("qualityMetricsTable 'ORDERS' collides with flow 'orders'")
    intercept[IllegalArgumentException](builder.build()).getMessage should include("qualityMetricsTable")
  }

  it should "reject malformed schema identifiers before reading a source" in {
    val flow = TestFixtures.flowConfig(
      "sales.daily",
      columns = Seq(
        ColumnConfig("OrderId", "long", nullable = false),
        ColumnConfig("orderid", "long", nullable = false),
        ColumnConfig(" ", " ", nullable = true, sourceColumn = Some(" "))
      ),
      enforceSchema = true
    )

    val issues = IngestionPipeline
      .builder()
      .withGlobalConfig(TestFixtures.globalConfig())
      .withFlowConfigs(Seq(flow))
      .validate()
      .mkString(" ")

    issues should include("name must be unqualified")
    issues should include("schema column names must not be blank")
    issues should include("schema column types must not be blank")
    issues should include("case-insensitive duplicate column names")
    issues should include("sourceColumn values must not be blank")
  }

  it should "reject incomplete validation rules before reading a source" in {
    val flow = TestFixtures.flowConfig(
      "orders",
      columns = Seq(ColumnConfig("email", "string", nullable = true)),
      enforceSchema = true,
      rules = Seq(
        ValidationRule(ValidationRuleType.Regex, column = Some("email"), pattern = Some("[")),
        ValidationRule(ValidationRuleType.Range, column = Some("email")),
        ValidationRule(ValidationRuleType.Range, column = Some("email"), min = Some("NaN")),
        ValidationRule(ValidationRuleType.Domain, column = Some("email"), domainName = Some("missing")),
        ValidationRule(ValidationRuleType.Custom, `class` = Some(" ")),
        ValidationRule(ValidationRuleType.NotNull)
      )
    )

    val issues = IngestionPipeline
      .builder()
      .withGlobalConfig(TestFixtures.globalConfig())
      .withFlowConfigs(Seq(flow))
      .validate()
      .mkString(" ")

    issues should include("invalid regex pattern")
    issues should include("requires min or max")
    issues should include("'min' value 'NaN' must be finite")
    issues should include("domain 'missing' is not configured")
    issues should include("custom rule requires a non-blank class")
    issues should include("rule type 'not_null' is managed by the validation engine")
  }

  it should "reject colliding SCD2 metadata names" in {
    val base = TestFixtures.flowConfig(
      "customers",
      loadMode = LoadMode.SCD2,
      columns = Seq(
        ColumnConfig("id", "string", nullable = false),
        ColumnConfig("name", "string", nullable = true)
      ),
      enforceSchema = true
    )
    val flow = base.copy(
      loadMode = base.loadMode.copy(validFromColumn = Some("state_time"), validToColumn = Some("STATE_TIME"))
    )

    val issues = IngestionPipeline
      .builder()
      .withGlobalConfig(TestFixtures.globalConfig())
      .withFlowConfigs(Seq(flow))
      .validate()
      .mkString(" ")

    issues should include("SCD2 metadata column names must be unique case-insensitively")
  }

  it should "reject a non-positive flow concurrency limit" in {
    val base = TestFixtures.globalConfig(parallelFlows = true)
    val global = base.copy(performance = PerformanceConfig(parallelFlows = true, maxParallelFlows = 0))

    val issues = IngestionPipeline
      .builder()
      .withGlobalConfig(global)
      .withFlowConfigs(Seq(TestFixtures.flowConfig("orders")))
      .validate()

    issues should contain("performance.maxParallelFlows must be greater than zero")
  }

  it should "reject invalid Iceberg layout before creating a target table" in {
    val flow = TestFixtures.flowConfig(
      "events",
      columns = Seq(
        ColumnConfig("id", "long", nullable = false),
        ColumnConfig("event_time", "timestamp", nullable = false),
        ColumnConfig("payload", "string", nullable = true)
      ),
      enforceSchema = true,
      output = OutputConfig(
        sortOrder = Seq("missing", " ", "ID", "id"),
        icebergPartitions = Seq(
          "month(missing)",
          "bucket(0, id)",
          "truncate(-1, payload)",
          "unknown(event_time)",
          " ",
          "DAY(event_time)",
          "day(EVENT_TIME)"
        )
      )
    )

    val issues = IngestionPipeline
      .builder()
      .withGlobalConfig(TestFixtures.globalConfig())
      .withFlowConfigs(Seq(flow))
      .validate()
      .mkString(" ")

    issues should include("sortOrder columns must not be blank")
    issues should include("sortOrder columns must be unique case-insensitively")
    issues should include("sortOrder columns are not declared in schema: missing")
    issues should include("Iceberg partition transform 'month(missing)' references undeclared column 'missing'")
    issues should include("Iceberg partition transform 'bucket(0, id)' requires a positive width")
    issues should include("invalid Iceberg partition transform 'truncate(-1, payload)'")
    issues should include("unsupported Iceberg partition transform 'unknown(event_time)'")
    issues should include("Iceberg partition transforms must not be blank")
    issues should include("Iceberg partition transforms must be unique case-insensitively")
  }

  it should "reject invalid global runtime contracts during static validation" in {
    val base = TestFixtures.globalConfig()
    val global = base.copy(
      paths = PathsConfig(" ", "", " ", warningsPath = Some("")),
      processing = ProcessingConfig(batchIdFormat = "invalid", qualityMetricsTable = Some(" ")),
      iceberg = base.iceberg.copy(
        catalogMode = CatalogMode.Configure,
        catalogType = "missing",
        catalogName = " ",
        namespace = "",
        fileFormat = "json",
        catalogProperties = Map(" " -> "value")
      )
    )

    val issues = IngestionPipeline
      .builder()
      .withGlobalConfig(global)
      .withFlowConfigs(Seq(TestFixtures.flowConfig("orders")))
      .validate()
      .mkString(" ")

    issues should include("paths.outputPath must not be blank")
    issues should include("paths.rejectedPath must not be blank")
    issues should include("paths.metadataPath must not be blank")
    issues should include("paths.warningsPath must not be blank when configured")
    issues should include("processing.batchIdFormat is not a valid DateTimeFormatter pattern")
    issues should include("processing.qualityMetricsTable must not be blank")
    issues should include("iceberg.catalogName must not be blank")
    issues should include("iceberg.namespace must not be blank")
    issues should include("iceberg.fileFormat must be one of: avro, orc, parquet")
    issues should include("iceberg.catalogProperties keys must not be blank")
    issues should include("Unsupported catalog type: 'missing'")
  }

  it should "reject reserved and blank Iceberg table properties before execution" in {
    val flow = TestFixtures.flowConfig(
      "orders",
      output = OutputConfig(tableProperties = Map("format-version" -> "1", " " -> "value"))
    )

    val issues = IngestionPipeline
      .builder()
      .withGlobalConfig(TestFixtures.globalConfig())
      .withFlowConfigs(Seq(flow))
      .validate()
      .mkString(" ")

    issues should include("tableProperties must not override 'format-version'")
    issues should include("tableProperties keys must not be blank")
  }

  it should "reject unresolved code registrations before reading a source" in {
    val base = TestFixtures.flowConfig(
      "orders",
      rules = Seq(
        ValidationRule(
          ValidationRuleType.Custom,
          `class` = Some("com.example.validators.DoesNotExist")
        )
      )
    )
    val flow = base.copy(source = base.source.copy(`type` = SourceType.Custom("missing-reader")))
    val builder = IngestionPipeline
      .builder()
      .withGlobalConfig(TestFixtures.globalConfig())
      .withFlowConfigs(Seq(flow))
      .withFlowTransformation("orderz", preValidation = Some(context => context))

    val issues = builder.validate().mkString(" ")

    issues should include("Transformation references unknown flow 'orderz'")
    issues should include("Flow 'orders': custom source type 'missing-reader' has no registered data reader")
    issues should include(
      "Flow 'orders': custom validator 'com.example.validators.DoesNotExist' is neither registered nor a loadable Validator class"
    )
    intercept[IllegalArgumentException](builder.build()).getMessage should include("missing-reader")
  }
}
