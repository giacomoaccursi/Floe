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
}
