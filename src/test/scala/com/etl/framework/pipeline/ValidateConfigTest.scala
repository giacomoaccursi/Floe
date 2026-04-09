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
}
