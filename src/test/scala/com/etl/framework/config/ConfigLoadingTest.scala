package com.etl.framework.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yaml.snakeyaml.{DumperOptions, Yaml}
import pureconfig.ConfigWriter
import pureconfig.generic.auto._

import java.io.PrintWriter
import java.nio.file.Files
import ConfigHints._

class ConfigLoadingTest extends AnyFlatSpec with Matchers {

  implicit val flowConfigWriter: ConfigWriter[FlowConfig] =
    ConfigWriter[FlowConfigYaml].contramap(FlowConfigYaml.fromFlowConfig)

  private def roundTripConfig[T](config: T, loader: ConfigLoader[T])(implicit
      writer: ConfigWriter[T]
  ): Either[Exception, T] = {
    val tempFile = Files.createTempFile("config-test-", ".yaml")
    val file = tempFile.toFile
    file.deleteOnExit()
    try {
      val configValue = writer.to(config)
      val options = new DumperOptions()
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yamlDumper = new Yaml(options)
      val yamlString = yamlDumper.dump(configValue.unwrapped())

      val pw = new PrintWriter(file)
      try pw.write(yamlString)
      finally pw.close()

      loader.load(file.getAbsolutePath)
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  private def writeInvalidYaml[T](yaml: String, loader: ConfigLoader[T]) = {
    val tempFile = Files.createTempFile("invalid-config-test-", ".yaml")
    val file = tempFile.toFile
    file.deleteOnExit()
    try {
      val pw = new PrintWriter(file)
      try pw.write(yaml)
      finally pw.close()
      loader.load(file.getAbsolutePath)
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  private val sampleGlobalConfig = GlobalConfig(
    paths = PathsConfig(
      outputPath = "/data/output/warehouse",
      rejectedPath = "/data/rejected/errors",
      metadataPath = "/data/metadata/meta"
    ),
    processing = ProcessingConfig(
      batchIdFormat = "yyyyMMdd_HHmmss",
      failOnValidationError = false,
      maxRejectionRate = 0.1
    ),
    performance = PerformanceConfig(
      parallelFlows = true,
      parallelNodes = true
    )
  )

  private val sampleDomainsConfig = DomainsConfig(
    domains = Map(
      "status" -> DomainConfig(
        name = "status",
        description = "Status domain",
        values = List("active", "inactive", "pending"),
        caseSensitive = false
      ),
      "country" -> DomainConfig(
        name = "country",
        description = "Country codes",
        values = List("US", "UK", "IT", "DE"),
        caseSensitive = true
      )
    )
  )

  private val sampleFlowConfig = FlowConfig(
    name = "customers",
    description = "Customer data flow",
    version = "1.0",
    owner = "data_team",
    source = SourceConfig(
      `type` = SourceType.File,
      path = "/data/input/customers",
      format = FileFormat.CSV,
      options = Map("header" -> "true", "delimiter" -> ","),
      filePattern = Some("*.csv")
    ),
    schema = SchemaConfig(
      enforceSchema = true,
      allowExtraColumns = false,
      columns = Seq(
        ColumnConfig("id", "long", nullable = false, None, "Customer ID"),
        ColumnConfig("name", "string", nullable = false, None, "Name"),
        ColumnConfig("email", "string", nullable = true, None, "Email")
      )
    ),
    loadMode = LoadModeConfig(`type` = LoadMode.Full),
    validation = ValidationConfig(
      primaryKey = Seq("id"),
      foreignKeys = Seq.empty,
      rules = Seq(
        ValidationRule(
          `type` = ValidationRuleType.Regex,
          column = Some("email"),
          pattern = Some("^[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+$"),
          description = Some("Email format"),
          onFailure = OnFailureAction.Warn
        )
      )
    ),
    output = OutputConfig()
  )

  "GlobalConfig roundtrip" should "serialize and deserialize correctly" in {
    val result = roundTripConfig(sampleGlobalConfig, new GlobalConfigLoader())
    result shouldBe Right(sampleGlobalConfig)
  }

  "DomainsConfig roundtrip" should "serialize and deserialize correctly" in {
    val result = roundTripConfig(sampleDomainsConfig, new DomainsConfigLoader())
    result shouldBe Right(sampleDomainsConfig)
  }

  "FlowConfig roundtrip" should "serialize and deserialize correctly" in {
    val result = roundTripConfig(sampleFlowConfig, new FlowConfigLoader())
    result shouldBe Right(sampleFlowConfig)
  }

  "Invalid YAML syntax" should "produce error for GlobalConfig" in {
    val invalidYaml = "spark:\n  appName: test\n  config: {key: value"
    val result = writeInvalidYaml(invalidYaml, new GlobalConfigLoader())
    result.isLeft shouldBe true
    result.left.get.getMessage should not be empty
  }

  it should "produce error for FlowConfig" in {
    val invalidYaml = "spark:\n  appName: \"test\n  master: local"
    val result = writeInvalidYaml(invalidYaml, new FlowConfigLoader())
    result.isLeft shouldBe true
    result.left.get.getMessage should not be empty
  }

  it should "produce error for DomainsConfig" in {
    val invalidYaml = "columns:\n  - name: col1\n  type: string"
    val result = writeInvalidYaml(invalidYaml, new DomainsConfigLoader())
    result.isLeft shouldBe true
    result.left.get.getMessage should not be empty
  }

  "Missing required fields" should "produce error for GlobalConfig" in {
    val yaml =
      """
        |processing:
        |  batchIdFormat: yyyyMMdd_HHmmss
        |  failOnValidationError: false
        |  maxRejectionRate: 0.1
        |performance:
        |  parallelFlows: true
        |  parallelNodes: true
      """.stripMargin
    val result = writeInvalidYaml(yaml, new GlobalConfigLoader())
    result.isLeft shouldBe true
  }

  it should "produce error for FlowConfig with missing name" in {
    val yaml =
      """
        |description: Test flow
        |version: 1.0
        |owner: test
        |source:
        |  type: file
        |  path: /data/input
        |  format: csv
        |  options: {}
        |schema:
        |  enforceSchema: true
        |  allowExtraColumns: false
        |  columns: []
        |loadMode:
        |  type: full
        |validation:
        |  primaryKey: []
        |  foreignKeys: []
        |  rules: []
        |output:
        |  format: parquet
        |  partitionBy: []
        |  compression: snappy
        |  options: {}
      """.stripMargin
    val result = writeInvalidYaml(yaml, new FlowConfigLoader())
    result.isLeft shouldBe true
  }

  "Non-existent file" should "produce descriptive error" in {
    val loader = new GlobalConfigLoader()
    val result = loader.load("/tmp/nonexistent_file_12345.yaml")
    result.isLeft shouldBe true
    result.left.get.getMessage should include("Failed to read")
  }
}
