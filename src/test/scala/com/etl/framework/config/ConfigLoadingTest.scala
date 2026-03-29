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
      batchIdFormat = "yyyyMMdd_HHmmss"
    ),
    performance = PerformanceConfig(
      parallelFlows = true,
    ),
    iceberg = IcebergConfig(warehouse = "/tmp/test-warehouse")
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
      options = Map("header" -> "true", "delimiter" -> ",")
    ),
    schema = SchemaConfig(
      enforceSchema = true,
      allowExtraColumns = false,
      columns = Seq(
        ColumnConfig("id", "long", nullable = false, "Customer ID"),
        ColumnConfig("name", "string", nullable = false, "Name"),
        ColumnConfig("email", "string", nullable = true, "Email")
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
        |performance:
        |  parallelFlows: true
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

  "FlowConfigLoader" should "reject SCD2 flow with empty primaryKey" in {
    val yaml =
      """
        |name: scd2_flow
        |description: SCD2 flow
        |version: "1.0"
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
        |  type: scd2
        |  compareColumns:
        |    - name
        |    - email
        |  validFromColumn: valid_from
        |  validToColumn: valid_to
        |  isCurrentColumn: is_current
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
    result.left.get.getMessage should include("primaryKey")
  }

  it should "accept SCD2 flow with non-empty primaryKey and compareColumns" in {
    val yaml =
      """
        |name: scd2_flow
        |description: SCD2 flow
        |version: "1.0"
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
        |  type: scd2
        |  compareColumns:
        |    - name
        |    - email
        |  validFromColumn: valid_from
        |  validToColumn: valid_to
        |  isCurrentColumn: is_current
        |validation:
        |  primaryKey:
        |    - id
        |  foreignKeys: []
        |  rules: []
        |output:
        |  format: parquet
        |  partitionBy: []
        |  compression: snappy
        |  options: {}
      """.stripMargin

    val result = writeInvalidYaml(yaml, new FlowConfigLoader())
    result.isRight shouldBe true
  }

  it should "reject SCD2 flow when a PK column is declared nullable=true" in {
    // PK columns must be non-nullable for SCD2: the merge SQL uses NULL as a
    // sentinel to distinguish "new record to insert" from "existing record to
    // update". A nullable PK collides with this sentinel and causes duplicates
    // on every run.
    val yaml =
      """
        |name: scd2_nullable_pk
        |description: SCD2 with nullable PK
        |version: "1.0"
        |owner: test
        |source:
        |  type: file
        |  path: /data/input
        |  format: csv
        |  options: {}
        |schema:
        |  enforceSchema: true
        |  allowExtraColumns: false
        |  columns:
        |    - name: id
        |      type: string
        |      nullable: true
        |      description: PK column but nullable!
        |    - name: name
        |      type: string
        |      nullable: true
        |      description: business column
        |loadMode:
        |  type: scd2
        |  compareColumns:
        |    - name
        |  validFromColumn: valid_from
        |  validToColumn: valid_to
        |  isCurrentColumn: is_current
        |validation:
        |  primaryKey:
        |    - id
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
    result.left.get.getMessage should include("id")
    result.left.get.getMessage should (include("nullable") or include("null"))
  }

  it should "accept SCD2 flow when all PK columns are non-nullable" in {
    val yaml =
      """
        |name: scd2_nonnullable_pk
        |description: SCD2 with non-nullable PK
        |version: "1.0"
        |owner: test
        |source:
        |  type: file
        |  path: /data/input
        |  format: csv
        |  options: {}
        |schema:
        |  enforceSchema: true
        |  allowExtraColumns: false
        |  columns:
        |    - name: id
        |      type: string
        |      nullable: false
        |      description: PK column, non-nullable
        |    - name: name
        |      type: string
        |      nullable: true
        |      description: business column
        |loadMode:
        |  type: scd2
        |  compareColumns:
        |    - name
        |  validFromColumn: valid_from
        |  validToColumn: valid_to
        |  isCurrentColumn: is_current
        |validation:
        |  primaryKey:
        |    - id
        |  foreignKeys: []
        |  rules: []
        |output:
        |  format: parquet
        |  partitionBy: []
        |  compression: snappy
        |  options: {}
      """.stripMargin

    val result = writeInvalidYaml(yaml, new FlowConfigLoader())
    result.isRight shouldBe true
  }

  "FlowConfigLoader" should "substitute environment variables in YAML" in {
    assume(sys.env.contains("HOME"), "HOME env var must be set for this test")
    val homeValue = sys.env("HOME")
    val yaml =
      s"""
        |name: env_flow
        |description: Flow with env var
        |version: "1.0"
        |owner: test
        |source:
        |  type: file
        |  path: $${HOME}/data/input
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
    result.isRight shouldBe true
    result.right.get.source.path shouldBe s"$homeValue/data/input"
  }

  it should "return Left when a referenced environment variable is not set" in {
    val yaml =
      """
        |name: missing_env_flow
        |description: Flow with unresolved env var
        |version: "1.0"
        |owner: test
        |source:
        |  type: file
        |  path: ${DEFINITELY_MISSING_ENV_VAR_XYZ_789}/data
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
    result.left.get.getMessage should include("DEFINITELY_MISSING_ENV_VAR_XYZ_789")
  }

  "FlowConfigLoader.loadAll" should "report all errors when multiple files are invalid" in {
    import java.nio.file.{Files => NIOFiles}

    val tempDir = NIOFiles.createTempDirectory("loadAll-test").toFile
    tempDir.deleteOnExit()

    def writeFile(name: String, content: String): Unit = {
      val f = new java.io.File(tempDir, name)
      f.deleteOnExit()
      val pw = new PrintWriter(f)
      try pw.write(content)
      finally pw.close()
    }

    val missingNameYaml =
      """
        |description: flow without name
        |version: "1.0"
        |owner: test
        |source:
        |  type: file
        |  path: /data
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

    writeFile("bad1.yaml", missingNameYaml)
    writeFile("bad2.yaml", missingNameYaml)

    val loader = new FlowConfigLoader()
    val result = loader.loadAll(tempDir.getAbsolutePath)

    result.isLeft shouldBe true
    val msg = result.left.get.getMessage
    // Should report both failures, not just the first
    msg should include("2 flow configuration(s) failed to load")
  }

  it should "succeed when all files in directory are valid" in {
    import java.nio.file.{Files => NIOFiles}

    val tempDir = NIOFiles.createTempDirectory("loadAll-ok-test").toFile
    tempDir.deleteOnExit()

    val validYaml =
      """
        |name: valid_flow
        |description: Valid flow
        |version: "1.0"
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

    val f = new java.io.File(tempDir, "flow.yaml")
    f.deleteOnExit()
    val pw = new PrintWriter(f)
    try pw.write(validYaml)
    finally pw.close()

    val loader = new FlowConfigLoader()
    val result = loader.loadAll(tempDir.getAbsolutePath)

    result.isRight shouldBe true
    result.right.get should have size 1
    result.right.get.head.name shouldBe "valid_flow"
  }

  it should "reject duplicate flow names in loadAll" in {
    val tempDir = Files.createTempDirectory("dup-flow-test").toFile
    tempDir.deleteOnExit()

    val flowYaml =
      """
        |name: same_name
        |source:
        |  type: file
        |  path: "data/test.csv"
        |  format: csv
        |  options: {}
        |schema:
        |  enforceSchema: false
        |  allowExtraColumns: true
        |  columns: []
        |loadMode:
        |  type: full
        |validation:
        |  primaryKey: [id]
        |  foreignKeys: []
        |  rules: []
        |output:
        |  options: {}
      """.stripMargin

    Seq("flow_a.yaml", "flow_b.yaml").foreach { fileName =>
      val f = new java.io.File(tempDir, fileName)
      f.deleteOnExit()
      val pw = new PrintWriter(f)
      try pw.write(flowYaml)
      finally pw.close()
    }

    val loader = new FlowConfigLoader()
    val result = loader.loadAll(tempDir.getAbsolutePath)

    result.isLeft shouldBe true
    result.left.get.getMessage should include("Duplicate flow names")
    result.left.get.getMessage should include("same_name")
  }
}
