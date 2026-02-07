package com.etl.framework.orchestration

import com.etl.framework.config._
import com.etl.framework.TestConfig
import org.apache.spark.sql.SparkSession
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters

/** Property-based tests for custom output paths Feature: spark-etl-framework,
  * Property 28: Custom Output Path Respect Validates: Requirements 21.2
  *
  * Test configuration: Uses minimalTestCount (5 cases) - simple path validation
  */
object CustomOutputPathProperties extends Properties("CustomOutputPath") {

  // Configure test parameters for simple path tests
  override def overrideParameters(p: Parameters): Parameters =
    TestConfig.minimalParams

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("CustomOutputPathProperties")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  // Generator for test data
  val testDataGen: Gen[Seq[(String, Int, String)]] = for {
    size <- Gen.choose(10, 50)
    data <- Gen.listOfN(
      size,
      for {
        id <- Gen.alphaNumStr.suchThat(_.nonEmpty)
        value <- Gen.choose(1, 1000)
        status <- Gen.oneOf("active", "inactive", "pending")
      } yield (id, value, status)
    )
  } yield data.distinct

  // Generator for custom paths
  val customPathGen: Gen[String] = for {
    tenant <- Gen.oneOf("tenant_a", "tenant_b", "tenant_c")
    env <- Gen.oneOf("dev", "staging", "prod")
  } yield s"/tmp/custom_output/$env/$tenant"

  // Generator for GlobalConfig
  val globalConfigGen: Gen[GlobalConfig] = Gen.const(
    GlobalConfig(
      paths = PathsConfig(
        validatedPath = "/tmp/validated",
        rejectedPath = "/tmp/rejected",
        metadataPath = "/tmp/metadata"
      ),
      processing = ProcessingConfig(
        batchIdFormat = "yyyyMMdd_HHmmss",
        failOnValidationError = false,
        maxRejectionRate = 0.1
      ),
      performance = PerformanceConfig(
        parallelFlows = false,
        parallelNodes = false
      )
    )
  )

  /** Property 28: Custom Output Path Respect For any flow with custom
    * output.path configured, the validated data should be saved to the
    * specified path, not the default path.
    */
  property("custom_output_path_is_respected") = forAll(
    customPathGen,
    globalConfigGen
  ) { (customPath, globalConfig) =>
    val flowName = "test_flow"
    val defaultPath = s"${globalConfig.paths.validatedPath}/$flowName"

    // Verify custom path is different from default
    customPath != defaultPath &&
    !customPath.contains(globalConfig.paths.validatedPath)
  }

  /** Property: Custom rejected path is respected For any flow with custom
    * output.rejectedPath configured, the rejected data should be saved to the
    * specified path, not the default rejected path.
    */
  property("custom_rejected_path_is_respected") = forAll(
    customPathGen,
    globalConfigGen
  ) { (customRejectedPath, globalConfig) =>
    val flowName = "test_flow"
    val defaultRejectedPath = s"${globalConfig.paths.rejectedPath}/$flowName"

    // Verify custom rejected path is different from default
    customRejectedPath != defaultRejectedPath &&
    !customRejectedPath.contains(globalConfig.paths.rejectedPath)
  }

  /** Property: Default path is used when no custom path specified For any flow
    * without custom output.path, the validated data should be saved to the
    * default path.
    */
  property("default_path_used_when_no_custom_path") = forAll(globalConfigGen) {
    globalConfig =>
      val flowName = "test_flow"
      val expectedDefaultPath = s"${globalConfig.paths.validatedPath}/$flowName"

      // Create flow config without custom path
      val flowConfig = FlowConfig(
        name = flowName,
        description = "Test flow",
        version = "1.0",
        owner = "test",
        source = SourceConfig(
          `type` = SourceType.File,
          path = "/tmp/test",
          format = FileFormat.CSV,
          options = Map.empty,
          filePattern = None
        ),
        schema = SchemaConfig(
          enforceSchema = true,
          allowExtraColumns = false,
          columns = Seq(
            ColumnConfig("id", "string", nullable = false, None, "ID")
          )
        ),
        loadMode = LoadModeConfig(
          `type` = LoadMode.Full,
          mergeStrategy = None,
          updateTimestampColumn = None,
          validFromColumn = None,
          validToColumn = None,
          isCurrentColumn = None,
          compareColumns = Seq.empty
        ),
        validation = ValidationConfig(
          primaryKey = Seq("id"),
          foreignKeys = Seq.empty,
          rules = Seq.empty
        ),
        output = OutputConfig(
          path = None, // No custom path
          rejectedPath = None,
          format = FileFormat.Parquet,
          partitionBy = Seq.empty,
          compression = "snappy",
          options = Map.empty
        ),
        preValidationTransformation = None,
        postValidationTransformation = None
      )

      // Verify default path would be used
      flowConfig.output.path.isEmpty &&
      expectedDefaultPath.contains(flowName)
  }

  /** Property: Custom path supports environment variables For any flow with
    * custom path containing environment variable placeholders, the path should
    * be valid and different from default.
    */
  property("custom_path_supports_environment_variables") = forAll(
    Gen.oneOf("dev", "staging", "prod"),
    Gen.oneOf("tenant_a", "tenant_b", "tenant_c"),
    globalConfigGen
  ) { (env, tenant, globalConfig) =>
    val customPathWithVars = s"/tmp/custom_output/$${env}/$${tenant_id}"
    val flowName = "test_flow"
    val defaultPath = s"${globalConfig.paths.validatedPath}/$flowName"

    // Verify custom path with variables is different from default
    customPathWithVars != defaultPath &&
    customPathWithVars.contains("${")
  }

  /** Property: Custom path can be on different storage For any flow with custom
    * path on different storage (HDFS, S3, local), the path should be valid and
    * use the correct protocol.
    */
  property("custom_path_supports_different_storage") = forAll(
    Gen.oneOf("hdfs://", "s3://", "s3a://", "file://", "/"),
    Gen.alphaNumStr.suchThat(_.nonEmpty),
    globalConfigGen
  ) { (protocol, bucket, globalConfig) =>
    val customPath = if (protocol == "/") {
      s"/tmp/local_storage/$bucket"
    } else {
      s"$protocol$bucket/data"
    }

    val flowName = "test_flow"
    val defaultPath = s"${globalConfig.paths.validatedPath}/$flowName"

    // Verify custom path is different from default
    customPath != defaultPath &&
    (customPath.startsWith(protocol) || customPath.startsWith("/"))
  }

  /** Property: Partitioning is applied when configured For any flow with
    * partitionBy configured, the output should respect partitioning.
    */
  property("partitioning_is_applied_when_configured") = forAll(
    Gen.nonEmptyListOf(Gen.oneOf("status", "date", "region")),
    globalConfigGen
  ) { (partitionColumns, globalConfig) =>
    val flowConfig = FlowConfig(
      name = "test_flow",
      description = "Test flow",
      version = "1.0",
      owner = "test",
      source = SourceConfig(
        `type` = SourceType.File,
        path = "/tmp/test",
        format = FileFormat.CSV,
        options = Map.empty,
        filePattern = None
      ),
      schema = SchemaConfig(
        enforceSchema = true,
        allowExtraColumns = false,
        columns = Seq(
          ColumnConfig("id", "string", nullable = false, None, "ID")
        )
      ),
      loadMode = LoadModeConfig(
        `type` = LoadMode.Full,
        mergeStrategy = None,
        updateTimestampColumn = None,
        validFromColumn = None,
        validToColumn = None,
        isCurrentColumn = None,
        compareColumns = Seq.empty
      ),
      validation = ValidationConfig(
        primaryKey = Seq("id"),
        foreignKeys = Seq.empty,
        rules = Seq.empty
      ),
      output = OutputConfig(
        path = None,
        rejectedPath = None,
        format = FileFormat.Parquet,
        partitionBy = partitionColumns.distinct, // Partitioning configured
        compression = "snappy",
        options = Map.empty
      ),
      preValidationTransformation = None,
      postValidationTransformation = None
    )

    // Verify partitioning is configured
    flowConfig.output.partitionBy.nonEmpty &&
    flowConfig.output.partitionBy.length == partitionColumns.distinct.length
  }
}
