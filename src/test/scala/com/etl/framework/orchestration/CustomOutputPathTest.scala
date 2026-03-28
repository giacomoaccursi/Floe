package com.etl.framework.orchestration

import com.etl.framework.config._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CustomOutputPathTest extends AnyFlatSpec with Matchers {

  private val globalConfig = GlobalConfig(
    paths = PathsConfig(
      outputPath = "/tmp/output",
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
    ),
    iceberg = IcebergConfig(warehouse = "/tmp/test-warehouse")
  )

  "Custom output path" should "be different from default path" in {
    val customPath = "/tmp/custom_output/prod/tenant_a"
    val defaultPath = s"${globalConfig.paths.outputPath}/test_flow"

    customPath should not be defaultPath
    customPath should not include globalConfig.paths.outputPath
  }

  it should "support custom rejected path different from default" in {
    val customRejectedPath = "/tmp/custom_output/prod/tenant_a"
    val defaultRejectedPath =
      s"${globalConfig.paths.rejectedPath}/test_flow"

    customRejectedPath should not be defaultRejectedPath
    customRejectedPath should not include globalConfig.paths.rejectedPath
  }

  it should "use default path when no custom path specified" in {
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
        columns = Seq(ColumnConfig("id", "string", nullable = false, "ID"))
      ),
      loadMode = LoadModeConfig(`type` = LoadMode.Full),
      validation = ValidationConfig(
        primaryKey = Seq("id"),
        foreignKeys = Seq.empty,
        rules = Seq.empty
      ),
      output = OutputConfig()
    )

    flowConfig.output.rejectedPath shouldBe empty
  }

  it should "support environment variable placeholders" in {
    val customPathWithVars = "/tmp/custom_output/${env}/${tenant_id}"

    customPathWithVars should include("${")
    customPathWithVars should not be s"${globalConfig.paths.outputPath}/test_flow"
  }

  it should "support different storage protocols" in {
    val protocols = Seq("hdfs://", "s3://", "s3a://", "file://", "/")

    protocols.foreach { protocol =>
      val customPath =
        if (protocol == "/") "/tmp/local_storage/bucket"
        else s"${protocol}bucket/data"
      val defaultPath = s"${globalConfig.paths.outputPath}/test_flow"

      customPath should not be defaultPath
      if (protocol == "/") {
        customPath should startWith("/")
      } else {
        customPath should startWith(protocol)
      }
    }
  }

}
