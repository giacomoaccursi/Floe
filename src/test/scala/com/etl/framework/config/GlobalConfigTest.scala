package com.etl.framework.config

import com.etl.framework.config.ConfigDecoders._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe.yaml.parser._

class GlobalConfigTest extends AnyFlatSpec with Matchers {

  "GlobalConfig" should "be decodable from valid YAML" in {
    val yaml =
      """
        |paths:
        |  validatedPath: "/data/validated"
        |  rejectedPath: "/data/rejected"
        |  metadataPath: "/data/metadata"
        |processing:
        |  batchIdFormat: "yyyyMMdd_HHmmss"
        |  failOnValidationError: true
        |  maxRejectionRate: 0.05
        |performance:
        |  parallelFlows: true
        |  parallelNodes: false
      """.stripMargin

    val config = parse(yaml).flatMap(_.as[GlobalConfig])
    config.isRight shouldBe true
    val c = config.right.get

    c.paths.validatedPath shouldBe "/data/validated"
    c.processing.maxRejectionRate shouldBe 0.05
    c.performance.parallelFlows shouldBe true
  }
}

// Since GlobalConfig is a simple Case Class, most logic is in the Loader.
// We should test GlobalConfigLoader logic specifically.

class GlobalConfigLoaderLogicTest extends AnyFlatSpec with Matchers {

  class TestGlobalLoader extends GlobalConfigLoader {
    // Expose protected methods for testing
    def testSubstitute(text: String): String = substituteEnvVars(text)
    def testParse(yaml: String): Either[Exception, GlobalConfig] =
      parseYaml[GlobalConfig](yaml, "test")
        .asInstanceOf[Either[
          Exception,
          GlobalConfig
        ]] // Cast for test simplicity if needed
  }

  val loader = new TestGlobalLoader()

  "substituteEnvVars" should "replace existing env vars" in {
    // This depends on actual env vars.
    // We can use generic ones like PATH or create a test one if possible, but let's assume one exists or skip
    val path = sys.env.getOrElse("PATH", "")
    if (path.nonEmpty) {
      loader.testSubstitute("Path is ${PATH}") should include(path)
    }
  }

  it should "leave unknown vars as is" in {
    loader.testSubstitute(
      "Value is ${UNKNOWN_VAR_123}"
    ) shouldBe "Value is ${UNKNOWN_VAR_123}"
  }

  it should "parse full configuration correctly" in {
    val yaml =
      """
        |paths:
        |  validatedPath: "/out/val"
        |  rejectedPath: "/out/rej"
        |  metadataPath: "/out/meta"
        |processing:
        |  batchIdFormat: "timestamp"
        |  failOnValidationError: false
        |  maxRejectionRate: 0.1
        |performance:
        |  parallelFlows: true
        |  parallelNodes: true
      """.stripMargin

    val result = loader.testParse(yaml)
    result.isRight shouldBe true
    val config = result.right.get

    config.paths.validatedPath shouldBe "/out/val"
    config.performance.parallelNodes shouldBe true
  }
}
