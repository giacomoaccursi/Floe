package com.etl.framework.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.yaml._

class GlobalConfigTest extends AnyFlatSpec with Matchers {

  "GlobalConfig" should "be decodable from valid YAML" in {
    val yaml =
      """
        |paths:
        |  outputPath: "/data/output"
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

    import ConfigHints._
    val config = YamlConfigSource.string(yaml).load[GlobalConfig]
    config.isRight shouldBe true
    val c = config.toOption.get

    c.paths.outputPath shouldBe "/data/output"
    c.processing.maxRejectionRate shouldBe 0.05
    c.performance.parallelFlows shouldBe true
  }
}

// Since GlobalConfig is a simple Case Class, most logic is in the Loader.
// We should test GlobalConfigLoader logic specifically.

class GlobalConfigLoaderLogicTest extends AnyFlatSpec with Matchers {
  import ConfigHints._

  class TestGlobalLoader extends GlobalConfigLoader {
    // Expose protected methods for testing
    def testSubstitute(text: String): String = substituteEnvVars(text)
    def testParse(yaml: String): Either[Exception, GlobalConfig] =
      parseYaml(yaml, "test").left.map(e => e: Exception)
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

  it should "throw on unresolved env vars" in {
    val ex = intercept[IllegalArgumentException] {
      loader.testSubstitute("Value is ${UNKNOWN_VAR_123}")
    }
    ex.getMessage should include("UNKNOWN_VAR_123")
  }

  it should "parse full configuration correctly" in {
    val yaml =
      """
        |paths:
        |  outputPath: "/out/output"
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
    val config = result.toOption.get

    config.paths.outputPath shouldBe "/out/output"
    config.performance.parallelNodes shouldBe true
  }
}
