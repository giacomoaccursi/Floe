package com.etl.framework.config

import com.etl.framework.exceptions.{ConfigFileException, ConfigurationException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.generic.auto._
import java.io.{File, PrintWriter}
import scala.util.Using

class ConfigLoaderTest extends AnyFlatSpec with Matchers {

  case class TestConfig(name: String, value: Int, items: List[String])

  class TestConfigLoader extends ConfigLoader[TestConfig] {
    override def load(
        path: String
    ): Either[ConfigurationException, TestConfig] = {
      loadFromYamlFile(path)
    }

    // Public wrapper for testing protected method
    def parseYamlString(
        yaml: String,
        path: String = "test.yaml"
    ): Either[ConfigurationException, TestConfig] = {
      parseYaml(yaml, path)
    }

    // Public wrapper for testing protected method
    def substituteVars(
        text: String,
        path: String = "test.yaml",
        variables: Map[String, String] = Map.empty
    ): Either[ConfigurationException, String] = {
      substituteEnvVars(text, path, variables)
    }
  }

  val loader = new TestConfigLoader()

  "ConfigLoader" should "parse valid YAML" in {
    val yaml =
      """
        |name: "test"
        |value: 42
        |items:
        |  - "a"
        |  - "b"
      """.stripMargin

    val result = loader.parseYamlString(yaml)
    result shouldBe Right(TestConfig("test", 42, List("a", "b")))
  }

  it should "fail with ConfigFileException on invalid YAML syntax" in {
    val invalidYaml =
      """
        |name: "test"
        |value: [unclosed list
      """.stripMargin

    val result = loader.parseYamlString(invalidYaml)
    result.isLeft shouldBe true
    result.left.get shouldBe a[ConfigFileException]
  }

  it should "fail with ConfigFileException on missing required field" in {
    val incompleteYaml =
      """
        |name: "test"
        |items: []
      """.stripMargin // Missing 'value'

    val result = loader.parseYamlString(incompleteYaml)

    result.isLeft shouldBe true
    val exc = result.left.get
    exc shouldBe a[ConfigFileException]
    exc.getMessage should include("value")
  }

  it should "fail with type mismatch error" in {
    val typeMismatchYaml =
      """
        |name: "test"
        |value: "not an int"
        |items: []
      """.stripMargin

    val result = loader.parseYamlString(typeMismatchYaml)

    result.isLeft shouldBe true
    result.left.get shouldBe a[ConfigFileException]
  }

  "loadYamlFile" should "return ConfigFileException if file does not exist" in {
    val result = loader.load("non_existent_file.yaml")
    result.isLeft shouldBe true
    result.left.get shouldBe a[ConfigFileException]
    result.left.get.getMessage should include(
      "Failed to read configuration file"
    )
  }

  // Helper to create temp files
  def withTempFile(content: String)(f: File => Unit): Unit = {
    val file = File.createTempFile("test_config", ".yaml")
    file.deleteOnExit()
    Using(new PrintWriter(file)) { writer =>
      writer.write(content)
    }
    try {
      f(file)
    } finally {
      file.delete()
    }
  }

  it should "load from valid file" in {
    val content =
      """
        |name: "file_test"
        |value: 100
        |items: ["x"]
      """.stripMargin

    withTempFile(content) { file =>
      val result = loader.load(file.getAbsolutePath)
      result shouldBe Right(TestConfig("file_test", 100, List("x")))
    }
  }

  "substituteEnvVars" should "return Right with substituted text when variable is set" in {
    val originalEnv = sys.env.get("HOME")
    assume(originalEnv.isDefined, "HOME env var must be set for this test")
    val text = "path: ${HOME}/data"
    val result = loader.substituteVars(text)
    result.isRight shouldBe true
    result.right.get should include(originalEnv.get)
    result.right.get should not include "${"
  }

  it should "return Left(ConfigFileException) when variable is not set" in {
    val text = "value: ${NONEXISTENT_VAR_XYZ_12345}"
    val result = loader.substituteVars(text, "config.yaml")
    result.isLeft shouldBe true
    result.left.get shouldBe a[ConfigFileException]
    result.left.get.getMessage should include("NONEXISTENT_VAR_XYZ_12345")
  }

  it should "report all unresolved variables in a single Left" in {
    val text = "a: ${MISSING_A_XYZ} b: ${MISSING_B_XYZ}"
    val result = loader.substituteVars(text)
    result.isLeft shouldBe true
    val msg = result.left.get.getMessage
    msg should include("MISSING_A_XYZ")
    msg should include("MISSING_B_XYZ")
  }

  it should "return Right with unchanged text when no variables are present" in {
    val text = "plain: value"
    val result = loader.substituteVars(text)
    result shouldBe Right(text)
  }

  it should "propagate env var errors through loadFromYamlFile as Left, not exception" in {
    val content = "name: ${NONEXISTENT_VAR_XYZ_99999}\nvalue: 1\nitems: []"
    withTempFile(content) { file =>
      val result = loader.load(file.getAbsolutePath)
      result.isLeft shouldBe true
      result.left.get shouldBe a[ConfigFileException]
      result.left.get.getMessage should include("NONEXISTENT_VAR_XYZ_99999")
    }
  }

  it should "treat $$ as an escaped literal dollar sign" in {
    val text = "pattern: $$USD"
    val result = loader.substituteVars(text)
    result shouldBe Right("pattern: $USD")
  }

  it should "not treat $$ followed by letters as an env var" in {
    val text = "desc: Amount in $$USD is valid"
    val result = loader.substituteVars(text)
    result shouldBe Right("desc: Amount in $USD is valid")
  }

  it should "resolve variables from explicit map before sys.env" in {
    val text = "path: ${MY_CUSTOM_VAR}/data"
    val result = loader.substituteVars(text, variables = Map("MY_CUSTOM_VAR" -> "/custom/path"))
    result shouldBe Right("path: /custom/path/data")
  }

  it should "fall back to sys.env when variable not in explicit map" in {
    val home = sys.env.getOrElse("HOME", "")
    assume(home.nonEmpty, "HOME env var must be set for this test")
    val text = "path: ${HOME}/data"
    val result = loader.substituteVars(text, variables = Map("OTHER" -> "x"))
    result.isRight shouldBe true
    result.right.get shouldBe s"path: $home/data"
  }

  it should "prefer explicit variable over sys.env" in {
    val text = "path: ${HOME}/data"
    val result = loader.substituteVars(text, variables = Map("HOME" -> "/overridden"))
    result shouldBe Right("path: /overridden/data")
  }

  "ConfigLoader with defaults" should "use case class default values when field is missing" in {
    case class ConfigWithDefaults(
        name: String,
        optional: String = "default_value",
        items: List[String] = List.empty
    )

    class DefaultsLoader extends ConfigLoader[ConfigWithDefaults] {
      override def load(
          path: String
      ): Either[ConfigurationException, ConfigWithDefaults] = {
        loadFromYamlFile(path)
      }

      def parseYamlString(
          yaml: String
      ): Either[ConfigurationException, ConfigWithDefaults] = {
        parseYaml(yaml, "test.yaml")
      }
    }

    val loader = new DefaultsLoader()
    val yaml = """name: "only_required""""

    val result = loader.parseYamlString(yaml)
    result shouldBe Right(
      ConfigWithDefaults("only_required", "default_value", List.empty)
    )
  }
}
