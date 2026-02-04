package com.etl.framework.config

import com.etl.framework.exceptions.{ConfigFileException, ConfigurationException, YAMLSyntaxException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe.generic.auto._
import java.io.{File, PrintWriter}
import scala.util.Using

class ConfigLoaderTest extends AnyFlatSpec with Matchers {

  case class TestConfig(name: String, value: Int, items: List[String])
  
  class TestConfigLoader extends ConfigLoader[TestConfig] {
    override def load(path: String): Either[ConfigurationException, TestConfig] = {
      for {
        yaml <- loadYamlFile(path)
        config <- parseYaml[TestConfig](yaml, path)
      } yield config
    }
    
    // Public wrapper for testing protected method
    def parseYamlString(yaml: String, path: String = "test.yaml"): Either[ConfigurationException, TestConfig] = {
      parseYaml[TestConfig](yaml, path)
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
  
  it should "fail with YAMLSyntaxException on invalid YAML syntax" in {
    val invalidYaml = 
      """
        |name: "test"
        |value: [unclosed list
      """.stripMargin
      
    val result = loader.parseYamlString(invalidYaml)
    result.left.get shouldBe a [YAMLSyntaxException]
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
    exc shouldBe a [ConfigFileException]
    exc.getMessage should include("Missing required field 'value'")
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
    result.left.get shouldBe a [ConfigFileException] 
  }
  
  "loadYamlFile" should "return ConfigFileException if file does not exist" in {
    val result = loader.load("non_existent_file.yaml")
    result.isLeft shouldBe true
    result.left.get shouldBe a [ConfigFileException]
    result.left.get.getMessage should include("Failed to read configuration file")
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
}
