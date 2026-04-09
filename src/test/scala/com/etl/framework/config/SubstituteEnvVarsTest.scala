package com.etl.framework.config

import com.etl.framework.{config, exceptions}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SubstituteEnvVarsTest extends AnyFlatSpec with Matchers {

  // Expose protected method for testing
  private object TestLoader extends ConfigLoader[String] {
    override def load(path: String): Either[exceptions.ConfigurationException, String] = Right("")
    def testSubstitute(text: String, variables: Map[String, String]): Either[Exception, String] =
      substituteEnvVars(text, "<test>", variables)
  }

  private def substitute(
      text: String,
      variables: Map[String, String] = Map.empty
  ): Either[Exception, String] =
    TestLoader.testSubstitute(text, variables)

  "substituteEnvVars" should "replace ${VAR} with variable value" in {
    substitute("path/${DIR}/data", Map("DIR" -> "output")) shouldBe Right("path/output/data")
  }

  it should "replace $VAR without braces" in {
    substitute("path/$DIR/data", Map("DIR" -> "output")) shouldBe Right("path/output/data")
  }

  it should "replace multiple variables" in {
    substitute("${A}/${B}", Map("A" -> "x", "B" -> "y")) shouldBe Right("x/y")
  }

  it should "return Left for unresolved variables" in {
    val result = substitute("${MISSING_VAR}")
    result.isLeft shouldBe true
    result.left.get.getMessage should include("MISSING_VAR")
  }

  it should "report all unresolved variables at once" in {
    val result = substitute("${A}/${B}")
    result.isLeft shouldBe true
    val msg = result.left.get.getMessage
    msg should include("A")
    msg should include("B")
  }

  it should "prefer explicit variables over env vars" in {
    // HOME is always set in env
    substitute("${HOME}", Map("HOME" -> "/custom")) shouldBe Right("/custom")
  }

  it should "fall back to env vars when variable not in explicit map" in {
    // HOME is always set
    val result = substitute("${HOME}")
    result.isRight shouldBe true
    result.right.get should not be empty
  }

  it should "handle text with no variables" in {
    substitute("plain text without vars") shouldBe Right("plain text without vars")
  }

  it should "handle empty text" in {
    substitute("") shouldBe Right("")
  }

  it should "handle variable at start and end" in {
    substitute("${A}", Map("A" -> "value")) shouldBe Right("value")
  }

  it should "handle adjacent variables" in {
    substitute("${A}${B}", Map("A" -> "hello", "B" -> "world")) shouldBe Right("helloworld")
  }

  it should "handle variable with underscores and numbers" in {
    substitute("${MY_VAR_123}", Map("MY_VAR_123" -> "ok")) shouldBe Right("ok")
  }

  it should "preserve literal dollar signs in resolved output" in {
    substitute("price: ${AMOUNT}", Map("AMOUNT" -> "$100")) shouldBe Right("price: $100")
  }
}
