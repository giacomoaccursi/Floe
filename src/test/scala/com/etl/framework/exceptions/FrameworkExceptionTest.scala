package com.etl.framework.exceptions

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.etl.framework.exceptions.ContextKeys._

class FrameworkExceptionTest extends AnyFlatSpec with Matchers {

  "ConfigurationException" should "have correct error code" in {
    val ex = ConfigFileException("test.yaml", "msg")
    ex.errorCode shouldBe "CONFIG_FILE_ERROR"
    ex.context should contain(FILE -> "test.yaml")
  }

  "ValidationConfigException" should "include context information" in {
    val ex = ValidationConfigException("Detailed error")

    ex.errorCode shouldBe "VALIDATION_CONFIG_ERROR"
    val ctx = ex.context
    ctx should contain(MESSAGE -> "Detailed error")
  }

  "SchemaValidationException" should "include context information" in {
    val ex = SchemaValidationException(
      "flow1",
      Seq("col1"),
      Map("col2" -> ("string", "int"))
    )

    ex.errorCode shouldBe "VALIDATION_SCHEMA"
    val ctx = ex.context
    ctx should contain(FLOW_NAME -> "flow1")
    ctx.getOrElse(MISSING_COLUMNS, "").toString should include("col1")
    ctx(TYPE_MISMATCHES).toString should include("col2")
  }

  "PKUniqueness exception" should "capture duplicate count" in {
    val ex = PrimaryKeyViolationException("flowA", 5, Seq("id"))

    ex.errorCode shouldBe "VALIDATION_PK_VIOLATION"
    ex.context should contain(DUPLICATE_COUNT -> 5L)
    ex.context should contain(KEY_COLUMNS -> "id")
  }
}
