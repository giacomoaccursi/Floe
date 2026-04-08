package com.etl.framework.validation.validators

import com.etl.framework.config.{ValidationRule, DomainsConfig, DomainConfig, ValidationRuleType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

class DomainValidatorTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DomainValidatorTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  import spark.implicits._

  def createDomainsConfig(
      domainName: String,
      values: Seq[String],
      caseSensitive: Boolean = true
  ): DomainsConfig = {
    DomainsConfig(
      domains = Map(
        domainName -> DomainConfig(
          name = domainName,
          description = "test domain",
          values = values,
          caseSensitive = caseSensitive
        )
      )
    )
  }

  def createRule(domainName: String): ValidationRule = {
    ValidationRule(
      `type` = ValidationRuleType.Domain,
      column = Some("status"),
      domainName = Some(domainName)
    )
  }

  "DomainValidator" should "validate values against a configured domain" in {
    val schema = StructType(Seq(StructField("status", StringType, true)))
    val data = Seq(
      Row("ACTIVE"),
      Row("INACTIVE"),
      Row("PENDING")
    )
    val df = spark.createDataFrame(data.asJava, schema)

    val domainsConfig =
      createDomainsConfig("STATUS_DOMAIN", Seq("ACTIVE", "INACTIVE", "PENDING"))
    val rule = createRule("STATUS_DOMAIN")
    val validator = new DomainValidator(Some(domainsConfig))

    val result = validator.validate(df, rule)

    result.rejected match {
      case Some(rejectedDf) => rejectedDf.count() shouldBe 0
      case None             => succeed
    }
    result.valid.count() shouldBe 3
  }

  it should "detect invalid values not in the domain" in {
    val schema = StructType(Seq(StructField("status", StringType, true)))
    val data = Seq(
      Row("ACTIVE"),
      Row("UNKNOWN"),
      Row("DELETED")
    )
    val df = spark.createDataFrame(data.asJava, schema)

    val domainsConfig =
      createDomainsConfig("STATUS_DOMAIN", Seq("ACTIVE", "INACTIVE"))
    val rule = createRule("STATUS_DOMAIN")
    val result = new DomainValidator(Some(domainsConfig)).validate(df, rule)

    result.valid.count() shouldBe 1
    result.valid.select("status").as[String].collect() should contain("ACTIVE")

    val rejected = result.rejected.get
    rejected.count() shouldBe 2
    rejected.select("status").as[String].collect() should contain allOf (
      "UNKNOWN",
      "DELETED"
    )
  }

  it should "be case sensitive by default" in {
    val schema = StructType(Seq(StructField("status", StringType, true)))
    val data = Seq(Row("active")) // lowercase
    val df = spark.createDataFrame(data.asJava, schema)

    val domainsConfig =
      createDomainsConfig("STATUS_DOMAIN", Seq("ACTIVE")) // Uppercase
    val rule = createRule("STATUS_DOMAIN")
    val result = new DomainValidator(Some(domainsConfig)).validate(df, rule)

    result.valid.count() shouldBe 0
    result.rejected.get.count() shouldBe 1
  }

  it should "support case insensitive validation when configured" in {
    val schema = StructType(Seq(StructField("status", StringType, true)))
    val data = Seq(Row("active"), Row("ACTIVE"))
    val df = spark.createDataFrame(data.asJava, schema)

    val domainsConfig =
      createDomainsConfig("STATUS_DOMAIN", Seq("ACTIVE"), caseSensitive = false)
    val rule = createRule("STATUS_DOMAIN")
    val result = new DomainValidator(Some(domainsConfig)).validate(df, rule)

    result.valid.count() shouldBe 2
    result.rejected.flatMap(df => Some(df.count())).getOrElse(0L) shouldBe 0
  }
}
