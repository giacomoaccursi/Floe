package com.etl.framework.validation

import com.etl.framework.TestFixtures
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PrimaryKeyValidatorRegressionTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("PrimaryKeyValidatorRegressionTest")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  "Primary-key validation" should "reject null keys instead of losing them in an equality join" in {
    val input = Seq[java.lang.Integer](null, null, 1).toDF("id")
    val result = new ValidationEngine().validate(input, TestFixtures.flowConfig("nullable_pk"))

    result.valid.select("id").collect().map(_.getInt(0)).toSeq shouldBe Seq(1)
    result.rejected.get.count() shouldBe 2L
  }
}
