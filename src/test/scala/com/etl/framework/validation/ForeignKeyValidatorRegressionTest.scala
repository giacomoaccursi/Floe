package com.etl.framework.validation

import com.etl.framework.TestFixtures
import com.etl.framework.config.{ForeignKeyConfig, ReferenceConfig}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ForeignKeyValidatorRegressionTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("ForeignKeyValidatorRegressionTest")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  "Foreign key validation" should "not multiply valid children when a parent key has multiple versions and another child is orphaned" in {
    val flowConfig = TestFixtures.flowConfig(
      name = "child",
      primaryKey = Seq("child_id"),
      foreignKeys = Seq(
        ForeignKeyConfig(
          columns = Seq("parent_id"),
          references = ReferenceConfig(flow = "parent", columns = Seq("parent_id"))
        )
      )
    )
    val parents = Seq((1, "old"), (1, "current")).toDF("parent_id", "version")
    val children = Seq((10, 1), (11, 2)).toDF("child_id", "parent_id")

    val result = new ValidationEngine().validate(children, flowConfig, Map("parent" -> parents))

    result.valid.select("child_id").collect().map(_.getInt(0)).toSeq shouldBe Seq(10)
    result.rejected.get.select("child_id").collect().map(_.getInt(0)).toSeq shouldBe Seq(11)
  }
}
