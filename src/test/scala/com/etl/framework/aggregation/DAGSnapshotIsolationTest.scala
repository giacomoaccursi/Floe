package com.etl.framework.aggregation

import com.etl.framework.config._
import com.etl.framework.exceptions.ValidationConfigException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class DAGSnapshotIsolationTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val warehouse = Files.createTempDirectory("dag-snapshot-isolation")

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DAGSnapshotIsolationTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.dag_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.dag_catalog.type", "hadoop")
    .config("spark.sql.catalog.dag_catalog.warehouse", warehouse.toString)
    .getOrCreate()

  import spark.implicits._

  private val tableName = "dag_catalog.default.dag_snapshot_source"
  private val emptyTableName = "dag_catalog.default.dag_snapshotless_source"
  private val globalConfig = GlobalConfig(
    paths = PathsConfig("/tmp/dag-output", "/tmp/dag-rejected", "/tmp/dag-metadata"),
    iceberg = IcebergConfig(catalogName = "dag_catalog", namespace = "default", warehouse = warehouse.toString)
  )

  "DAGOrchestrator.execute" should "keep managed input state stable after the lazy plan is returned" in {
    Seq((1, "initial")).toDF("id", "value").writeTo(tableName).create()
    val dag = AggregationConfig(
      name = "snapshot_isolation",
      nodes = Seq(DAGNode(id = "source", sourceFlow = "dag_snapshot_source"))
    )

    val result = new DAGOrchestrator(dag, globalConfig).execute()
    Seq((2, "later")).toDF("id", "value").writeTo(tableName).overwrite(lit(true))

    result.as[(Int, String)].collect().toSeq shouldBe Seq((1, "initial"))
  }

  it should "keep a snapshotless empty input stable when its first snapshot appears later" in {
    spark.sql(s"CREATE TABLE $emptyTableName (id INT, value STRING) USING iceberg")
    val dag = AggregationConfig(
      name = "snapshotless_isolation",
      nodes = Seq(DAGNode(id = "source", sourceFlow = "dag_snapshotless_source"))
    )

    val result = new DAGOrchestrator(dag, globalConfig).execute()
    Seq((1, "later")).toDF("id", "value").writeTo(emptyTableName).append()

    result.schema.fieldNames shouldBe Array("id", "value")
    result.count() shouldBe 0L
  }

  it should "reject a source that cannot provide Iceberg snapshot stability" in {
    Seq((1, "view")).toDF("id", "value").createOrReplaceTempView("dag_non_iceberg_view")
    val dag = AggregationConfig(
      name = "non_iceberg_source",
      nodes = Seq(DAGNode(id = "source", sourceTable = Some("dag_non_iceberg_view")))
    )

    val error = intercept[ValidationConfigException] {
      new DAGOrchestrator(dag, globalConfig).execute()
    }

    error.getMessage should include("must resolve to a readable Iceberg table")
  }

  override def afterAll(): Unit = {
    try {
      Seq(tableName, emptyTableName).foreach { name =>
        val _ = spark.sql(s"DROP TABLE IF EXISTS $name")
      }
      val _ = spark.catalog.dropTempView("dag_non_iceberg_view")
    } finally spark.stop()
    super.afterAll()
  }
}
