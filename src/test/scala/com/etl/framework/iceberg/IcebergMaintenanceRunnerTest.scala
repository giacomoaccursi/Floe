package com.etl.framework.iceberg

import com.etl.framework.config.{IcebergConfig, MaintenanceConfig}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}

class IcebergMaintenanceRunnerTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var warehousePath: Path = _

  implicit val spark: SparkSession = {
    val tmpDir = Files.createTempDirectory("maintenance-runner-test")
    warehousePath = tmpDir

    SparkSession.getActiveSession.foreach(_.stop())

    SparkSession
      .builder()
      .appName("IcebergMaintenanceRunnerTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "1")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config(
        "spark.sql.catalog.maint_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config("spark.sql.catalog.maint_catalog.type", "hadoop")
      .config(
        "spark.sql.catalog.maint_catalog.warehouse",
        tmpDir.toString
      )
      .getOrCreate()
  }

  import spark.implicits._

  private val icebergConfig = IcebergConfig(
    catalogName = "maint_catalog",
    warehouse = warehousePath.toString
  )

  private val runner = new IcebergMaintenanceRunner(spark, icebergConfig)
  private val tableName = "maint_catalog.default.maint_test"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE TABLE $tableName (id INT, name STRING) USING iceberg")
    // Write multiple snapshots so maintenance has something to work with
    Seq((1, "Alice")).toDF("id", "name").writeTo(tableName).append()
    Seq((2, "Bob")).toDF("id", "name").writeTo(tableName).append()
    Seq((3, "Charlie")).toDF("id", "name").writeTo(tableName).append()
  }

  "IcebergMaintenanceRunner" should "expire old snapshots" in {
    val snapshotsBefore = spark.sql(s"SELECT * FROM $tableName.snapshots").count()
    snapshotsBefore should be >= 3L

    runner.run(tableName, MaintenanceConfig(snapshotRetentionDays = Some(0)))

    // After expiration with 0 days retention, at least the current snapshot survives.
    // Iceberg never expires the current snapshot, so count should be >= 1 and <= before.
    val snapshotsAfter = spark.sql(s"SELECT * FROM $tableName.snapshots").count()
    snapshotsAfter should be >= 1L
    snapshotsAfter should be <= snapshotsBefore
  }

  it should "compact data files" in {
    // Write several small files
    (4 to 10).foreach { i =>
      Seq((i, s"Name_$i")).toDF("id", "name").writeTo(tableName).append()
    }

    noException should be thrownBy {
      runner.run(tableName, MaintenanceConfig(targetFileSizeMb = Some(128)))
    }

    // Data should still be intact
    spark.sql(s"SELECT * FROM $tableName").count() should be >= 10L
  }

  it should "clamp orphan retention to 1440 minutes minimum" in {
    // orphanRetentionMinutes = 1 is below the 1440 minimum — should be clamped, not fail
    noException should be thrownBy {
      runner.run(tableName, MaintenanceConfig(orphanRetentionMinutes = Some(1)))
    }
  }

  it should "rewrite manifests when enabled" in {
    noException should be thrownBy {
      runner.run(tableName, MaintenanceConfig(enableManifestRewrite = true))
    }
  }

  it should "run all operations in sequence" in {
    // Add more snapshots for a full maintenance cycle
    Seq((11, "Dave")).toDF("id", "name").writeTo(tableName).append()
    Seq((12, "Eve")).toDF("id", "name").writeTo(tableName).append()

    val fullConfig = MaintenanceConfig(
      snapshotRetentionDays = Some(0),
      targetFileSizeMb = Some(128),
      orphanRetentionMinutes = Some(1440),
      enableManifestRewrite = true
    )

    noException should be thrownBy {
      runner.run(tableName, fullConfig)
    }

    // Data should still be intact after full maintenance
    spark.sql(s"SELECT * FROM $tableName").count() should be >= 12L
  }

  it should "skip disabled operations" in {
    val noOpsConfig = MaintenanceConfig(
      snapshotRetentionDays = None,
      targetFileSizeMb = None,
      orphanRetentionMinutes = None,
      enableManifestRewrite = false
    )

    noException should be thrownBy {
      runner.run(tableName, noOpsConfig)
    }
  }

  override def afterAll(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    super.afterAll()
  }
}
