package com.etl.framework.iceberg

import com.etl.framework.config.{IcebergConfig, MaintenanceConfig}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}
import java.time.{Clock, Instant, ZoneOffset}

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

  private val clock = Clock.fixed(Instant.parse("2100-01-01T00:00:00Z"), ZoneOffset.UTC)
  private val runner = new IcebergMaintenanceRunner(spark, icebergConfig, clock)
  private val tableName = "maint_catalog.default.maint_test"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE TABLE $tableName (id INT, name STRING) USING iceberg")
    Seq((1, "Alice")).toDF("id", "name").writeTo(tableName).append()
    Seq((2, "Bob")).toDF("id", "name").writeTo(tableName).append()
    Seq((3, "Charlie")).toDF("id", "name").writeTo(tableName).append()
  }

  "IcebergMaintenanceRunner" should "expire snapshots and expose every procedure metric" in {
    val result = runner.run(tableName, config(snapshotRetentionDays = Some(1)))

    result.status shouldBe MaintenanceStatus.Succeeded
    result.tableUuid should not be empty
    result.startingSnapshotId should not be empty
    result.actions.map(_.action) shouldBe Seq(MaintenanceAction.ExpireSnapshots)
    result.actions.head.status shouldBe MaintenanceActionStatus.Succeeded
    result.actions.head.metrics.keySet shouldBe Set(
      "deleted_data_files_count",
      "deleted_position_delete_files_count",
      "deleted_equality_delete_files_count",
      "deleted_manifest_files_count",
      "deleted_manifest_lists_count"
    )
    spark.sql(s"SELECT * FROM $tableName.snapshots").count() should be >= 1L
  }

  it should "compact data files and preserve table contents" in {
    (4 to 10).foreach { i =>
      Seq((i, s"Name_$i")).toDF("id", "name").writeTo(tableName).append()
    }

    val result = runner.run(tableName, config(targetFileSizeMb = Some(128)))

    result.status shouldBe MaintenanceStatus.Succeeded
    result.actions.head.metrics.keySet shouldBe Set(
      "rewritten_data_files_count",
      "added_data_files_count",
      "rewritten_bytes_count",
      "failed_data_files_count",
      "removed_delete_files_count"
    )
    spark.sql(s"SELECT * FROM $tableName").count() should be >= 10L
  }

  it should "reject unsafe orphan retention instead of silently changing it" in {
    val exception = intercept[IllegalArgumentException] {
      runner.run(tableName, config(orphanRetentionMinutes = Some(1439)))
    }

    exception.getMessage should include("at least 1440")
  }

  it should "remove eligible orphan files and return the deleted-file count" in {
    val result = runner.run(tableName, config(orphanRetentionMinutes = Some(4320)))

    result.status shouldBe MaintenanceStatus.Succeeded
    result.actions.map(_.action) shouldBe Seq(MaintenanceAction.RemoveOrphanFiles)
    result.actions.head.metrics.keySet shouldBe Set("deleted_orphan_files_count")
  }

  it should "rewrite manifests and return action metrics" in {
    val result = runner.run(tableName, config(enableManifestRewrite = true))

    result.status shouldBe MaintenanceStatus.Succeeded
    result.actions.head.metrics.keySet shouldBe Set("rewritten_manifests_count", "added_manifests_count")
  }

  it should "return FAILED before mutation when the table cannot be loaded" in {
    val result = runner.run("maint_catalog.default.missing_table", config(enableManifestRewrite = true))

    result.status shouldBe MaintenanceStatus.Failed
    result.success shouldBe false
    result.tableUuid shouldBe empty
    result.startingSnapshotId shouldBe empty
    result.actions.map(_.status) shouldBe Seq(MaintenanceActionStatus.NotAttempted)
    result.error should not be empty
  }

  it should "stop after the first uncertain action and retain completed action evidence" in {
    val invoked = Seq.newBuilder[MaintenanceAction]
    val failingRunner = new IcebergMaintenanceRunner(spark, icebergConfig, clock) {
      override protected[iceberg] def executeAction(
          action: MaintenanceAction,
          targetTable: String,
          maintenanceConfig: MaintenanceConfig
      ): Map[String, Long] = {
        invoked += action
        action match {
          case MaintenanceAction.ExpireSnapshots => Map("deleted_data_files_count" -> 2L)
          case MaintenanceAction.RewriteDataFiles =>
            throw new RuntimeException("password=super-secret")
          case _ => fail("Later maintenance actions must not run after an unknown outcome")
        }
      }
    }
    val result = failingRunner.run(
      tableName,
      config(
        snapshotRetentionDays = Some(7),
        targetFileSizeMb = Some(128),
        orphanRetentionMinutes = Some(1440),
        enableManifestRewrite = true
      )
    )

    invoked.result() shouldBe Seq(MaintenanceAction.ExpireSnapshots, MaintenanceAction.RewriteDataFiles)
    result.status shouldBe MaintenanceStatus.Unknown
    result.actions.map(_.status) shouldBe Seq(
      MaintenanceActionStatus.Succeeded,
      MaintenanceActionStatus.Unknown,
      MaintenanceActionStatus.NotAttempted,
      MaintenanceActionStatus.NotAttempted
    )
    result.actions(1).error.get should include("[REDACTED]")
    result.actions(1).error.get should not include "super-secret"
  }

  it should "require an explicit enable flag and at least one valid action" in {
    val disabled = config(enableManifestRewrite = true).copy(enabled = false)
    intercept[IllegalArgumentException](runner.run(tableName, disabled)).getMessage should include(
      "maintenance.enabled"
    )

    val noActions = config()
    intercept[IllegalArgumentException](runner.run(tableName, noActions)).getMessage should include(
      "at least one maintenance action"
    )

    val invalidSize = config(targetFileSizeMb = Some(0))
    intercept[IllegalArgumentException](runner.run(tableName, invalidSize)).getMessage should include(
      "targetFileSizeMb"
    )
  }

  private def config(
      snapshotRetentionDays: Option[Int] = None,
      targetFileSizeMb: Option[Int] = None,
      orphanRetentionMinutes: Option[Int] = None,
      enableManifestRewrite: Boolean = false
  ): MaintenanceConfig =
    MaintenanceConfig(
      enabled = true,
      snapshotRetentionDays = snapshotRetentionDays,
      targetFileSizeMb = targetFileSizeMb,
      orphanRetentionMinutes = orphanRetentionMinutes,
      enableManifestRewrite = enableManifestRewrite
    )

  override def afterAll(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    super.afterAll()
  }
}
