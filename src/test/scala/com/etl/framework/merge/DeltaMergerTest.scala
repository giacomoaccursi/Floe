package com.etl.framework.merge

import com.etl.framework.config.LoadModeConfig
import com.etl.framework.exceptions.{MergeException, UnsupportedOperationException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class DeltaMergerTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DeltaMergerTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  // --- Factory tests ---

  "DeltaMergerFactory" should "create FullReplaceMerger for full load mode" in {
    val merger = DeltaMergerFactory.create(LoadModeConfig(`type` = "full"))
    merger shouldBe a[FullReplaceMerger]
  }

  it should "create UpsertMerger for delta upsert strategy" in {
    val merger = DeltaMergerFactory.create(LoadModeConfig(
      `type` = "delta",
      mergeStrategy = Some("upsert"),
      keyColumns = Seq("id")
    ))
    merger shouldBe a[UpsertMerger]
  }

  it should "create AppendMerger for delta append strategy" in {
    val merger = DeltaMergerFactory.create(LoadModeConfig(
      `type` = "delta",
      mergeStrategy = Some("append")
    ))
    merger shouldBe a[AppendMerger]
  }

  it should "create SCD2Merger for scd2 load mode" in {
    val merger = DeltaMergerFactory.create(LoadModeConfig(
      `type` = "scd2",
      keyColumns = Seq("id"),
      compareColumns = Seq("name", "value"),
      validFromColumn = Some("valid_from"),
      validToColumn = Some("valid_to"),
      isCurrentColumn = Some("is_current")
    ))
    merger shouldBe a[SCD2Merger]
  }

  it should "throw MergeException for invalid merge strategy" in {
    val ex = intercept[MergeException] {
      DeltaMergerFactory.create(LoadModeConfig(
        `type` = "delta",
        mergeStrategy = Some("invalid")
      ))
    }
    ex.mergeStrategy shouldBe "invalid"
  }

  it should "throw UnsupportedOperationException for unknown load mode type" in {
    intercept[UnsupportedOperationException] {
      DeltaMergerFactory.create(LoadModeConfig(`type` = "unknown"))
    }
  }

  it should "throw for scd2 without required fields" in {
    intercept[IllegalArgumentException] {
      DeltaMergerFactory.create(LoadModeConfig(
        `type` = "scd2",
        keyColumns = Seq("id"),
        compareColumns = Seq("name"),
        validFromColumn = None,
        validToColumn = Some("valid_to"),
        isCurrentColumn = Some("is_current")
      ))
    }
  }

  // --- FullReplaceMerger tests ---

  "FullReplaceMerger" should "return new data ignoring existing" in {
    val merger = new FullReplaceMerger()
    val newData = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val existing = Seq((3, "Charlie")).toDF("id", "name")

    val result = merger.merge(newData, Some(existing))
    result.count() shouldBe 2
    result.collect().map(_.getAs[String]("name")).sorted shouldBe Array("Alice", "Bob")
  }

  it should "return new data when no existing data" in {
    val merger = new FullReplaceMerger()
    val newData = Seq((1, "Alice")).toDF("id", "name")

    val result = merger.merge(newData, None)
    result.count() shouldBe 1
  }

  // --- AppendMerger tests ---

  "AppendMerger" should "append new data to existing" in {
    val merger = new AppendMerger()
    val existing = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val newData = Seq((3, "Charlie")).toDF("id", "name")

    val result = merger.merge(newData, Some(existing))
    result.count() shouldBe 3
  }

  it should "return new data when no existing data" in {
    val merger = new AppendMerger()
    val newData = Seq((1, "Alice")).toDF("id", "name")

    val result = merger.merge(newData, None)
    result.count() shouldBe 1
  }

  it should "allow duplicate keys when appending" in {
    val merger = new AppendMerger()
    val existing = Seq((1, "Alice")).toDF("id", "name")
    val newData = Seq((1, "Alice_updated")).toDF("id", "name")

    val result = merger.merge(newData, Some(existing))
    result.count() shouldBe 2
    result.filter(col("id") === 1).count() shouldBe 2
  }

  // --- UpsertMerger tests ---

  "UpsertMerger" should "insert new records and update existing without timestamp" in {
    val merger = new UpsertMerger(Seq("id"), None)
    val existing = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val newData = Seq((2, "Bob_updated"), (3, "Charlie")).toDF("id", "name")

    val result = merger.merge(newData, Some(existing))
    result.count() shouldBe 3

    val names = result.orderBy("id").collect().map(_.getAs[String]("name"))
    names shouldBe Array("Alice", "Bob_updated", "Charlie")
  }

  it should "return new data when no existing data" in {
    val merger = new UpsertMerger(Seq("id"), None)
    val newData = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")

    val result = merger.merge(newData, None)
    result.count() shouldBe 2
  }

  it should "keep most recent record when using timestamp" in {
    val merger = new UpsertMerger(Seq("id"), Some("ts"))
    val existing = Seq((1, "Alice_old", 1L), (2, "Bob", 1L)).toDF("id", "name", "ts")
    val newData = Seq((1, "Alice_new", 2L), (3, "Charlie", 1L)).toDF("id", "name", "ts")

    val result = merger.merge(newData, Some(existing))
    result.count() shouldBe 3

    val aliceRecord = result.filter(col("id") === 1).collect().head
    aliceRecord.getAs[String]("name") shouldBe "Alice_new"
  }

  it should "require non-empty key columns" in {
    intercept[IllegalArgumentException] {
      new UpsertMerger(Seq.empty, None)
    }
  }

  it should "handle composite key columns" in {
    val merger = new UpsertMerger(Seq("id", "category"), None)
    val existing = Seq((1, "A", "old"), (1, "B", "old")).toDF("id", "category", "value")
    val newData = Seq((1, "A", "new"), (2, "A", "new")).toDF("id", "category", "value")

    val result = merger.merge(newData, Some(existing))
    result.count() shouldBe 3

    val updated = result.filter(col("id") === 1 && col("category") === "A").collect().head
    updated.getAs[String]("value") shouldBe "new"

    val unchanged = result.filter(col("id") === 1 && col("category") === "B").collect().head
    unchanged.getAs[String]("value") shouldBe "old"
  }

  // --- SCD2Merger tests ---

  "SCD2Merger" should "add SCD2 columns on first load" in {
    val merger = new SCD2Merger(Seq("id"), Seq("name"), "valid_from", "valid_to", "is_current")
    val newData = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")

    val result = merger.merge(newData, None)
    result.count() shouldBe 2
    result.columns should contain allOf ("valid_from", "valid_to", "is_current")

    // All records should be current
    result.filter(col("is_current") === true).count() shouldBe 2
    result.filter(col("valid_to").isNull).count() shouldBe 2
  }

  it should "close old version and add new version for changed records" in {
    val merger = new SCD2Merger(Seq("id"), Seq("name"), "valid_from", "valid_to", "is_current")

    // Existing data with SCD2 columns
    val existing = Seq((1, "Alice", java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), null.asInstanceOf[java.sql.Timestamp], true))
      .toDF("id", "name", "valid_from", "valid_to", "is_current")

    val newData = Seq((1, "Alice_updated")).toDF("id", "name")

    val result = merger.merge(newData, Some(existing))

    // Should have 2 records: closed old + new current
    result.count() shouldBe 2
    result.filter(col("is_current") === true).count() shouldBe 1
    result.filter(col("is_current") === false).count() shouldBe 1

    // Current version should have new name
    val current = result.filter(col("is_current") === true).collect().head
    current.getAs[String]("name") shouldBe "Alice_updated"
  }

  it should "keep unchanged records as-is" in {
    val merger = new SCD2Merger(Seq("id"), Seq("name"), "valid_from", "valid_to", "is_current")

    val existing = Seq((1, "Alice", java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), null.asInstanceOf[java.sql.Timestamp], true))
      .toDF("id", "name", "valid_from", "valid_to", "is_current")

    // Same data - no changes
    val newData = Seq((1, "Alice")).toDF("id", "name")

    val result = merger.merge(newData, Some(existing))
    result.count() shouldBe 1
    result.filter(col("is_current") === true).count() shouldBe 1
  }

  it should "add new records that don't exist in existing data" in {
    val merger = new SCD2Merger(Seq("id"), Seq("name"), "valid_from", "valid_to", "is_current")

    val existing = Seq((1, "Alice", java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), null.asInstanceOf[java.sql.Timestamp], true))
      .toDF("id", "name", "valid_from", "valid_to", "is_current")

    // Include existing record + new record (SCD2 expects full snapshot)
    val newData = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")

    val result = merger.merge(newData, Some(existing))
    result.count() shouldBe 2

    // Both should be current
    result.filter(col("is_current") === true).count() shouldBe 2
  }

  it should "require non-empty key and compare columns" in {
    intercept[IllegalArgumentException] {
      new SCD2Merger(Seq.empty, Seq("name"), "valid_from", "valid_to", "is_current")
    }
    intercept[IllegalArgumentException] {
      new SCD2Merger(Seq("id"), Seq.empty, "valid_from", "valid_to", "is_current")
    }
  }
}
