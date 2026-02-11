package com.etl.framework.merge

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class AppendMergeRecord(id: Int, name: String, value: Double)

class AppendMergeTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("AppendMergeTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  private val existingRecords = Seq(
    AppendMergeRecord(1, "alice", 100.0),
    AppendMergeRecord(2, "bob", 200.0),
    AppendMergeRecord(3, "charlie", 300.0)
  )

  private val newRecords = Seq(
    AppendMergeRecord(4, "diana", 400.0),
    AppendMergeRecord(5, "eve", 500.0)
  )

  "Append merge" should "contain all records from both datasets" in {
    val existingDf = existingRecords.toDF()
    val newDf = newRecords.toDF()

    val merger = new AppendMerger()
    val result = merger.merge(newDf, Some(existingDf))

    result.count() shouldBe 5L
  }

  it should "preserve existing records" in {
    val existingDf = existingRecords.toDF()
    val newDf = newRecords.toDF()

    val merger = new AppendMerger()
    val result = merger.merge(newDf, Some(existingDf))

    existingRecords.foreach { record =>
      result
        .filter(
          $"id" === record.id && $"name" === record.name && $"value" === record.value
        )
        .count() should be >= 1L
    }
  }

  it should "include new records" in {
    val existingDf = existingRecords.toDF()
    val newDf = newRecords.toDF()

    val merger = new AppendMerger()
    val result = merger.merge(newDf, Some(existingDf))

    newRecords.foreach { record =>
      result
        .filter(
          $"id" === record.id && $"name" === record.name && $"value" === record.value
        )
        .count() should be >= 1L
    }
  }

  it should "not modify records" in {
    val existingDf = existingRecords.toDF()
    val newDf = newRecords.toDF()

    val merger = new AppendMerger()
    val result = merger.merge(newDf, Some(existingDf))

    val resultRecords = result.as[AppendMergeRecord].collect()
    val allSourceRecords = existingRecords ++ newRecords

    resultRecords.foreach { r =>
      allSourceRecords.exists(s =>
        s.id == r.id && s.name == r.name && s.value == r.value
      ) shouldBe true
    }
  }

  it should "allow duplicates" in {
    val duplicateExisting = (1 to 10).map(i =>
      AppendMergeRecord(i % 3, s"existing_$i", i.toDouble)
    )
    val duplicateNew = (1 to 10).map(i =>
      AppendMergeRecord(i % 3, s"new_$i", (i * 10).toDouble)
    )

    val existingDf = duplicateExisting.toDF()
    val newDf = duplicateNew.toDF()

    val merger = new AppendMerger()
    val result = merger.merge(newDf, Some(existingDf))

    result.count() shouldBe 20L
    result.select("id").distinct().count() should be < result.count()
  }

  it should "return new data on first load" in {
    val newDf = newRecords.toDF()

    val merger = new AppendMerger()
    val result = merger.merge(newDf, None)

    result.count() shouldBe newDf.count()
    result.schema shouldBe newDf.schema
  }

  it should "preserve schema" in {
    val existingDf = existingRecords.toDF()
    val newDf = newRecords.toDF()

    val merger = new AppendMerger()
    val result = merger.merge(newDf, Some(existingDf))

    result.schema shouldBe newDf.schema
    result.columns.toSet shouldBe newDf.columns.toSet
  }

  it should "handle empty existing dataset" in {
    val newDf = newRecords.toDF()
    val emptyDf = Seq.empty[AppendMergeRecord].toDF()

    val merger = new AppendMerger()
    val result = merger.merge(newDf, Some(emptyDf))

    result.count() shouldBe newRecords.size
  }

  it should "handle empty new dataset" in {
    val existingDf = existingRecords.toDF()
    val emptyDf = Seq.empty[AppendMergeRecord].toDF()

    val merger = new AppendMerger()
    val result = merger.merge(emptyDf, Some(existingDf))

    result.count() shouldBe existingRecords.size
  }

  it should "produce same count regardless of append order" in {
    val dfA = existingRecords.toDF()
    val dfB = newRecords.toDF()

    val merger = new AppendMerger()
    val resultAB = merger.merge(dfB, Some(dfA))
    val resultBA = merger.merge(dfA, Some(dfB))

    resultAB.count() shouldBe resultBA.count()
  }
}
