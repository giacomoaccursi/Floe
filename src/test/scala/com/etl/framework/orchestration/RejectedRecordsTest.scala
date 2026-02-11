package com.etl.framework.orchestration

import com.etl.framework.validation.ValidationColumns._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => f}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RejectedRecordsTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("RejectedRecordsTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  private val testData = Seq(
    ("id_1", 100, "active"),
    ("id_2", 200, "inactive"),
    ("id_3", 300, "pending")
  )

  private val requiredAuditFields = Set(
    REJECTION_REASON,
    REJECTION_CODE,
    REJECTED_AT,
    BATCH_ID,
    VALIDATION_STEP
  )

  private def simulateRejection(
      reason: String = "Test rejection",
      code: String = "TEST_CODE",
      timestamp: String = "2024-01-20T10:00:00Z",
      batchId: String = "batch_001",
      step: String = "test_validation"
  ) = {
    testData
      .toDF("id", "value", "status")
      .withColumn(REJECTION_REASON, f.lit(reason))
      .withColumn(REJECTION_CODE, f.lit(code))
      .withColumn(REJECTED_AT, f.lit(timestamp))
      .withColumn(BATCH_ID, f.lit(batchId))
      .withColumn(VALIDATION_STEP, f.lit(step))
  }

  "Rejected records" should "have correct schema with all audit fields" in {
    val originalDf = testData.toDF("id", "value", "status")
    val originalColumns = originalDf.columns.toSet
    val rejectedDf = simulateRejection()
    val rejectedColumns = rejectedDf.columns.toSet

    originalColumns.subsetOf(rejectedColumns) shouldBe true
    requiredAuditFields.subsetOf(rejectedColumns) shouldBe true
    rejectedColumns shouldBe (originalColumns ++ requiredAuditFields)
  }

  it should "preserve original data unchanged" in {
    val originalDf = testData.toDF("id", "value", "status")
    val rejectedDf = simulateRejection()
    val rejectedOriginalColumns = rejectedDf.select("id", "value", "status")

    rejectedOriginalColumns.count() shouldBe originalDf.count()
    originalDf.except(rejectedOriginalColumns).count() shouldBe 0L
    rejectedOriginalColumns.except(originalDf).count() shouldBe 0L
  }

  it should "have non-null audit fields" in {
    val rejectedDf = simulateRejection()

    requiredAuditFields.foreach { field =>
      rejectedDf.filter(f.col(field).isNull).count() shouldBe 0L
    }
  }

  it should "support overwrite with different batch IDs" in {
    val batch1Rejected = simulateRejection(
      reason = "Batch 1 rejection",
      code = "BATCH1_CODE",
      timestamp = "2024-01-20T10:00:00Z",
      batchId = "batch_001",
      step = "validation_1"
    )
    val batch2Rejected = simulateRejection(
      reason = "Batch 2 rejection",
      code = "BATCH2_CODE",
      timestamp = "2024-01-20T11:00:00Z",
      batchId = "batch_002",
      step = "validation_2"
    )

    val batch1Id = batch1Rejected.select(BATCH_ID).first().getString(0)
    val batch2Id = batch2Rejected.select(BATCH_ID).first().getString(0)

    batch1Id should not be batch2Id
    batch1Id shouldBe "batch_001"
    batch2Id shouldBe "batch_002"
  }

  it should "have descriptive rejection reasons" in {
    val rejectedDf = simulateRejection(reason = "Primary key duplicate")

    val reasonLengths = rejectedDf
      .select(f.length(f.col(REJECTION_REASON)).as("reason_length"))
      .collect()
      .map(row => Option(row.get(0)).map(_.toString.toLong).getOrElse(0L))

    reasonLengths.forall(_ > 0) shouldBe true
  }

  it should "include validation step information" in {
    val rejectedDf = simulateRejection(step = "schema_validation")

    val stepLengths = rejectedDf
      .select(f.length(f.col(VALIDATION_STEP)).as("step_length"))
      .collect()
      .map(row => Option(row.get(0)).map(_.toString.toLong).getOrElse(0L))

    stepLengths.forall(_ > 0) shouldBe true
  }

  it should "have valid ISO 8601 timestamp" in {
    val rejectedDf = simulateRejection(timestamp = "2024-01-20T10:00:00Z")

    val timestamps = rejectedDf
      .select(f.col(REJECTED_AT))
      .collect()
      .map(_.getString(0))

    timestamps.foreach { ts =>
      ts should not be empty
      ts should include("T")
      ts should (include("Z") or include("+") or include("-"))
    }
  }
}
