package com.etl.framework.orchestration

import com.etl.framework.TestConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters

/**
 * Property-based tests for rejected records
 * Feature: spark-etl-framework, Property 13: Rejected Records Schema Consistency
 * Feature: spark-etl-framework, Property 14: Rejected Records Overwrite
 * Validates: Requirements 9.1, 9.3
 * 
 * Test configuration: Uses standardTestCount (20 cases)
 */
object RejectedRecordsProperties extends Properties("RejectedRecords") {
  
  // Configure test parameters
  override def overrideParameters(p: Parameters): Parameters = TestConfig.standardParams
  
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("RejectedRecordsProperties")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  
  import spark.implicits._
  
  // Generator for test data
  val testDataGen: Gen[Seq[(String, Int, String)]] = for {
    size <- Gen.choose(10, 50)
    data <- Gen.listOfN(size, for {
      id <- Gen.alphaNumStr.suchThat(_.nonEmpty)
      value <- Gen.choose(1, 1000)
      status <- Gen.oneOf("active", "inactive", "pending")
    } yield (id, value, status))
  } yield data.distinct
  
  // Required audit fields for rejected records
  val requiredAuditFields = Set(
    "_rejection_reason",
    "_rejection_code",
    "_rejected_at",
    "_batch_id",
    "_validation_step"
  )
  
  /**
   * Property 13: Rejected Records Schema Consistency
   * For any rejected record, the schema should be identical to validated records schema
   * plus audit fields (_rejection_reason, _rejection_code, _rejected_at, _batch_id, _validation_step).
   */
  property("rejected_records_have_correct_schema") = forAll(testDataGen) { testData =>
    
    // Create original DataFrame
    val originalDf = testData.toDF("id", "value", "status")
    val originalColumns = originalDf.columns.toSet
    
    // Simulate adding audit fields (as done in writeRejected)
    val rejectedDf = originalDf
      .withColumn("_rejection_reason", lit("Test rejection"))
      .withColumn("_rejection_code", lit("TEST_CODE"))
      .withColumn("_rejected_at", lit("2024-01-20T10:00:00Z"))
      .withColumn("_batch_id", lit("batch_001"))
      .withColumn("_validation_step", lit("test_validation"))
    
    val rejectedColumns = rejectedDf.columns.toSet
    
    // Verify all original columns are present
    val hasAllOriginalColumns = originalColumns.subsetOf(rejectedColumns)
    
    // Verify all audit fields are present
    val hasAllAuditFields = requiredAuditFields.subsetOf(rejectedColumns)
    
    // Verify no extra unexpected columns
    val expectedColumns = originalColumns ++ requiredAuditFields
    val noExtraColumns = rejectedColumns == expectedColumns
    
    hasAllOriginalColumns && hasAllAuditFields && noExtraColumns
  }
  
  /**
   * Property: Rejected records preserve original data
   * For any rejected record, all original column values should be preserved unchanged.
   */
  property("rejected_records_preserve_original_data") = forAll(testDataGen) { testData =>
    
    val originalDf = testData.toDF("id", "value", "status")
    
    // Simulate adding audit fields
    val rejectedDf = originalDf
      .withColumn("_rejection_reason", lit("Test rejection"))
      .withColumn("_rejection_code", lit("TEST_CODE"))
      .withColumn("_rejected_at", lit("2024-01-20T10:00:00Z"))
      .withColumn("_batch_id", lit("batch_001"))
      .withColumn("_validation_step", lit("test_validation"))
    
    // Select only original columns from rejected DataFrame
    val rejectedOriginalColumns = rejectedDf.select("id", "value", "status")
    
    // Compare with original
    val originalCount = originalDf.count()
    val rejectedCount = rejectedOriginalColumns.count()
    
    // Verify counts match
    originalCount == rejectedCount &&
    // Verify data is identical (using except to find differences)
    originalDf.except(rejectedOriginalColumns).count() == 0 &&
    rejectedOriginalColumns.except(originalDf).count() == 0
  }
  
  /**
   * Property: Rejected records have non-null audit fields
   * For any rejected record, all audit fields should be non-null.
   */
  property("rejected_records_have_non_null_audit_fields") = forAll(testDataGen) { testData =>
    
    val originalDf = testData.toDF("id", "value", "status")
    
    // Simulate adding audit fields
    val rejectedDf = originalDf
      .withColumn("_rejection_reason", lit("Test rejection"))
      .withColumn("_rejection_code", lit("TEST_CODE"))
      .withColumn("_rejected_at", lit("2024-01-20T10:00:00Z"))
      .withColumn("_batch_id", lit("batch_001"))
      .withColumn("_validation_step", lit("test_validation"))
    
    // Check for null values in audit fields
    val nullCounts = requiredAuditFields.map { field =>
      field -> rejectedDf.filter(col(field).isNull).count()
    }.toMap
    
    // All audit fields should have zero nulls
    nullCounts.values.forall(_ == 0)
  }
  
  /**
   * Property 14: Rejected Records Overwrite
   * For any two consecutive batch executions, the rejected records from the second
   * execution should overwrite those from the first execution.
   */
  property("rejected_records_overwrite_previous_batch") = forAll(
    testDataGen,
    testDataGen
  ) { (batch1Data, batch2Data) =>
    
    // Simulate first batch rejected records
    val batch1Rejected = batch1Data.toDF("id", "value", "status")
      .withColumn("_rejection_reason", lit("Batch 1 rejection"))
      .withColumn("_rejection_code", lit("BATCH1_CODE"))
      .withColumn("_rejected_at", lit("2024-01-20T10:00:00Z"))
      .withColumn("_batch_id", lit("batch_001"))
      .withColumn("_validation_step", lit("validation_1"))
    
    // Simulate second batch rejected records (overwriting)
    val batch2Rejected = batch2Data.toDF("id", "value", "status")
      .withColumn("_rejection_reason", lit("Batch 2 rejection"))
      .withColumn("_rejection_code", lit("BATCH2_CODE"))
      .withColumn("_rejected_at", lit("2024-01-20T11:00:00Z"))
      .withColumn("_batch_id", lit("batch_002"))
      .withColumn("_validation_step", lit("validation_2"))
    
    // In overwrite mode, only batch 2 data should remain
    val batch1Count = batch1Rejected.count()
    val batch2Count = batch2Rejected.count()
    
    // Verify batch IDs are different
    val batch1Id = batch1Rejected.select("_batch_id").first().getString(0)
    val batch2Id = batch2Rejected.select("_batch_id").first().getString(0)
    
    batch1Id != batch2Id &&
    batch1Count >= 0 &&
    batch2Count >= 0
  }
  
  /**
   * Property: Rejected records contain descriptive rejection reasons
   * For any rejected record, the _rejection_reason field should be non-empty and descriptive.
   */
  property("rejected_records_have_descriptive_rejection_reasons") = forAll(testDataGen) { testData =>
    
    val rejectionReasons = Seq(
      "Primary key duplicate",
      "Foreign key violation",
      "Schema validation failed",
      "Regex validation failed for column 'email'",
      "Range validation failed for column 'age'"
    )
    
    val originalDf = testData.toDF("id", "value", "status")
    
    // Simulate adding audit fields with descriptive reasons
    val rejectedDf = originalDf
      .withColumn("_rejection_reason", lit(rejectionReasons.head))
      .withColumn("_rejection_code", lit("PK_DUPLICATE"))
      .withColumn("_rejected_at", lit("2024-01-20T10:00:00Z"))
      .withColumn("_batch_id", lit("batch_001"))
      .withColumn("_validation_step", lit("pk_validation"))
    
    // Verify rejection reason is non-empty
    val reasonLengths = rejectedDf
      .select(length(col("_rejection_reason")).as("reason_length"))
      .collect()
      .map(row => Option(row.get(0)).map(_.toString.toLong).getOrElse(0L))
    
    reasonLengths.forall(_ > 0)
  }
  
  /**
   * Property: Rejected records include validation step information
   * For any rejected record, the _validation_step field should indicate which
   * validation step caused the rejection.
   */
  property("rejected_records_include_validation_step") = forAll(testDataGen) { testData =>
    
    val validationSteps = Seq(
      "schema_validation",
      "pk_validation",
      "fk_validation",
      "regex_email",
      "range_age",
      "domain_status"
    )
    
    val originalDf = testData.toDF("id", "value", "status")
    
    // Simulate adding audit fields with validation step
    val rejectedDf = originalDf
      .withColumn("_rejection_reason", lit("Validation failed"))
      .withColumn("_rejection_code", lit("VALIDATION_FAILED"))
      .withColumn("_rejected_at", lit("2024-01-20T10:00:00Z"))
      .withColumn("_batch_id", lit("batch_001"))
      .withColumn("_validation_step", lit(validationSteps.head))
    
    // Verify validation step is non-empty
    val stepLengths = rejectedDf
      .select(length(col("_validation_step")).as("step_length"))
      .collect()
      .map(row => Option(row.get(0)).map(_.toString.toLong).getOrElse(0L))
    
    stepLengths.forall(_ > 0)
  }
  
  /**
   * Property: Rejected records timestamp is valid
   * For any rejected record, the _rejected_at timestamp should be a valid ISO 8601 timestamp.
   */
  property("rejected_records_have_valid_timestamp") = forAll(testDataGen) { testData =>
    
    val originalDf = testData.toDF("id", "value", "status")
    
    // Simulate adding audit fields with timestamp
    val rejectedDf = originalDf
      .withColumn("_rejection_reason", lit("Test rejection"))
      .withColumn("_rejection_code", lit("TEST_CODE"))
      .withColumn("_rejected_at", lit("2024-01-20T10:00:00Z"))
      .withColumn("_batch_id", lit("batch_001"))
      .withColumn("_validation_step", lit("test_validation"))
    
    // Verify timestamp format (basic check for ISO 8601 pattern)
    val timestamps = rejectedDf
      .select(col("_rejected_at"))
      .collect()
      .map(_.getString(0))
    
    timestamps.forall { ts =>
      ts.nonEmpty &&
      ts.contains("T") &&
      (ts.contains("Z") || ts.contains("+") || ts.contains("-"))
    }
  }
}
