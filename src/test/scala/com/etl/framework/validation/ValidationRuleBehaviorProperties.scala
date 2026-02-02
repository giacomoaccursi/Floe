package com.etl.framework.validation

import com.etl.framework.config._
import com.etl.framework.TestConfig
import com.etl.framework.validation.ValidationColumns._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters
import scala.util.Try

/**
 * Property-based tests for validation rule behaviors (reject vs warn)
 * Feature: spark-etl-framework, Property 11: Validation Rule Reject Behavior
 * Feature: spark-etl-framework, Property 12: Validation Rule Warn Behavior
 * Validates: Requirements 8.5, 8.6
 * 
 * Test configuration: Uses standardTestCount (20 cases)
 */
object ValidationRuleBehaviorProperties extends Properties("ValidationRuleBehavior") {
  
  // Configure test parameters
  override def overrideParameters(p: Parameters): Parameters = TestConfig.standardParams
  
  // Create a local Spark session for testing
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("ValidationRuleBehaviorPropertiesTest")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  
  import spark.implicits._
  
  // Test record for validation
  case class TestRecord(
    id: Int,
    email: String,
    age: Int,
    status: String
  )
  
  // Generator for valid email addresses
  val validEmailGen: Gen[String] = for {
    username <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    domain <- Gen.oneOf("example.com", "test.org", "demo.net")
  } yield s"$username@$domain"
  
  // Generator for invalid email addresses (no @ symbol)
  val invalidEmailGen: Gen[String] = for {
    str <- Gen.alphaNumStr.suchThat(_.nonEmpty)
  } yield str
  
  // Generator for test records with controllable email validity
  def testRecordGen(validEmail: Boolean): Gen[TestRecord] = for {
    id <- Gen.choose(1, 10000)
    email <- if (validEmail) validEmailGen else invalidEmailGen
    age <- Gen.choose(18, 100)
    status <- Gen.oneOf("active", "inactive", "pending")
  } yield TestRecord(id, email, age, status)
  
  // Generator for mixed valid/invalid records
  val mixedRecordsGen: Gen[(Seq[TestRecord], Int)] = for {
    validCount <- Gen.choose(5, 30)
    invalidCount <- Gen.choose(1, 20)
    validRecords <- Gen.listOfN(validCount, testRecordGen(validEmail = true))
    invalidRecords <- Gen.listOfN(invalidCount, testRecordGen(validEmail = false))
  } yield {
    // Ensure unique IDs to avoid PK conflicts
    val allRecords = (validRecords ++ invalidRecords).zipWithIndex.map { 
      case (rec, idx) => rec.copy(id = idx + 1) 
    }
    (allRecords, invalidCount)
  }
  
  // Helper to create DataFrame from test records
  def createDataFrame(records: Seq[TestRecord]): DataFrame = {
    records.toDF()
  }
  
  // Helper to create flow config with regex validation rule
  def flowConfigWithRegexRule(onFailure: String): FlowConfig = {
    FlowConfig(
      name = "test_flow",
      description = "Test flow with regex validation",
      version = "1.0",
      owner = "test",
      source = SourceConfig(
        `type` = "file",
        path = "/test",
        format = "csv",
        options = Map.empty,
        filePattern = None
      ),
      schema = SchemaConfig(
        enforceSchema = true,
        allowExtraColumns = false,
        columns = Seq(
          ColumnConfig("id", "int", nullable = false, None, "ID column"),
          ColumnConfig("email", "string", nullable = false, None, "Email column"),
          ColumnConfig("age", "int", nullable = false, None, "Age column"),
          ColumnConfig("status", "string", nullable = false, None, "Status column")
        )
      ),
      loadMode = LoadModeConfig(
        `type` = "full",
        keyColumns = Seq.empty
      ),
      validation = ValidationConfig(
        primaryKey = Seq("id"),
        foreignKeys = Seq.empty,
        rules = Seq(
          ValidationRule(
            `type` = "regex",
            column = Some("email"),
            pattern = Some("^[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+$"),
            description = Some("Email must be valid format"),
            skipNull = Some(false),
            onFailure = onFailure
          )
        )
      ),
      output = OutputConfig(
        path = None,
        rejectedPath = None,
        format = "parquet",
        partitionBy = Seq.empty,
        compression = "snappy",
        options = Map.empty
      )
    )
  }
  
  /**
   * Property 11: Validation Rule Reject Behavior
   * For any validation rule configured with on_failure: reject,
   * all records that fail the validation should be moved to rejected records
   */
  property("reject_behavior_moves_failed_records_to_rejected") = forAll(mixedRecordsGen) {
    case (records, _) =>
      Try {
        if (records.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)
          val flowConfig = flowConfigWithRegexRule(onFailure = "reject")
          
          // Count actual invalid emails in the input
          val emailPattern = "^[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+$"
          val actualInvalidCount = records.count(r => !r.email.matches(emailPattern))
          
          // Run validation
          val engine = new ValidationEngine()
          val result = engine.validate(df, flowConfig)
          
          // All invalid records should be rejected
          val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)
          val rejectedHasInvalidRecords = rejectedCount == actualInvalidCount
          
          // Valid records should not contain invalid emails
          val validHasNoInvalidEmails = {
            val validEmails = result.valid.select("email").collect().map(_.getString(0))
            validEmails.forall(email => email.matches(emailPattern))
          }
          
          // Rejected records should have rejection metadata
          val hasRejectionMetadata = if (actualInvalidCount > 0) {
            result.rejected.exists { rejectedDf =>
              rejectedDf.columns.contains(REJECTION_CODE) &&
              rejectedDf.columns.contains(REJECTION_REASON) &&
              rejectedDf.columns.contains(VALIDATION_STEP) &&
              rejectedDf.columns.contains(REJECTED_AT)
            }
          } else {
            true // No rejected records expected
          }
          
          // Rejection code should be REGEX_VALIDATION_FAILED
          val correctRejectionCode = if (actualInvalidCount > 0) {
            result.rejected.exists { rejectedDf =>
              rejectedDf.filter(col(REJECTION_CODE) === "REGEX_VALIDATION_FAILED").count() == actualInvalidCount
            }
          } else {
            true // No rejected records expected
          }
          
          rejectedHasInvalidRecords && validHasNoInvalidEmails && hasRejectionMetadata && correctRejectionCode
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 11: Validation Rule Reject Behavior - Record Count Invariant
   * For any validation with reject behavior, input_count = valid_count + rejected_count
   */
  property("reject_behavior_preserves_record_count") = forAll(mixedRecordsGen) {
    case (records, _) =>
      Try {
        if (records.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)
          val inputCount = df.count()
          val flowConfig = flowConfigWithRegexRule(onFailure = "reject")
          
          // Run validation
          val engine = new ValidationEngine()
          val result = engine.validate(df, flowConfig)
          
          val validCount = result.valid.count()
          val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)
          
          // Invariant: input = valid + rejected
          inputCount == validCount + rejectedCount
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 11: Validation Rule Reject Behavior - No Invalid Records in Valid Set
   * For any validation with reject behavior, valid records should not contain any that fail the rule
   */
  property("reject_behavior_no_invalid_in_valid") = forAll(mixedRecordsGen) {
    case (records, _) =>
      Try {
        if (records.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)
          val flowConfig = flowConfigWithRegexRule(onFailure = "reject")
          
          // Run validation
          val engine = new ValidationEngine()
          val result = engine.validate(df, flowConfig)
          
          // Check that all valid records pass the regex validation
          val emailPattern = "^[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+$"
          val allValidEmailsMatch = result.valid
            .filter(!col("email").rlike(emailPattern))
            .isEmpty
          
          allValidEmailsMatch
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 12: Validation Rule Warn Behavior
   * For any validation rule configured with on_failure: warn,
   * all records that fail the validation should pass with a warning added to the warnings column
   */
  property("warn_behavior_adds_warning_to_records") = forAll(mixedRecordsGen) {
    case (records, _) =>
      Try {
        if (records.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)
          val flowConfig = flowConfigWithRegexRule(onFailure = "warn")
          
          // Count actual invalid emails in the input
          val emailPattern = "^[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+$"
          val actualInvalidCount = records.count(r => !r.email.matches(emailPattern))
          
          // Run validation
          val engine = new ValidationEngine()
          val result = engine.validate(df, flowConfig)
          
          // No records should be rejected for regex validation
          val noRegexRejections = result.rejected.forall { rejectedDf =>
            rejectedDf.filter(col(REJECTION_CODE) === "REGEX_VALIDATION_FAILED").isEmpty
          }
          
          // All records should be in valid DataFrame (may be rejected for other reasons like PK)
          val allRecordsAccountedFor = {
            val validCount = result.valid.count()
            val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)
            validCount + rejectedCount == records.size
          }
          
          // Valid DataFrame should have warnings column
          val hasWarningsColumn = result.valid.columns.contains(WARNINGS)
          
          // Records with invalid emails should have warnings
          val invalidRecordsHaveWarnings = if (actualInvalidCount > 0) {
            val recordsWithInvalidEmails = result.valid
              .filter(!col("email").rlike(emailPattern))
            
            if (recordsWithInvalidEmails.isEmpty) {
              true // No invalid emails in valid set (might have been rejected for PK)
            } else {
              // Check that these records have non-empty warnings
              recordsWithInvalidEmails
                .filter(size(col(WARNINGS)) > 0)
                .count() == recordsWithInvalidEmails.count()
            }
          } else {
            true // No invalid emails to check
          }
          
          noRegexRejections && allRecordsAccountedFor && hasWarningsColumn && invalidRecordsHaveWarnings
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 12: Validation Rule Warn Behavior - Warning Content
   * For any validation with warn behavior, warnings should contain descriptive information
   */
  property("warn_behavior_warning_content_descriptive") = forAll(mixedRecordsGen) {
    case (records, _) =>
      Try {
        if (records.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)
          val flowConfig = flowConfigWithRegexRule(onFailure = "warn")
          
          // Count actual invalid emails in the input
          val emailPattern = "^[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+$"
          val actualInvalidCount = records.count(r => !r.email.matches(emailPattern))
          
          // Run validation
          val engine = new ValidationEngine()
          val result = engine.validate(df, flowConfig)
          
          // Find records with invalid emails
          val recordsWithInvalidEmails = result.valid
            .filter(!col("email").rlike(emailPattern))
          
          if (recordsWithInvalidEmails.isEmpty || actualInvalidCount == 0) {
            true // No invalid emails to check
          } else {
            // Check that warnings mention the validation issue
            val warningsAreDescriptive = recordsWithInvalidEmails
              .select(WARNINGS)
              .collect()
              .forall { row =>
                val warnings = row.getSeq[String](0)
                warnings.nonEmpty && warnings.exists { warning =>
                  warning.toLowerCase.contains("email") || 
                  warning.toLowerCase.contains("validation") ||
                  warning.toLowerCase.contains("pattern")
                }
              }
            
            warningsAreDescriptive
          }
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 12: Validation Rule Warn Behavior - Valid Records Have Empty Warnings
   * For any validation with warn behavior, records that pass should have empty warnings array
   */
  property("warn_behavior_valid_records_empty_warnings") = forAll(Gen.choose(5, 30)) { count =>
    Try {
      // Create records with all valid emails
      val validRecords = (1 to count).map { i =>
        TestRecord(i, s"user$i@example.com", 25 + i, "active")
      }
      
      val df = createDataFrame(validRecords)
      val flowConfig = flowConfigWithRegexRule(onFailure = "warn")
      
      // Run validation
      val engine = new ValidationEngine()
      val result = engine.validate(df, flowConfig)
      
      // All records should have empty warnings (or only warnings from other validations)
      val allHaveEmptyOrNoRegexWarnings = result.valid
        .select(WARNINGS)
        .collect()
        .forall { row =>
          val warnings = row.getSeq[String](0)
          // Warnings should be empty or not contain regex validation warnings
          warnings.isEmpty || !warnings.exists(w => 
            w.toLowerCase.contains("email") && w.toLowerCase.contains("pattern")
          )
        }
      
      allHaveEmptyOrNoRegexWarnings
    }.getOrElse(false)
  }
  
  /**
   * Property 12: Validation Rule Warn Behavior - Record Count Invariant
   * For any validation with warn behavior, input_count = valid_count + rejected_count
   * (rejected only for other validation failures, not for the warn rule)
   */
  property("warn_behavior_preserves_record_count") = forAll(mixedRecordsGen) {
    case (records, _) =>
      Try {
        if (records.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)
          val inputCount = df.count()
          val flowConfig = flowConfigWithRegexRule(onFailure = "warn")
          
          // Run validation
          val engine = new ValidationEngine()
          val result = engine.validate(df, flowConfig)
          
          val validCount = result.valid.count()
          val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)
          
          // Invariant: input = valid + rejected
          inputCount == validCount + rejectedCount
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 11 & 12: Multiple Rules with Different Behaviors
   * For any validation with multiple rules having different on_failure settings,
   * each rule should behave according to its configuration
   */
  property("multiple_rules_different_behaviors") = forAll(Gen.choose(5, 30)) { count =>
    Try {
      // Create records with some having invalid emails and some having invalid ages
      val records = (1 to count).map { i =>
        val email = if (i % 3 == 0) s"invalid_email_$i" else s"user$i@example.com"
        val age = if (i % 4 == 0) 150 else 25 + i // 150 is out of range
        TestRecord(i, email, age, "active")
      }
      
      val df = createDataFrame(records)
      
      // Create config with two rules: email (warn) and age (reject)
      val flowConfig = FlowConfig(
        name = "test_flow",
        description = "Test flow with multiple validation rules",
        version = "1.0",
        owner = "test",
        source = SourceConfig(
          `type` = "file",
          path = "/test",
          format = "csv",
          options = Map.empty,
          filePattern = None
        ),
        schema = SchemaConfig(
          enforceSchema = true,
          allowExtraColumns = false,
          columns = Seq(
            ColumnConfig("id", "int", nullable = false, None, "ID column"),
            ColumnConfig("email", "string", nullable = false, None, "Email column"),
            ColumnConfig("age", "int", nullable = false, None, "Age column"),
            ColumnConfig("status", "string", nullable = false, None, "Status column")
          )
        ),
        loadMode = LoadModeConfig(
          `type` = "full",
          keyColumns = Seq.empty
        ),
        validation = ValidationConfig(
          primaryKey = Seq("id"),
          foreignKeys = Seq.empty,
          rules = Seq(
            ValidationRule(
              `type` = "regex",
              column = Some("email"),
              pattern = Some("^[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+$"),
              description = Some("Email must be valid format"),
              skipNull = Some(false),
              onFailure = "warn"  // Warn for email
            ),
            ValidationRule(
              `type` = "range",
              column = Some("age"),
              min = Some("18"),
              max = Some("120"),
              description = Some("Age must be between 18 and 120"),
              skipNull = Some(false),
              onFailure = "reject"  // Reject for age
            )
          )
        ),
        output = OutputConfig(
          path = None,
          rejectedPath = None,
          format = "parquet",
          partitionBy = Seq.empty,
          compression = "snappy",
          options = Map.empty
        )
      )
      
      // Run validation
      val engine = new ValidationEngine()
      val result = engine.validate(df, flowConfig)
      
      // Records with invalid age should be rejected
      val ageRejectionsExist = result.rejected.exists { rejectedDf =>
        rejectedDf.filter(col(REJECTION_CODE) === "RANGE_VALIDATION_FAILED").count() > 0
      }
      
      // Records with invalid email but valid age should be in valid with warnings
      val emailWarningsExist = {
        val recordsWithInvalidEmailValidAge = result.valid
          .filter(!col("email").rlike("^[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+$"))
          .filter(col("age") >= 18 && col("age") <= 120)
        
        if (recordsWithInvalidEmailValidAge.isEmpty) {
          true // No such records
        } else {
          recordsWithInvalidEmailValidAge
            .filter(size(col(WARNINGS)) > 0)
            .count() == recordsWithInvalidEmailValidAge.count()
        }
      }
      
      // No records should be rejected for email validation
      val noEmailRejections = result.rejected.forall { rejectedDf =>
        rejectedDf.filter(col(REJECTION_CODE) === "REGEX_VALIDATION_FAILED").isEmpty
      }
      
      // Record count invariant
      val recordCountPreserved = {
        val validCount = result.valid.count()
        val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)
        validCount + rejectedCount == records.size
      }
      
      ageRejectionsExist && emailWarningsExist && noEmailRejections && recordCountPreserved
    }.getOrElse(false)
  }
  
  /**
   * Property 11: Validation Rule Reject Behavior - Empty DataFrame
   * For any empty DataFrame with reject rule, validation should pass without errors
   */
  property("reject_behavior_empty_dataframe") = forAll(Gen.const(())) { _ =>
    Try {
      val emptyDf = Seq.empty[TestRecord].toDF()
      val flowConfig = flowConfigWithRegexRule(onFailure = "reject")
      
      // Run validation
      val engine = new ValidationEngine()
      val result = engine.validate(emptyDf, flowConfig)
      
      // Should have no valid or rejected records
      val noRecords = result.valid.count() == 0 && result.rejected.forall(_.count() == 0)
      
      noRecords
    }.getOrElse(false)
  }
  
  /**
   * Property 12: Validation Rule Warn Behavior - Empty DataFrame
   * For any empty DataFrame with warn rule, validation should pass without errors
   */
  property("warn_behavior_empty_dataframe") = forAll(Gen.const(())) { _ =>
    Try {
      val emptyDf = Seq.empty[TestRecord].toDF()
      val flowConfig = flowConfigWithRegexRule(onFailure = "warn")
      
      // Run validation
      val engine = new ValidationEngine()
      val result = engine.validate(emptyDf, flowConfig)
      
      // Should have no valid or rejected records
      val noRecords = result.valid.count() == 0 && result.rejected.forall(_.count() == 0)
      
      // Should have warnings column
      val hasWarningsColumn = result.valid.columns.contains(WARNINGS)
      
      noRecords && hasWarningsColumn
    }.getOrElse(false)
  }
}
