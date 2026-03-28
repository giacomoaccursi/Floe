package com.etl.framework.validation

import com.etl.framework.config._
import com.etl.framework.TestConfig
import com.etl.framework.validation.ValidationColumns._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters
import scala.util.Try

/** Property-based tests for Primary Key validation Feature: spark-etl-framework, Property 9: Primary Key Uniqueness
  * Enforcement Validates: Requirements 6.1
  *
  * Test configuration: Uses expensiveTestCount (10 cases) due to Spark operations
  */
object PKValidationProperties extends Properties("PKValidation") {

  // Configure test parameters for expensive Spark operations
  override def overrideParameters(p: Parameters): Parameters =
    TestConfig.expensiveParams

  // Create a local Spark session for testing
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("PKValidationPropertiesTest")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  // Generator for test records with controllable PK duplicates
  case class TestRecord(id: Int, name: String, value: Double)

  val uniqueRecordGen: Gen[TestRecord] = for {
    id <- Gen.choose(1, 10000)
    name <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    value <- Gen.choose(0.0, 1000.0)
  } yield TestRecord(id, name, value)

  // Generator for a list of unique records
  val uniqueRecordsGen: Gen[Seq[TestRecord]] = for {
    size <- Gen.choose(5, 50)
    records <- Gen.listOfN(size, uniqueRecordGen)
  } yield {
    // Ensure uniqueness by using distinct IDs
    records.zipWithIndex.map { case (rec, idx) => rec.copy(id = idx + 1) }
  }

  // Generator for records with duplicates
  val recordsWithDuplicatesGen: Gen[(Seq[TestRecord], Set[Int])] = for {
    uniqueCount <- Gen.choose(3, 30)
    uniqueRecords <- Gen.listOfN(uniqueCount, uniqueRecordGen).map { records =>
      records.zipWithIndex.map { case (rec, idx) => rec.copy(id = idx + 1) }
    }
    // Pick some records to duplicate
    duplicateCount <- Gen.choose(1, Math.min(5, uniqueCount))
    indicesToDuplicate <- Gen.pick(duplicateCount, uniqueRecords.indices)
  } yield {
    val duplicatedRecords = indicesToDuplicate.flatMap { idx =>
      val original = uniqueRecords(idx)
      // Create 1-3 duplicates of this record
      val numDuplicates = scala.util.Random.nextInt(3) + 1
      Seq.fill(numDuplicates)(original.copy(name = s"${original.name}_dup"))
    }

    val allRecords = uniqueRecords ++ duplicatedRecords
    val duplicatedIds =
      indicesToDuplicate.map(idx => uniqueRecords(idx).id).toSet

    (allRecords, duplicatedIds)
  }

  // Generator for flow configuration with PK validation
  def flowConfigWithPKGen(pkColumns: Seq[String]): Gen[FlowConfig] = for {
    name <- Gen.alphaNumStr.suchThat(_.nonEmpty)
  } yield FlowConfig(
    name = name,
    description = "Test flow with PK validation",
    version = "1.0",
    owner = "test",
    source = SourceConfig(
      `type` = SourceType.File,
      path = "/test",
      format = FileFormat.CSV,
      options = Map.empty,
      filePattern = None
    ),
    schema = SchemaConfig(
      enforceSchema = true,
      allowExtraColumns = false,
      columns = Seq(
        ColumnConfig("id", "int", nullable = false, "ID column"),
        ColumnConfig("name", "string", nullable = true, "Name column"),
        ColumnConfig("value", "double", nullable = true, "Value column")
      )
    ),
    loadMode = LoadModeConfig(
      `type` = LoadMode.Full
    ),
    validation = ValidationConfig(
      primaryKey = pkColumns,
      foreignKeys = Seq.empty,
      rules = Seq.empty
    ),
    output = OutputConfig()
  )

  // Helper function to create DataFrame from test records
  def createDataFrame(records: Seq[TestRecord]): DataFrame = {
    records.toDF()
  }

  /** Property 9: Primary Key Uniqueness Enforcement - All Duplicates Rejected For any dataset with duplicate Primary
    * Keys, all duplicate records should be rejected
    */
  property("duplicate_pk_records_all_rejected") = forAll(recordsWithDuplicatesGen, flowConfigWithPKGen(Seq("id"))) {
    case ((records, duplicatedIds), flowConfig) =>
      Try {
        if (records.isEmpty || duplicatedIds.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)

          // Run validation
          val engine = new ValidationEngine()
          val result = engine.validate(df, flowConfig)

          // Count how many records have duplicated IDs
          val duplicateRecordCount =
            records.count(r => duplicatedIds.contains(r.id))

          // All duplicate records should be rejected
          val allDuplicatesRejected = result.rejected.exists { rejectedDf =>
            val rejectedCount = rejectedDf.count()
            rejectedCount == duplicateRecordCount
          }

          // Valid records should only contain unique PKs
          val validHasUniqueKeys = {
            val validCount = result.valid.count()
            val distinctCount = result.valid.select("id").distinct().count()
            validCount == distinctCount
          }

          // Rejected records should have correct metadata
          val hasCorrectMetadata = result.rejected.exists { rejectedDf =>
            rejectedDf.columns.contains(REJECTION_CODE) &&
            rejectedDf.columns.contains(REJECTION_REASON) &&
            rejectedDf.columns.contains(VALIDATION_STEP) &&
            rejectedDf
              .filter(col(REJECTION_CODE) === "PK_DUPLICATE")
              .count() == duplicateRecordCount
          }

          allDuplicatesRejected && validHasUniqueKeys && hasCorrectMetadata
        }
      }.getOrElse(false)
  }

  /** Property 9: Primary Key Uniqueness Enforcement - Unique Records Pass For any dataset with all unique Primary Keys,
    * no records should be rejected for PK violation
    */
  property("unique_pk_records_all_pass") = forAll(uniqueRecordsGen, flowConfigWithPKGen(Seq("id"))) {
    (records, flowConfig) =>
      Try {
        if (records.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)

          // Run validation
          val engine = new ValidationEngine()
          val result = engine.validate(df, flowConfig)

          // No records should be rejected for PK violation
          val noPKRejections = result.rejected.forall { rejectedDf =>
            rejectedDf.filter(col(REJECTION_CODE) === "PK_DUPLICATE").isEmpty
          }

          // All records should be in valid DataFrame
          val allRecordsValid = result.valid.count() == records.size

          // Valid records should have warnings column
          val hasWarningsColumn = result.valid.columns.contains(WARNINGS)

          noPKRejections && allRecordsValid && hasWarningsColumn
        }
      }.getOrElse(false)
  }

  /** Property 9: Primary Key Uniqueness Enforcement - Composite PK For any dataset with composite Primary Keys,
    * duplicates should be identified correctly
    */
  property("composite_pk_duplicate_detection") = forAll(Gen.choose(5, 30)) { size =>
    Try {
      // Create records with composite PK (id, name)
      val uniqueRecords = (1 to size).map { i =>
        TestRecord(i % 10, s"name_${i % 5}", i.toDouble)
      }

      // Some records will have duplicate (id, name) combinations
      val df = createDataFrame(uniqueRecords)

      val flowConfig = FlowConfig(
        name = "test_composite_pk",
        description = "Test flow with composite PK",
        version = "1.0",
        owner = "test",
        source = SourceConfig(
          `type` = SourceType.File,
          path = "/test",
          format = FileFormat.CSV,
          options = Map.empty,
          filePattern = None
        ),
        schema = SchemaConfig(
          enforceSchema = true,
          allowExtraColumns = false,
          columns = Seq(
            ColumnConfig("id", "int", nullable = false, "ID column"),
            ColumnConfig(
          "name",
          "string",
          nullable = false,
          "Name column"
        ),
            ColumnConfig(
          "value",
          "double",
          nullable = true,
          "Value column"
        )
          )
        ),
        loadMode = LoadModeConfig(
          `type` = LoadMode.Full
        ),
        validation = ValidationConfig(
          primaryKey = Seq("id", "name"),
          foreignKeys = Seq.empty,
          rules = Seq.empty
        ),
        output = OutputConfig()
      )

      // Run validation
      val engine = new ValidationEngine()
      val result = engine.validate(df, flowConfig)

      // Valid records should have unique composite keys
      val validHasUniqueKeys = {
        val validCount = result.valid.count()
        val distinctCount =
          result.valid.select("id", "name").distinct().count()
        validCount == distinctCount
      }

      // Total records should be preserved (valid + rejected = input)
      val recordsPreserved = {
        val inputCount = df.count()
        val validCount = result.valid.count()
        val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)
        inputCount == validCount + rejectedCount
      }

      validHasUniqueKeys && recordsPreserved
    }.getOrElse(false)
  }

  /** Property 9: Primary Key Uniqueness Enforcement - Empty DataFrame For any empty DataFrame, PK validation should
    * pass without errors
    */
  property("empty_dataframe_pk_validation") = forAll(flowConfigWithPKGen(Seq("id"))) { flowConfig =>
    Try {
      // Create empty DataFrame with correct schema
      val emptyDf = Seq.empty[TestRecord].toDF()

      // Run validation
      val engine = new ValidationEngine()
      val result = engine.validate(emptyDf, flowConfig)

      // Should have no valid or rejected records
      val noRecords =
        result.valid.count() == 0 && result.rejected.forall(_.count() == 0)

      // Should not throw exception
      noRecords
    }.getOrElse(false)
  }

  /** Property 9: Primary Key Uniqueness Enforcement - Rejection Metadata Completeness For any records rejected due to
    * PK duplication, all metadata fields should be present
    */
  property("pk_rejection_metadata_complete") = forAll(recordsWithDuplicatesGen, flowConfigWithPKGen(Seq("id"))) {
    case ((records, duplicatedIds), flowConfig) =>
      Try {
        if (records.isEmpty || duplicatedIds.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)

          // Run validation
          val engine = new ValidationEngine()
          val result = engine.validate(df, flowConfig)

          // Check that all required metadata fields are present in rejected records
          result.rejected.exists { rejectedDf =>
            val requiredFields = Set(
              REJECTION_CODE,
              REJECTION_REASON,
              VALIDATION_STEP,
              REJECTED_AT
            )

            val actualFields = rejectedDf.columns.toSet
            val hasAllFields = requiredFields.subsetOf(actualFields)

            // Check that rejection code is correct
            val correctCode = rejectedDf
              .select(REJECTION_CODE)
              .distinct()
              .collect()
              .forall(_.getString(0) == "PK_DUPLICATE")

            // Check that rejection reason mentions primary key
            val reasonMentionsPK = rejectedDf
              .select(REJECTION_REASON)
              .distinct()
              .collect()
              .forall { row =>
                val reason = row.getString(0)
                reason.toLowerCase.contains(
                  "primary key"
                ) || reason.toLowerCase.contains("duplicate")
              }

            // Check that validation step is correct
            val correctStep = rejectedDf
              .select(VALIDATION_STEP)
              .distinct()
              .collect()
              .forall(_.getString(0) == "pk_validation")

            hasAllFields && correctCode && reasonMentionsPK && correctStep
          }
        }
      }.getOrElse(false)
  }

  /** Property 9: Primary Key Uniqueness Enforcement - Multiple Duplicates of Same Key For any dataset where a PK
    * appears multiple times, all occurrences should be rejected
    */
  property("multiple_duplicates_same_key_all_rejected") = forAll(Gen.choose(3, 10)) { duplicateCount =>
    Try {
      // Create records where one PK appears multiple times
      val duplicateId = 42
      val duplicateRecords = (1 to duplicateCount).map { i =>
        TestRecord(duplicateId, s"duplicate_$i", i.toDouble)
      }

      // Add some unique records
      val uniqueRecords = (1 to 5).map { i =>
        TestRecord(i, s"unique_$i", i.toDouble)
      }

      val allRecords = duplicateRecords ++ uniqueRecords
      val df = createDataFrame(allRecords)

      val flowConfig = FlowConfig(
        name = "test_multiple_duplicates",
        description = "Test flow",
        version = "1.0",
        owner = "test",
        source = SourceConfig(
          `type` = SourceType.File,
          path = "/test",
          format = FileFormat.CSV,
          options = Map.empty,
          filePattern = None
        ),
        schema = SchemaConfig(
          enforceSchema = true,
          allowExtraColumns = false,
          columns = Seq(
            ColumnConfig("id", "int", nullable = false, "ID column"),
            ColumnConfig(
          "name",
          "string",
          nullable = true,
          "Name column"
        ),
            ColumnConfig(
          "value",
          "double",
          nullable = true,
          "Value column"
        )
          )
        ),
        loadMode = LoadModeConfig(
          `type` = LoadMode.Full
        ),
        validation = ValidationConfig(
          primaryKey = Seq("id"),
          foreignKeys = Seq.empty,
          rules = Seq.empty
        ),
        output = OutputConfig()
      )

      // Run validation
      val engine = new ValidationEngine()
      val result = engine.validate(df, flowConfig)

      // All duplicate records should be rejected
      val allDuplicatesRejected = result.rejected.exists { rejectedDf =>
        rejectedDf.count() == duplicateCount
      }

      // Only unique records should remain valid
      val onlyUniqueValid = result.valid.count() == uniqueRecords.size

      // Valid records should not contain the duplicate ID
      val noDuplicateIdInValid = !result.valid
        .filter($"id" === duplicateId)
        .isEmpty == false

      allDuplicatesRejected && onlyUniqueValid && noDuplicateIdInValid
    }.getOrElse(false)
  }

  /** Property 9: Primary Key Uniqueness Enforcement - Record Count Invariant For any dataset, input_count = valid_count
    * + rejected_count
    */
  property("pk_validation_preserves_record_count") = forAll(recordsWithDuplicatesGen, flowConfigWithPKGen(Seq("id"))) {
    case ((records, _), flowConfig) =>
      Try {
        if (records.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)
          val inputCount = df.count()

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
}
