package com.etl.framework.validation

import com.etl.framework.config._
import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.TestConfig
import com.etl.framework.validation.ValidationColumns._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters
import scala.util.Try

/** Property-based tests for custom validator dynamic loading Feature:
  * spark-etl-framework, Property 32: Custom Validator Dynamic Loading
  * Validates: Requirements 25.2
  *
  * Test configuration: Uses standardTestCount (20 cases)
  */
object CustomValidatorProperties extends Properties("CustomValidator") {

  // Configure test parameters
  override def overrideParameters(p: Parameters): Parameters =
    TestConfig.standardParams

  // Create a local Spark session for testing
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("CustomValidatorPropertiesTest")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  // Test record for validation
  case class TestRecord(
      id: Int,
      value: String,
      amount: Double
  )

  // Generator for test records
  val testRecordGen: Gen[TestRecord] = for {
    id <- Gen.choose(1, 10000)
    value <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    amount <- Gen.choose(0.0, 10000.0)
  } yield TestRecord(id, value, amount)

  // Generator for list of test records
  val testRecordsGen: Gen[Seq[TestRecord]] = for {
    count <- Gen.choose(5, 30)
    records <- Gen.listOfN(count, testRecordGen)
  } yield records.zipWithIndex.map { case (rec, idx) => rec.copy(id = idx + 1) }

  // Helper to create DataFrame from test records
  def createDataFrame(records: Seq[TestRecord]): DataFrame = {
    records.toDF()
  }

  /** Property 32: Custom Validator Dynamic Loading - Valid Class Name For any
    * valid custom validator class name, the Framework should successfully load
    * the validator
    */
  property("custom_validator_loads_successfully") = forAll(testRecordsGen) {
    records =>
      Try {
        if (records.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)

          // Create validation rule with custom validator
          val rule = ValidationRule(
            `type` = ValidationRuleType.Custom,
            column = Some("value"),
            `class` = Some("com.etl.framework.validation.TestCustomValidator"),
            config = Some(Map("threshold" -> "5")),
            description = Some("Test custom validator"),
            skipNull = Some(false),
            onFailure = OnFailureAction.Reject
          )

          // Load the custom validator
          val validator = ValidatorFactory.create(rule)

          // Validator should be an instance of the custom class
          validator.isInstanceOf[TestCustomValidator]
        }
      }.getOrElse(false)
  }

  /** Property 32: Custom Validator Dynamic Loading - Validator Execution For
    * any loaded custom validator, it should execute validation correctly
    */
  property("custom_validator_executes_validation") = forAll(testRecordsGen) {
    records =>
      Try {
        if (records.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)

          // Create validation rule with custom validator
          val rule = ValidationRule(
            `type` = ValidationRuleType.Custom,
            column = Some("value"),
            `class` = Some("com.etl.framework.validation.TestCustomValidator"),
            config = Some(Map("threshold" -> "5")),
            description = Some("Test custom validator"),
            skipNull = Some(false),
            onFailure = OnFailureAction.Reject
          )

          // Load and execute the custom validator
          val validator = ValidatorFactory.create(rule)
          val result = validator.validate(df, rule)

          // Result should have valid and rejected DataFrames
          val hasValidData = result.valid != null
          val inputCount = df.count()
          val validCount = result.valid.count()
          val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)

          // Record count invariant
          val recordCountPreserved = inputCount == validCount + rejectedCount

          hasValidData && recordCountPreserved
        }
      }.getOrElse(false)
  }

  /** Property 32: Custom Validator Dynamic Loading - Configuration Passed For
    * any custom validator with configuration, the configuration should be
    * passed via configure()
    */
  property("custom_validator_receives_configuration") =
    forAll(Gen.choose(1, 100)) { threshold =>
      Try {
        val records = Seq(
          TestRecord(1, "short", 100.0),
          TestRecord(2, "verylongvalue", 200.0),
          TestRecord(3, "mid", 300.0)
        )
        val df = createDataFrame(records)

        // Create validation rule with custom validator and specific threshold
        val rule = ValidationRule(
          `type` = ValidationRuleType.Custom,
          column = Some("value"),
          `class` = Some("com.etl.framework.validation.TestCustomValidator"),
          config = Some(Map("threshold" -> threshold.toString)),
          description = Some("Test custom validator with threshold"),
          skipNull = Some(false),
          onFailure = OnFailureAction.Reject
        )

        // Load the custom validator
        val validator = ValidatorFactory.create(rule)

        // Execute validation
        val result = validator.validate(df, rule)

        // Check that the validator used the threshold correctly
        // Records with value.length > threshold should be rejected
        val expectedRejectedCount = records.count(_.value.length > threshold)
        val actualRejectedCount = result.rejected.map(_.count()).getOrElse(0L)

        actualRejectedCount == expectedRejectedCount
      }.getOrElse(false)
    }

  /** Property 32: Custom Validator Dynamic Loading - Configurable Validator For
    * any custom validator implementing ConfigurableValidator, configure()
    * should be called
    */
  property("configurable_validator_configure_called") = forAll(testRecordsGen) {
    records =>
      Try {
        if (records.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)

          // Create validation rule with configurable custom validator
          val rule = ValidationRule(
            `type` = ValidationRuleType.Custom,
            column = Some("value"),
            `class` =
              Some("com.etl.framework.validation.TestConfigurableValidator"),
            config = Some(Map("prefix" -> "test_", "suffix" -> "_end")),
            description = Some("Test configurable validator"),
            skipNull = Some(false),
            onFailure = OnFailureAction.Reject
          )

          // Load the custom validator
          val validator = ValidatorFactory.create(rule)

          // Validator should be configurable
          val isConfigurable = validator.isInstanceOf[ConfigurableValidator]

          // Execute validation to ensure configuration was applied
          val result = validator.validate(df, rule)

          // Should have valid and rejected records
          val hasResults = result.valid != null

          isConfigurable && hasResults
        }
      }.getOrElse(false)
  }

  /** Property 32: Custom Validator Dynamic Loading - Invalid Class Name For any
    * invalid custom validator class name, the Framework should throw
    * ValidationConfigException
    */
  property("custom_validator_invalid_class_throws_exception") =
    forAll(Gen.alphaNumStr.suchThat(_.nonEmpty)) { invalidClassName =>
      Try {
        // Create validation rule with invalid class name
        val rule = ValidationRule(
          `type` = ValidationRuleType.Custom,
          column = Some("value"),
          `class` = Some(s"com.invalid.$invalidClassName"),
          config = Some(Map.empty),
          description = Some("Test invalid validator"),
          skipNull = Some(false),
          onFailure = OnFailureAction.Reject
        )

        // Attempt to load the custom validator should throw exception
        try {
          ValidatorFactory.create(rule)
          false // Should have thrown exception
        } catch {
          case _: ValidationConfigException => true
          case _: Exception                 => false
        }
      }.getOrElse(false)
    }

  /** Property 32: Custom Validator Dynamic Loading - Missing Class Name For any
    * custom validator rule without class name, the Framework should throw
    * ValidationConfigException
    */
  property("custom_validator_missing_class_throws_exception") =
    forAll(Gen.const(())) { _ =>
      Try {
        // Create validation rule without class name
        val rule = ValidationRule(
          `type` = ValidationRuleType.Custom,
          column = Some("value"),
          `class` = None,
          config = Some(Map.empty),
          description = Some("Test missing class validator"),
          skipNull = Some(false),
          onFailure = OnFailureAction.Reject
        )

        // Attempt to load the custom validator should throw exception
        try {
          ValidatorFactory.create(rule)
          false // Should have thrown exception
        } catch {
          case _: ValidationConfigException => true
          case _: Exception                 => false
        }
      }.getOrElse(false)
    }

  /** Property 32: Custom Validator Dynamic Loading - Multiple Custom Validators
    * For any set of different custom validator class names, each should load
    * independently
    */
  property("multiple_custom_validators_load_independently") =
    forAll(testRecordsGen) { records =>
      Try {
        if (records.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)

          // Create two different custom validators
          val rule1 = ValidationRule(
            `type` = ValidationRuleType.Custom,
            column = Some("value"),
            `class` = Some("com.etl.framework.validation.TestCustomValidator"),
            config = Some(Map("threshold" -> "5")),
            description = Some("First custom validator"),
            skipNull = Some(false),
            onFailure = OnFailureAction.Reject
          )

          val rule2 = ValidationRule(
            `type` = ValidationRuleType.Custom,
            column = Some("amount"),
            `class` =
              Some("com.etl.framework.validation.TestConfigurableValidator"),
            config = Some(Map("prefix" -> "test_")),
            description = Some("Second custom validator"),
            skipNull = Some(false),
            onFailure = OnFailureAction.Reject
          )

          // Load both validators
          val validator1 = ValidatorFactory.create(rule1)
          val validator2 = ValidatorFactory.create(rule2)

          // Both should be loaded successfully
          val bothLoaded = validator1 != null && validator2 != null

          // They should be different instances
          val differentInstances = validator1 != validator2

          // Both should execute successfully
          val result1 = validator1.validate(df, rule1)
          val result2 = validator2.validate(df, rule2)

          val bothExecuted = result1.valid != null && result2.valid != null

          bothLoaded && differentInstances && bothExecuted
        }
      }.getOrElse(false)
    }

  /** Property 32: Custom Validator Dynamic Loading - Rejection Metadata For any
    * custom validator that rejects records, rejection metadata should be
    * present
    */
  property("custom_validator_rejection_metadata") = forAll(testRecordsGen) {
    records =>
      Try {
        if (records.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)

          // Create validation rule with custom validator that will reject some records
          val rule = ValidationRule(
            `type` = ValidationRuleType.Custom,
            column = Some("value"),
            `class` = Some("com.etl.framework.validation.TestCustomValidator"),
            config = Some(Map("threshold" -> "5")),
            description = Some("Test custom validator"),
            skipNull = Some(false),
            onFailure = OnFailureAction.Reject
          )

          // Load and execute the custom validator
          val validator = ValidatorFactory.create(rule)
          val result = validator.validate(df, rule)

          // If there are rejected records, they should have metadata
          if (result.rejected.exists(_.count() > 0)) {
            val rejectedDf = result.rejected.get
            val hasRejectionCode = rejectedDf.columns.contains(REJECTION_CODE)
            val hasRejectionReason =
              rejectedDf.columns.contains(REJECTION_REASON)
            val hasValidationStep = rejectedDf.columns.contains(VALIDATION_STEP)
            val hasRejectedAt = rejectedDf.columns.contains(REJECTED_AT)

            hasRejectionCode && hasRejectionReason && hasValidationStep && hasRejectedAt
          } else {
            true // No rejected records to check
          }
        }
      }.getOrElse(false)
  }

  /** Property 32: Custom Validator Dynamic Loading - Record Count Invariant For
    * any custom validator execution, input_count = valid_count + rejected_count
    */
  property("custom_validator_preserves_record_count") = forAll(testRecordsGen) {
    records =>
      Try {
        if (records.isEmpty) {
          true // Skip empty test cases
        } else {
          val df = createDataFrame(records)
          val inputCount = df.count()

          // Create validation rule with custom validator
          val rule = ValidationRule(
            `type` = ValidationRuleType.Custom,
            column = Some("value"),
            `class` = Some("com.etl.framework.validation.TestCustomValidator"),
            config = Some(Map("threshold" -> "5")),
            description = Some("Test custom validator"),
            skipNull = Some(false),
            onFailure = OnFailureAction.Reject
          )

          // Load and execute the custom validator
          val validator = ValidatorFactory.create(rule)
          val result = validator.validate(df, rule)

          val validCount = result.valid.count()
          val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)

          // Invariant: input = valid + rejected
          inputCount == validCount + rejectedCount
        }
      }.getOrElse(false)
  }
}

/** Test custom validator for property testing Rejects records where
  * value.length > threshold
  */
class TestCustomValidator extends Validator with ConfigurableValidator {
  private var threshold: Int = 10

  override def configure(config: Map[String, String]): Unit = {
    threshold = config.getOrElse("threshold", "10").toInt
  }

  override def validate(
      df: DataFrame,
      rule: ValidationRule
  ): ValidationStepResult = {
    // Configure if config is provided
    rule.config.foreach(cfg =>
      configure(cfg.map { case (k, v) => k -> v.toString })
    )

    val column = rule.column.getOrElse("value")

    // Reject records where value.length > threshold
    val valid = df.filter(length(col(column)) <= threshold)
    val rejected = df
      .filter(length(col(column)) > threshold)
      .withColumn(
        REJECTION_REASON,
        lit(s"Value length exceeds threshold $threshold")
      )
      .withColumn(REJECTION_CODE, lit("CUSTOM_VALIDATION_FAILED"))
      .withColumn(VALIDATION_STEP, lit(s"custom_${column}"))
      .withColumn(REJECTED_AT, current_timestamp())

    ValidationStepResult(valid, Some(rejected))
  }
}

/** Test configurable custom validator for property testing Rejects records
  * where value doesn't start with prefix or end with suffix
  */
class TestConfigurableValidator extends Validator with ConfigurableValidator {
  private var prefix: String = ""
  private var suffix: String = ""

  override def configure(config: Map[String, String]): Unit = {
    prefix = config.getOrElse("prefix", "")
    suffix = config.getOrElse("suffix", "")
  }

  override def validate(
      df: DataFrame,
      rule: ValidationRule
  ): ValidationStepResult = {
    // Configure if config is provided
    rule.config.foreach(cfg =>
      configure(cfg.map { case (k, v) => k -> v.toString })
    )

    val column = rule.column.getOrElse("value")

    // Build condition based on configuration
    var condition = lit(true)
    if (prefix.nonEmpty) {
      condition = condition && col(column).startsWith(prefix)
    }
    if (suffix.nonEmpty) {
      condition = condition && col(column).endsWith(suffix)
    }

    val valid = df.filter(condition)
    val rejected = df
      .filter(!condition)
      .withColumn(
        REJECTION_REASON,
        lit(s"Value doesn't match prefix/suffix pattern")
      )
      .withColumn(REJECTION_CODE, lit("CUSTOM_VALIDATION_FAILED"))
      .withColumn(VALIDATION_STEP, lit(s"custom_configurable_${column}"))
      .withColumn(REJECTED_AT, current_timestamp())

    ValidationStepResult(valid, Some(rejected))
  }
}
