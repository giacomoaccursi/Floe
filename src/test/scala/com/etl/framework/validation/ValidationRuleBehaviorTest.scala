package com.etl.framework.validation

import com.etl.framework.config._
import com.etl.framework.validation.ValidationColumns._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => f}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class ValidationRuleBehaviorRecord(id: Int, email: String, age: Int, status: String)

class ValidationRuleBehaviorTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("ValidationRuleBehaviorTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  private val emailPattern = "^[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+$"

  private val validRecords = Seq(
    ValidationRuleBehaviorRecord(1, "alice@example.com", 25, "active"),
    ValidationRuleBehaviorRecord(2, "bob@test.org", 30, "active"),
    ValidationRuleBehaviorRecord(3, "charlie@demo.net", 35, "inactive")
  )

  private val invalidRecords = Seq(
    ValidationRuleBehaviorRecord(4, "invalidemail", 28, "active"),
    ValidationRuleBehaviorRecord(5, "noemail", 40, "pending")
  )

  private val allRecords = validRecords ++ invalidRecords

  def flowConfigWithRegexRule(onFailure: OnFailureAction): FlowConfig = {
    FlowConfig(
      name = "test_flow",
      description = "Test flow",
      version = "1.0",
      owner = "test",
      source = SourceConfig(SourceType.File, "/test", FileFormat.CSV, Map.empty, None),
      schema = SchemaConfig(
        enforceSchema = true,
        allowExtraColumns = false,
        columns = Seq(
          ColumnConfig("id", "int", nullable = false, ""),
          ColumnConfig("email", "string", nullable = false, ""),
          ColumnConfig("age", "int", nullable = false, ""),
          ColumnConfig("status", "string", nullable = false, "")
        )
      ),
      loadMode = LoadModeConfig(LoadMode.Full),
      validation = ValidationConfig(
        primaryKey = Seq("id"),
        foreignKeys = Seq.empty,
        rules = Seq(
          ValidationRule(
            `type` = ValidationRuleType.Regex,
            column = Some("email"),
            pattern = Some(emailPattern),
            description = Some("Email must be valid format"),
            skipNull = Some(false),
            onFailure = onFailure
          )
        )
      ),
      output = OutputConfig()
    )
  }

  "Reject behavior" should "move failed records to rejected" in {
    val df = allRecords.toDF()
    val flowConfig = flowConfigWithRegexRule(OnFailureAction.Reject)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    result.rejected.isDefined shouldBe true
    result.rejected.get.count() shouldBe 2

    val validEmails = result.valid.select("email").collect().map(_.getString(0))
    validEmails.foreach(email => email should fullyMatch regex emailPattern)
  }

  it should "preserve record count (input = valid + rejected)" in {
    val df = allRecords.toDF()
    val flowConfig = flowConfigWithRegexRule(OnFailureAction.Reject)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    val validCount = result.valid.count()
    val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)

    (validCount + rejectedCount) shouldBe allRecords.size
  }

  it should "not include invalid records in valid set" in {
    val df = allRecords.toDF()
    val flowConfig = flowConfigWithRegexRule(OnFailureAction.Reject)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    result.valid
      .filter(!f.col("email").rlike(emailPattern))
      .isEmpty shouldBe true
  }

  it should "include rejection metadata on rejected records" in {
    val df = allRecords.toDF()
    val flowConfig = flowConfigWithRegexRule(OnFailureAction.Reject)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    val rejected = result.rejected.get
    rejected.columns should contain allOf (
      REJECTION_CODE,
      REJECTION_REASON,
      VALIDATION_STEP,
      REJECTED_AT
    )
    rejected
      .filter(f.col(REJECTION_CODE) === "REGEX_VALIDATION_FAILED")
      .count() shouldBe 2
  }

  it should "handle empty DataFrame without errors" in {
    val df = Seq.empty[ValidationRuleBehaviorRecord].toDF()
    val flowConfig = flowConfigWithRegexRule(OnFailureAction.Reject)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    result.valid.count() shouldBe 0
    result.rejected.forall(_.count() == 0) shouldBe true
  }

  "Warn behavior" should "keep failing records in valid without rejecting them" in {
    val df = allRecords.toDF()
    val flowConfig = flowConfigWithRegexRule(OnFailureAction.Warn)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    val noRegexRejections = result.rejected.forall { rejectedDf =>
      rejectedDf
        .filter(f.col(REJECTION_CODE) === "REGEX_VALIDATION_FAILED")
        .isEmpty
    }
    noRegexRejections shouldBe true

    // All records (valid and invalid emails) should be in the valid DataFrame
    result.valid.count() should be >= allRecords.size.toLong
  }

  it should "produce warned DataFrame with warning metadata" in {
    val df = allRecords.toDF()
    val flowConfig = flowConfigWithRegexRule(OnFailureAction.Warn)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    result.warned shouldBe defined
    val warned = result.warned.get
    warned.columns should contain allOf (WARNING_RULE, WARNING_MESSAGE, WARNING_COLUMN, WARNED_AT)
    warned.count() should be > 0L
  }

  it should "not produce warnings for passing records" in {
    val onlyValidRecords = validRecords
    val df = onlyValidRecords.toDF()
    val flowConfig = flowConfigWithRegexRule(OnFailureAction.Warn)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    result.warned match {
      case Some(warned) => warned.isEmpty shouldBe true
      case None         => succeed
    }
  }

  it should "preserve record count (input = valid + rejected)" in {
    val df = allRecords.toDF()
    val flowConfig = flowConfigWithRegexRule(OnFailureAction.Warn)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    val validCount = result.valid.count()
    val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)

    (validCount + rejectedCount) shouldBe allRecords.size
  }

  it should "handle empty DataFrame without errors" in {
    val df = Seq.empty[ValidationRuleBehaviorRecord].toDF()
    val flowConfig = flowConfigWithRegexRule(OnFailureAction.Warn)

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    result.valid.count() shouldBe 0
  }

  "RangeValidator on string column" should "use numeric comparison, not lexicographic" in {
    // "9" > "10" lexicographically but 9 < 10 numerically.
    // Without a numeric cast, "9" would incorrectly pass a min=10 range check.
    val records = Seq(
      ("1", "9"), // 9 < 10 numerically → should be REJECTED
      ("2", "15"), // 15 in [10, 100]   → should be VALID
      ("3", "101"), // 101 > 100          → should be REJECTED
      ("4", "100"), // 100 == max         → should be VALID
      ("5", "10") // 10 == min          → should be VALID
    )
    val df = records.toDF("id", "score")

    val flowConfig = FlowConfig(
      name = "test_range",
      description = "",
      version = "1.0",
      owner = "test",
      source = SourceConfig(SourceType.File, "/test", FileFormat.CSV, Map.empty, None),
      schema = SchemaConfig(
        enforceSchema = true,
        allowExtraColumns = false,
        columns = Seq(
          ColumnConfig("id", "string", nullable = false, ""),
          ColumnConfig("score", "string", nullable = false, "")
        )
      ),
      loadMode = LoadModeConfig(LoadMode.Full),
      validation = ValidationConfig(
        primaryKey = Seq("id"),
        foreignKeys = Seq.empty,
        rules = Seq(
          ValidationRule(
            `type` = ValidationRuleType.Range,
            column = Some("score"),
            min = Some("10"),
            max = Some("100"),
            description = Some("Score must be between 10 and 100"),
            skipNull = Some(false),
            onFailure = OnFailureAction.Reject
          )
        )
      ),
      output = OutputConfig()
    )

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)
    rejectedCount shouldBe 2

    val rejectedIds = result.rejected.get.select("id").collect().map(_.getString(0)).toSet
    rejectedIds should contain("1") // "9" < 10 numerically
    rejectedIds should contain("3") // "101" > 100 numerically
    rejectedIds should not contain "2"
    rejectedIds should not contain "4"
    rejectedIds should not contain "5"
  }

  "Multiple rules with different behaviors" should "reject for reject-rules and warn for warn-rules" in {
    val records = Seq(
      ValidationRuleBehaviorRecord(1, "alice@example.com", 25, "active"), // all valid
      ValidationRuleBehaviorRecord(2, "invalidemail", 30, "active"), // invalid email (warn)
      ValidationRuleBehaviorRecord(3, "bob@test.org", 150, "inactive"), // invalid age (reject)
      ValidationRuleBehaviorRecord(4, "noemail", 200, "pending") // both invalid
    )
    val df = records.toDF()

    val flowConfig = FlowConfig(
      name = "test_flow",
      description = "Test flow",
      version = "1.0",
      owner = "test",
      source = SourceConfig(SourceType.File, "/test", FileFormat.CSV, Map.empty, None),
      schema = SchemaConfig(
        enforceSchema = true,
        allowExtraColumns = false,
        columns = Seq(
          ColumnConfig("id", "int", nullable = false, ""),
          ColumnConfig("email", "string", nullable = false, ""),
          ColumnConfig("age", "int", nullable = false, ""),
          ColumnConfig("status", "string", nullable = false, "")
        )
      ),
      loadMode = LoadModeConfig(LoadMode.Full),
      validation = ValidationConfig(
        primaryKey = Seq("id"),
        foreignKeys = Seq.empty,
        rules = Seq(
          ValidationRule(
            `type` = ValidationRuleType.Regex,
            column = Some("email"),
            pattern = Some(emailPattern),
            description = Some("Email must be valid format"),
            skipNull = Some(false),
            onFailure = OnFailureAction.Warn
          ),
          ValidationRule(
            `type` = ValidationRuleType.Range,
            column = Some("age"),
            min = Some("18"),
            max = Some("120"),
            description = Some("Age must be between 18 and 120"),
            skipNull = Some(false),
            onFailure = OnFailureAction.Reject
          )
        )
      ),
      output = OutputConfig()
    )

    val engine = new ValidationEngine()
    val result = engine.validate(df, flowConfig)

    // Records with invalid age should be rejected
    result.rejected.exists { rejectedDf =>
      rejectedDf
        .filter(f.col(REJECTION_CODE) === "RANGE_VALIDATION_FAILED")
        .count() > 0
    } shouldBe true

    // No records rejected for email regex
    result.rejected.forall { rejectedDf =>
      rejectedDf
        .filter(f.col(REJECTION_CODE) === "REGEX_VALIDATION_FAILED")
        .isEmpty
    } shouldBe true

    // Record count invariant
    val validCount = result.valid.count()
    val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)
    (validCount + rejectedCount) shouldBe records.size
  }
}
