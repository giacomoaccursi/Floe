package com.etl.framework.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigEnumsTest extends AnyFlatSpec with Matchers {

  // ═══════════════════════════════════════════════════════════════════════════
  // JOIN STRATEGY
  // ═══════════════════════════════════════════════════════════════════════════
  "JoinStrategy.fromString" should "parse valid strategies" in {
    JoinStrategy.fromString("nest") shouldBe Right(JoinStrategy.Nest)
    JoinStrategy.fromString("flatten") shouldBe Right(JoinStrategy.Flatten)
    JoinStrategy.fromString("aggregate") shouldBe Right(JoinStrategy.Aggregate)
  }

  it should "be case-insensitive" in {
    JoinStrategy.fromString("NEST") shouldBe Right(JoinStrategy.Nest)
    JoinStrategy.fromString("Flatten") shouldBe Right(JoinStrategy.Flatten)
  }

  it should "return Left for invalid values" in {
    val result = JoinStrategy.fromString("invalid")
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("Unknown join strategy")
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // SOURCE TYPE
  // ═══════════════════════════════════════════════════════════════════════════
  "SourceType.fromString" should "parse valid types" in {
    SourceType.fromString("file") shouldBe Right(SourceType.File)
    SourceType.fromString("jdbc") shouldBe Right(SourceType.JDBC)
  }

  it should "return Left for invalid values" in {
    SourceType.fromString("kafka").isLeft shouldBe true // Assuming Kafka not yet supported based on file content
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // FILE FORMAT
  // ═══════════════════════════════════════════════════════════════════════════
  "FileFormat.fromString" should "parse valid formats" in {
    FileFormat.fromString("csv") shouldBe Right(FileFormat.CSV)
    FileFormat.fromString("parquet") shouldBe Right(FileFormat.Parquet)
    FileFormat.fromString("json") shouldBe Right(FileFormat.JSON)
  }
  
  "FileFormat values" should "have correct spark format mapping" in {
    FileFormat.CSV.sparkFormat shouldBe "csv"
    FileFormat.Parquet.sparkFormat shouldBe "parquet"
    FileFormat.JSON.sparkFormat shouldBe "json"
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // LOAD MODE
  // ═══════════════════════════════════════════════════════════════════════════
  "LoadMode.fromString" should "parse valid modes" in {
    LoadMode.fromString("full") shouldBe Right(LoadMode.Full)
    LoadMode.fromString("delta") shouldBe Right(LoadMode.Delta)
    LoadMode.fromString("scd2") shouldBe Right(LoadMode.SCD2)
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // VALIDATION RULE TYPE
  // ═══════════════════════════════════════════════════════════════════════════
  "ValidationRuleType.fromString" should "parse valid rules" in {
    ValidationRuleType.fromString("pk_uniqueness") shouldBe Right(ValidationRuleType.PKUniqueness)
    ValidationRuleType.fromString("fk_integrity") shouldBe Right(ValidationRuleType.FKIntegrity)
    ValidationRuleType.fromString("regex") shouldBe Right(ValidationRuleType.Regex)
    ValidationRuleType.fromString("range") shouldBe Right(ValidationRuleType.Range)
    ValidationRuleType.fromString("domain") shouldBe Right(ValidationRuleType.Domain)
    ValidationRuleType.fromString("not_null") shouldBe Right(ValidationRuleType.NotNull)
    ValidationRuleType.fromString("custom") shouldBe Right(ValidationRuleType.Custom)
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // ON FAILURE ACTION
  // ═══════════════════════════════════════════════════════════════════════════
  "OnFailureAction.fromString" should "parse valid actions" in {
    OnFailureAction.fromString("reject") shouldBe Right(OnFailureAction.Reject)
    OnFailureAction.fromString("warn") shouldBe Right(OnFailureAction.Warn)
    OnFailureAction.fromString("skip") shouldBe Right(OnFailureAction.Skip)
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // AGGREGATION FUNCTION
  // ═══════════════════════════════════════════════════════════════════════════
  "AggregationFunction.fromString" should "parse valid functions" in {
    AggregationFunction.fromString("sum") shouldBe Right(AggregationFunction.Sum)
    AggregationFunction.fromString("count") shouldBe Right(AggregationFunction.Count)
    AggregationFunction.fromString("avg") shouldBe Right(AggregationFunction.Avg)
    AggregationFunction.fromString("average") shouldBe Right(AggregationFunction.Avg) // Alias check
    AggregationFunction.fromString("collect_list") shouldBe Right(AggregationFunction.CollectList)
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // MERGE STRATEGY
  // ═══════════════════════════════════════════════════════════════════════════
  "MergeStrategy.fromString" should "parse valid strategies" in {
    MergeStrategy.fromString("upsert") shouldBe Right(MergeStrategy.Upsert)
    MergeStrategy.fromString("append") shouldBe Right(MergeStrategy.Append)
    MergeStrategy.fromString("replace") shouldBe Right(MergeStrategy.Replace)
  }
}
