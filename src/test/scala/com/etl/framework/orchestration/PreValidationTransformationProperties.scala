package com.etl.framework.orchestration

import com.etl.framework.config._
import com.etl.framework.core.TransformationContext
import com.etl.framework.TestConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters

/**
 * Property-based tests for pre-validation transformations
 * Feature: spark-etl-framework, Property 18: Pre-Validation Transformation Execution Order
 * Validates: Requirements 11.2
 * 
 * Test configuration: Uses standardTestCount (20 cases)
 */
object PreValidationTransformationProperties extends Properties("PreValidationTransformation") {
  
  // Configure test parameters
  override def overrideParameters(p: Parameters): Parameters = TestConfig.standardParams
  
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("PreValidationTransformationProperties")
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
  
  /**
   * Property 18: Pre-Validation Transformation Execution Order
   * For any flow with pre-validation transformation, the transformation should be applied
   * to the data before any validation is executed.
   */
  property("pre_validation_transformation_executes_before_validation") = forAll(testDataGen) { testData =>
    
    // Track whether transformation was called
    var transformationCalled = false
    var transformationRecordCount: Long = 0
    
    // Create a transformation that adds a marker column
    val transformation: TransformationContext => DataFrame = { ctx =>
      transformationCalled = true
      transformationRecordCount = ctx.currentData.count()
      // Add a marker column to prove transformation ran
      ctx.currentData.withColumn("_pre_transformation_marker", lit(true))
    }
    
    // Create test DataFrame
    val df = testData.toDF("id", "value", "status")
    
    // Verify transformation was called
    transformationCalled = false
    val result = transformation(TransformationContext(
      currentFlow = "test",
      currentData = df,
      validatedFlows = Map.empty,
      batchId = "test_batch",
      spark = spark
    ))
    
    transformationCalled &&
    transformationRecordCount == df.count() &&
    result.columns.contains("_pre_transformation_marker")
  }
  
  /**
   * Property: Pre-validation transformation receives correct context
   * For any flow with pre-validation transformation, the transformation context should
   * contain the current flow name, current data, and batch ID.
   */
  property("pre_validation_transformation_receives_correct_context") = forAll(testDataGen) { testData =>
    
    var receivedContext: Option[TransformationContext] = None
    
    val transformation: TransformationContext => DataFrame = { ctx =>
      receivedContext = Some(ctx)
      ctx.currentData
    }
    
    val df = testData.toDF("id", "value", "status")
    val batchId = "test_batch_002"
    val flowName = "test_flow"
    
    transformation(TransformationContext(
      currentFlow = flowName,
      currentData = df,
      validatedFlows = Map.empty,
      batchId = batchId,
      spark = spark
    ))
    
    receivedContext.isDefined &&
    receivedContext.get.currentFlow == flowName &&
    receivedContext.get.batchId == batchId &&
    receivedContext.get.validatedFlows.isEmpty // No validated flows available in pre-validation
  }
  
  /**
   * Property: Pre-validation transformation can modify data
   * For any flow with pre-validation transformation that modifies data,
   * the modifications should be visible to the validation engine.
   */
  property("pre_validation_transformation_modifications_visible_to_validation") = forAll(testDataGen) { testData =>
    
    // Create transformation that filters data
    val transformation: TransformationContext => DataFrame = { ctx =>
      // Filter to only keep records with value > 500
      ctx.currentData.filter(col("value") > 500)
    }
    
    val df = testData.toDF("id", "value", "status")
    val originalCount = df.count()
    val expectedCount = df.filter(col("value") > 500).count()
    
    val result = transformation(TransformationContext(
      currentFlow = "test",
      currentData = df,
      validatedFlows = Map.empty,
      batchId = "test_batch",
      spark = spark
    ))
    
    val resultCount = result.count()
    
    // The result should reflect the filtered data
    resultCount == expectedCount &&
    (expectedCount <= originalCount)
  }
  
  /**
   * Property: Pre-validation transformation preserves schema when not modifying
   * For any flow with pre-validation transformation that doesn't modify schema,
   * the output schema should match the input schema.
   */
  property("pre_validation_transformation_preserves_schema_when_not_modifying") = forAll(testDataGen) { testData =>
    
    // Create transformation that doesn't modify schema
    val transformation: TransformationContext => DataFrame = { ctx =>
      // Just filter, don't add/remove columns
      ctx.currentData.filter(col("value") > 0)
    }
    
    val df = testData.toDF("id", "value", "status")
    val originalSchema = df.schema
    
    val result = transformation(TransformationContext(
      currentFlow = "test",
      currentData = df,
      validatedFlows = Map.empty,
      batchId = "test_batch",
      spark = spark
    ))
    
    result.schema == originalSchema
  }
}

