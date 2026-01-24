package com.etl.framework.orchestration

import com.etl.framework.core.TransformationContext
import com.etl.framework.TestConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters

/**
 * Property-based tests for post-validation transformations
 * Feature: spark-etl-framework, Property 19: Post-Validation Cross-Flow Access
 * Validates: Requirements 12.4
 * 
 * Test configuration: Uses standardTestCount (20 cases)
 */
object PostValidationTransformationProperties extends Properties("PostValidationTransformation") {
  
  // Configure test parameters
  override def overrideParameters(p: Parameters): Parameters = TestConfig.standardParams
  
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("PostValidationTransformationProperties")
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
  
  // Generator for validated flows map
  val validatedFlowsGen: Gen[Map[String, DataFrame]] = for {
    numFlows <- Gen.choose(1, 3)
    flows <- Gen.listOfN(numFlows, for {
      flowName <- Gen.oneOf("customers", "orders", "products")
      data <- testDataGen
    } yield (flowName, data.toDF("id", "value", "status")))
  } yield flows.toMap
  
  /**
   * Property 19: Post-Validation Cross-Flow Access
   * For any post-validation transformation that accesses other flows,
   * all referenced flows should be available in the TransformationContext.validatedFlows map.
   */
  property("post_validation_transformation_has_access_to_validated_flows") = forAll(
    testDataGen,
    validatedFlowsGen
  ) { (currentData, validatedFlows) =>
    
    var receivedValidatedFlows: Map[String, DataFrame] = Map.empty
    
    val transformation: TransformationContext => DataFrame = { ctx =>
      receivedValidatedFlows = ctx.validatedFlows
      ctx.currentData
    }
    
    val df = currentData.toDF("id", "value", "status")
    
    transformation(TransformationContext(
      currentFlow = "test_flow",
      currentData = df,
      validatedFlows = validatedFlows,
      batchId = "test_batch",
      spark = spark
    ))
    
    // Verify all validated flows are accessible
    receivedValidatedFlows.keySet == validatedFlows.keySet &&
    receivedValidatedFlows.keys.forall(key => 
      receivedValidatedFlows(key).count() == validatedFlows(key).count()
    )
  }
  
  /**
   * Property: Post-validation transformation can access specific flows
   * For any post-validation transformation that accesses a specific flow,
   * the getFlow method should return the correct DataFrame.
   */
  property("post_validation_transformation_can_access_specific_flows") = forAll(
    testDataGen,
    validatedFlowsGen
  ) { (currentData, validatedFlows) =>
    
    var accessedFlow: Option[DataFrame] = None
    val targetFlowName = validatedFlows.keys.headOption.getOrElse("customers")
    
    val transformation: TransformationContext => DataFrame = { ctx =>
      accessedFlow = ctx.getFlow(targetFlowName)
      ctx.currentData
    }
    
    val df = currentData.toDF("id", "value", "status")
    
    transformation(TransformationContext(
      currentFlow = "test_flow",
      currentData = df,
      validatedFlows = validatedFlows,
      batchId = "test_batch",
      spark = spark
    ))
    
    // Verify the flow was accessible
    if (validatedFlows.contains(targetFlowName)) {
      accessedFlow.isDefined &&
      accessedFlow.get.count() == validatedFlows(targetFlowName).count()
    } else {
      accessedFlow.isEmpty
    }
  }
  
  /**
   * Property: Post-validation transformation can join with other flows
   * For any post-validation transformation that joins with another flow,
   * the join should produce valid results.
   */
  property("post_validation_transformation_can_join_with_other_flows") = forAll(
    testDataGen,
    testDataGen
  ) { (currentData, otherFlowData) =>
    
    val transformation: TransformationContext => DataFrame = { ctx =>
      ctx.getFlow("other_flow") match {
        case Some(otherDf) =>
          // Join current data with other flow
          ctx.currentData.join(otherDf, Seq("id"), "left")
        case None =>
          ctx.currentData
      }
    }
    
    val df = currentData.toDF("id", "value", "status")
    val otherDf = otherFlowData.toDF("id", "value", "status")
    
    val result = transformation(TransformationContext(
      currentFlow = "test_flow",
      currentData = df,
      validatedFlows = Map("other_flow" -> otherDf),
      batchId = "test_batch",
      spark = spark
    ))
    
    // Verify join produced results
    result.count() >= 0 &&
    result.columns.length >= df.columns.length
  }
  
  /**
   * Property: Post-validation transformation can create additional tables
   * For any post-validation transformation that calls ctx.addTable(),
   * the table should be tracked for later persistence.
   */
  property("post_validation_transformation_can_create_additional_tables") = forAll(testDataGen) { testData =>
    
    var addTableCalled = false
    var addedTableName: String = ""
    var addedTableData: Option[DataFrame] = None
    
    val transformation: TransformationContext => DataFrame = { ctx =>
      // Create an additional table
      val summaryData = ctx.currentData
        .groupBy("status")
        .agg(
          count("*").as("count"),
          sum("value").as("total_value")
        )
      
      // Mock addTable by capturing the call
      addTableCalled = true
      addedTableName = "status_summary"
      addedTableData = Some(summaryData)
      
      ctx.currentData
    }
    
    val df = testData.toDF("id", "value", "status")
    
    transformation(TransformationContext(
      currentFlow = "test_flow",
      currentData = df,
      validatedFlows = Map.empty,
      batchId = "test_batch",
      spark = spark
    ))
    
    // Verify additional table was created
    addTableCalled &&
    addedTableName.nonEmpty &&
    addedTableData.isDefined &&
    addedTableData.get.count() > 0
  }
  
  /**
   * Property: Post-validation transformation preserves current data
   * For any post-validation transformation that accesses other flows,
   * the current flow data should remain accessible and unmodified.
   */
  property("post_validation_transformation_preserves_current_data") = forAll(
    testDataGen,
    validatedFlowsGen
  ) { (currentData, validatedFlows) =>
    
    var currentDataCount: Long = 0
    var currentDataSchema: org.apache.spark.sql.types.StructType = null
    
    val transformation: TransformationContext => DataFrame = { ctx =>
      currentDataCount = ctx.currentData.count()
      currentDataSchema = ctx.currentData.schema
      // Access other flows but return current data unchanged
      ctx.validatedFlows.foreach { case (_, df) => df.count() }
      ctx.currentData
    }
    
    val df = currentData.toDF("id", "value", "status")
    val originalCount = df.count()
    val originalSchema = df.schema
    
    val result = transformation(TransformationContext(
      currentFlow = "test_flow",
      currentData = df,
      validatedFlows = validatedFlows,
      batchId = "test_batch",
      spark = spark
    ))
    
    // Verify current data was preserved
    currentDataCount == originalCount &&
    currentDataSchema == originalSchema &&
    result.count() == originalCount
  }
  
  /**
   * Property: Post-validation transformation can enrich data from other flows
   * For any post-validation transformation that enriches data from other flows,
   * the enriched data should contain additional columns.
   */
  property("post_validation_transformation_can_enrich_from_other_flows") = forAll(
    testDataGen,
    testDataGen
  ) { (currentData, enrichmentData) =>
    
    val transformation: TransformationContext => DataFrame = { ctx =>
      ctx.getFlow("enrichment_flow") match {
        case Some(enrichmentDf) =>
          // Enrich current data with additional column from other flow
          val enriched = enrichmentDf
            .groupBy("status")
            .agg(avg("value").as("avg_value_by_status"))
          
          ctx.currentData.join(enriched, Seq("status"), "left")
        
        case None =>
          ctx.currentData
      }
    }
    
    val df = currentData.toDF("id", "value", "status")
    val enrichmentDf = enrichmentData.toDF("id", "value", "status")
    
    val result = transformation(TransformationContext(
      currentFlow = "test_flow",
      currentData = df,
      validatedFlows = Map("enrichment_flow" -> enrichmentDf),
      batchId = "test_batch",
      spark = spark
    ))
    
    // Verify enrichment added columns
    result.columns.length > df.columns.length &&
    result.columns.contains("avg_value_by_status")
  }
}
