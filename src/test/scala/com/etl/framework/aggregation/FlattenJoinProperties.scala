package com.etl.framework.aggregation

import com.etl.framework.config._
import com.etl.framework.TestConfig
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Property-based tests for flatten join strategy
 * Feature: spark-etl-framework, Property 23: Flatten Join Strategy
 * Validates: Requirements 15.3
 * 
 * Test configuration: Uses standardTestCount (20 cases)
 */
object FlattenJoinProperties extends Properties("FlattenJoin") {
  
  // Configure test parameters
  override def overrideParameters(p: Parameters): Parameters = TestConfig.standardParams
  
  // Create a minimal Spark session for testing
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("FlattenJoinPropertiesTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  
  import spark.implicits._
  
  // Generator for parent records
  case class ParentRecord(id: String, parent_name: String, parent_value: Int)
  
  val parentRecordGen: Gen[ParentRecord] = for {
    id <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    name <- Gen.alphaNumStr
    value <- Gen.choose(1, 1000)
  } yield ParentRecord(id, name, value)
  
  val parentDataGen: Gen[Seq[ParentRecord]] = for {
    count <- Gen.choose(1, 20)
    records <- Gen.listOfN(count, parentRecordGen)
  } yield records.groupBy(_.id).map(_._2.head).toSeq  // Ensure unique IDs
  
  // Generator for child records
  case class ChildRecord(parent_id: String, child_name: String, child_value: Int)
  
  def childRecordGen(parentIds: Seq[String]): Gen[ChildRecord] = for {
    parentId <- Gen.oneOf(parentIds)
    childName <- Gen.alphaNumStr
    childValue <- Gen.choose(1, 100)
  } yield ChildRecord(parentId, childName, childValue)
  
  def childDataGen(parentIds: Seq[String]): Gen[Seq[ChildRecord]] = for {
    count <- Gen.choose(0, 50)
    records <- Gen.listOfN(count, childRecordGen(parentIds))
  } yield records
  
  // Generator for join configuration
  val flattenJoinConfigGen: Gen[JoinConfig] = for {
    joinType <- Gen.oneOf("inner", "left_outer", "right_outer", "full_outer")
  } yield JoinConfig(
    `type` = joinType,
    parent = "parent_node",
    on = Seq(JoinCondition("id", "parent_id")),
    strategy = "flatten",
    nestAs = None,
    aggregations = Seq.empty
  )
  
  // Helper to create DataFrames
  def createParentDF(records: Seq[ParentRecord]): DataFrame = {
    records.toDF("id", "parent_name", "parent_value")
  }
  
  def createChildDF(records: Seq[ChildRecord]): DataFrame = {
    records.toDF("parent_id", "child_name", "child_value")
  }
  
  // Helper to apply flatten join using DAGOrchestrator's private method
  def applyFlattenJoin(parent: DataFrame, child: DataFrame, joinConfig: JoinConfig): DataFrame = {
    val globalConfig = GlobalConfig(
      spark = SparkConfig("test", "local[*]", Map.empty),
      paths = PathsConfig("/data/input", "/data/output", "/data/validated", "/data/rejected",
        "/data/metadata", "/data/model", "/data/staging", "/data/checkpoint"),
      processing = ProcessingConfig("yyyyMMdd_HHmmss", "batch", false, 0.1, false, "5m"),
      performance = PerformanceConfig(false, false, 10485760L, false, 200),
      monitoring = MonitoringConfig(false, None, None, "INFO"),
      security = SecurityConfig(false, None, false)
    )
    
    val dagConfig = AggregationConfig(
      name = "test",
      description = "Test",
      version = "1.0",
      batchModel = ModelConfig("com.example.BatchModel", None, None),
      finalModel = ModelConfig("com.example.FinalModel", None, None),
      output = DAGOutputConfig(
        batch = OutputConfig(Some("/data/model/batch"), None, "parquet", Seq.empty, "snappy", Map.empty),
        `final` = OutputConfig(Some("/data/model/final"), None, "parquet", Seq.empty, "snappy", Map.empty)
      ),
      nodes = Seq.empty
    )
    
    val orchestrator = new DAGOrchestrator(dagConfig, globalConfig, autoDiscoverAdditionalTables = false)
    
    // Use reflection to access private applyFlattenJoin method
    val method = orchestrator.getClass.getDeclaredMethod(
      "applyFlattenJoin",
      classOf[DataFrame],
      classOf[DataFrame],
      classOf[JoinConfig]
    )
    method.setAccessible(true)
    method.invoke(orchestrator, parent, child, joinConfig).asInstanceOf[DataFrame]
  }
  
  /**
   * Property 23: Flatten Join Preserves Parent Columns
   * For any parent-child join with flatten strategy, all parent columns should be preserved.
   */
  property("flatten_join_preserves_parent_columns") = forAll(
    parentDataGen,
    flattenJoinConfigGen
  ) { (parentRecords, joinConfig) =>
    if (parentRecords.isEmpty) {
      true  // Skip empty parent data
    } else {
      try {
        val parentIds = parentRecords.map(_.id)
        val childRecords = childDataGen(parentIds).sample.get
        
        val parentDF = createParentDF(parentRecords)
        val childDF = createChildDF(childRecords)
        
        val result = applyFlattenJoin(parentDF, childDF, joinConfig)
        
        // All parent columns should be in result
        val parentColumns = parentDF.columns.toSet
        val resultColumns = result.columns.toSet
        
        parentColumns.subsetOf(resultColumns)
        
      } catch {
        case e: Exception =>
          println(s"Exception in flatten_join_preserves_parent_columns: ${e.getMessage}")
          e.printStackTrace()
          false
      }
    }
  }
  
  /**
   * Property 23: Flatten Join Includes Child Columns
   * For any parent-child join with flatten strategy, child columns (excluding join keys) 
   * should be included in the result.
   */
  property("flatten_join_includes_child_columns") = forAll(
    parentDataGen,
    flattenJoinConfigGen
  ) { (parentRecords, joinConfig) =>
    if (parentRecords.isEmpty) {
      true  // Skip empty parent data
    } else {
      try {
        val parentIds = parentRecords.map(_.id)
        val childRecords = childDataGen(parentIds).sample.get
        
        val parentDF = createParentDF(parentRecords)
        val childDF = createChildDF(childRecords)
        
        val result = applyFlattenJoin(parentDF, childDF, joinConfig)
        
        // Child data columns (excluding join keys) should be in result
        val childJoinKeys = joinConfig.on.map(_.right).toSet
        val childDataColumns = childDF.columns.toSet -- childJoinKeys
        val resultColumns = result.columns.toSet
        
        // Each child column should either be in result directly or with "child_" prefix
        childDataColumns.forall { childCol =>
          resultColumns.contains(childCol) || resultColumns.contains(s"child_$childCol")
        }
        
      } catch {
        case e: Exception =>
          println(s"Exception in flatten_join_includes_child_columns: ${e.getMessage}")
          e.printStackTrace()
          false
      }
    }
  }
  
  /**
   * Property 23: Flatten Join Handles Column Name Conflicts
   * For any parent-child join with flatten strategy where parent and child have columns 
   * with the same name, the child column should be prefixed with "child_".
   */
  property("flatten_join_handles_column_conflicts") = forAll(
    parentDataGen
  ) { parentRecords =>
    if (parentRecords.isEmpty) {
      true  // Skip empty parent data
    } else {
      try {
        val parentIds = parentRecords.map(_.id)
        // Create child records with conflicting column name "parent_name"
        val childRecords = parentIds.take(Math.min(5, parentIds.size)).map { id =>
          (id, "child_value", 42)
        }
        
        val parentDF = createParentDF(parentRecords)
        val childDF = childRecords.toDF("parent_id", "parent_name", "child_specific")
        
        val joinConfig = JoinConfig(
          `type` = "inner",
          parent = "parent_node",
          on = Seq(JoinCondition("id", "parent_id")),
          strategy = "flatten",
          nestAs = None,
          aggregations = Seq.empty
        )
        
        val result = applyFlattenJoin(parentDF, childDF, joinConfig)
        
        // Result should have both "parent_name" (from parent) and "child_parent_name" (from child)
        val resultColumns = result.columns.toSet
        
        resultColumns.contains("parent_name") && resultColumns.contains("child_parent_name")
        
      } catch {
        case e: Exception =>
          println(s"Exception in flatten_join_handles_column_conflicts: ${e.getMessage}")
          e.printStackTrace()
          false
      }
    }
  }
  
  /**
   * Property 23: Flatten Join Inner Join Produces Matching Records Only
   * For any parent-child inner join with flatten strategy, the result should only contain 
   * records where parent and child match.
   */
  property("flatten_join_inner_produces_matching_records") = forAll(
    parentDataGen
  ) { parentRecords =>
    if (parentRecords.isEmpty) {
      true  // Skip empty parent data
    } else {
      try {
        val parentIds = parentRecords.map(_.id)
        // Create children for only half of the parents
        val matchingParentIds = parentIds.take(Math.max(1, parentIds.size / 2))
        val childRecords = matchingParentIds.map { id =>
          ChildRecord(id, "child", 100)
        }
        
        val parentDF = createParentDF(parentRecords)
        val childDF = createChildDF(childRecords)
        
        val joinConfig = JoinConfig(
          `type` = "inner",
          parent = "parent_node",
          on = Seq(JoinCondition("id", "parent_id")),
          strategy = "flatten",
          nestAs = None,
          aggregations = Seq.empty
        )
        
        val result = applyFlattenJoin(parentDF, childDF, joinConfig)
        
        // Result count should equal number of matching records
        val resultCount = result.count()
        val expectedCount = childRecords.size
        
        resultCount == expectedCount
        
      } catch {
        case e: Exception =>
          println(s"Exception in flatten_join_inner_produces_matching_records: ${e.getMessage}")
          e.printStackTrace()
          false
      }
    }
  }
  
  /**
   * Property 23: Flatten Join Left Outer Preserves All Parent Records
   * For any parent-child left outer join with flatten strategy, all parent records should 
   * be preserved even if they have no matching children.
   */
  property("flatten_join_left_outer_preserves_parents") = forAll(
    parentDataGen
  ) { parentRecords =>
    if (parentRecords.isEmpty) {
      true  // Skip empty parent data
    } else {
      try {
        val parentIds = parentRecords.map(_.id)
        // Create children for only some parents
        val matchingParentIds = parentIds.take(Math.max(1, parentIds.size / 2))
        val childRecords = matchingParentIds.map { id =>
          ChildRecord(id, "child", 100)
        }
        
        val parentDF = createParentDF(parentRecords)
        val childDF = createChildDF(childRecords)
        
        val joinConfig = JoinConfig(
          `type` = "left_outer",
          parent = "parent_node",
          on = Seq(JoinCondition("id", "parent_id")),
          strategy = "flatten",
          nestAs = None,
          aggregations = Seq.empty
        )
        
        val result = applyFlattenJoin(parentDF, childDF, joinConfig)
        
        // All parent records should be in result
        val resultCount = result.count()
        val parentCount = parentRecords.size
        
        resultCount == parentCount
        
      } catch {
        case e: Exception =>
          println(s"Exception in flatten_join_left_outer_preserves_parents: ${e.getMessage}")
          e.printStackTrace()
          false
      }
    }
  }
}
