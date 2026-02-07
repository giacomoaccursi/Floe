package com.etl.framework.aggregation

import com.etl.framework.config._
import com.etl.framework.TestConfig
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/** Property-based tests for nest join strategy Feature: spark-etl-framework,
  * Property 22: Nest Join Strategy Validates: Requirements 15.2
  *
  * Test configuration: Uses standardTestCount (20 cases)
  */
object NestJoinProperties extends Properties("NestJoin") {

  // Configure test parameters
  override def overrideParameters(p: Parameters): Parameters =
    TestConfig.standardParams

  // Create a minimal Spark session for testing
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("NestJoinPropertiesTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  // Generator for parent records
  case class ParentRecord(id: String, name: String, value: Int)

  val parentRecordGen: Gen[ParentRecord] = for {
    id <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    name <- Gen.alphaNumStr
    value <- Gen.choose(1, 1000)
  } yield ParentRecord(id, name, value)

  val parentDataGen: Gen[Seq[ParentRecord]] = for {
    count <- Gen.choose(1, 20)
    records <- Gen.listOfN(count, parentRecordGen)
  } yield records.groupBy(_.id).map(_._2.head).toSeq // Ensure unique IDs

  // Generator for child records
  case class ChildRecord(
      parent_id: String,
      child_name: String,
      child_value: Int
  )

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
  val nestJoinConfigGen: Gen[JoinConfig] = for {
    joinType <- Gen.oneOf(JoinType.values)
    nestAs <- Gen.alphaNumStr.suchThat(_.nonEmpty)
  } yield JoinConfig(
    `type` = joinType,
    parent = "parent_node",
    on = Seq(JoinCondition("id", "parent_id")),
    strategy = JoinStrategy.Nest,
    nestAs = Some(nestAs),
    aggregations = Seq.empty
  )

  // Helper to create DataFrames
  def createParentDF(records: Seq[ParentRecord]): DataFrame = {
    records.toDF("id", "name", "value")
  }

  def createChildDF(records: Seq[ChildRecord]): DataFrame = {
    records.toDF("parent_id", "child_name", "child_value")
  }

  // Helper to apply nest join using JoinStrategyExecutor directly
  def applyNestJoin(
      parent: DataFrame,
      child: DataFrame,
      joinConfig: JoinConfig
  ): DataFrame = {
    val joinExecutor = new JoinStrategyExecutor()
    joinExecutor.applyJoin(parent, child, joinConfig)
  }

  /** Property 22: Nest Join Creates Nested Array Field For any parent-child
    * join with strategy "nest", the result should have a nested array field
    * containing all related child records.
    */
  property("nest_join_creates_nested_array_field") = forAll(
    parentDataGen,
    nestJoinConfigGen
  ) { (parentRecords, joinConfig) =>
    if (parentRecords.isEmpty) {
      true // Skip empty parent data
    } else {
      try {
        val parentIds = parentRecords.map(_.id)
        val childRecords = childDataGen(parentIds).sample.get

        val parentDF = createParentDF(parentRecords)
        val childDF = createChildDF(childRecords)

        val result = applyNestJoin(parentDF, childDF, joinConfig)

        val nestFieldName = joinConfig.nestAs.getOrElse("nested_records")

        // Verify nested field exists
        val hasNestedField = result.columns.contains(nestFieldName)

        // Verify nested field is an array type
        val nestedFieldType = if (hasNestedField) {
          result.schema.fields.find(_.name == nestFieldName).map(_.dataType)
        } else {
          None
        }

        val isArrayType = nestedFieldType match {
          case Some(_: ArrayType) => true
          case _                  => false
        }

        hasNestedField && isArrayType

      } catch {
        case e: Exception =>
          println(
            s"Exception in nest_join_creates_nested_array_field: ${e.getMessage}"
          )
          e.printStackTrace()
          false
      }
    }
  }

  /** Property 22: Nest Join Preserves Parent Records (Left Outer) For any
    * parent-child left outer join with nest strategy, all parent records should
    * be preserved.
    */
  property("nest_join_preserves_parent_records_left_outer") = forAll(
    parentDataGen
  ) { parentRecords =>
    if (parentRecords.isEmpty) {
      true // Skip empty parent data
    } else {
      try {
        val parentIds = parentRecords.map(_.id)
        val childRecords = childDataGen(parentIds).sample.get

        val parentDF = createParentDF(parentRecords)
        val childDF = createChildDF(childRecords)

        val joinConfig = JoinConfig(
          `type` = JoinType.LeftOuter,
          parent = "parent_node",
          on = Seq(JoinCondition("id", "parent_id")),
          strategy = JoinStrategy.Nest,
          nestAs = Some("children"),
          aggregations = Seq.empty
        )

        val result = applyNestJoin(parentDF, childDF, joinConfig)

        // All parent records should be in result
        val resultCount = result.count()
        val parentCount = parentRecords.size

        resultCount == parentCount

      } catch {
        case e: Exception =>
          println(
            s"Exception in nest_join_preserves_parent_records_left_outer: ${e.getMessage}"
          )
          e.printStackTrace()
          false
      }
    }
  }

  /** Property 22: Nest Join Groups Child Records by Parent For any parent-child
    * join with nest strategy, each parent should have an array containing only
    * its related child records.
    */
  property("nest_join_groups_child_records_by_parent") = forAll(
    parentDataGen
  ) { parentRecords =>
    if (parentRecords.isEmpty) {
      true // Skip empty parent data
    } else {
      try {
        val parentIds = parentRecords.map(_.id)
        val childRecords = childDataGen(parentIds).sample.get

        val parentDF = createParentDF(parentRecords)
        val childDF = createChildDF(childRecords)

        val joinConfig = JoinConfig(
          `type` = JoinType.LeftOuter,
          parent = "parent_node",
          on = Seq(JoinCondition("id", "parent_id")),
          strategy = JoinStrategy.Nest,
          nestAs = Some("children"),
          aggregations = Seq.empty
        )

        val result = applyNestJoin(parentDF, childDF, joinConfig)

        // Count expected children per parent
        val expectedChildrenPerParent =
          childRecords.groupBy(_.parent_id).mapValues(_.size)

        // Verify each parent has correct number of children
        val resultData = result.collect()

        val allCorrect = resultData.forall { row =>
          val parentId = row.getAs[String]("id")
          val children = row.getAs[Seq[Row]]("children")
          val expectedCount = expectedChildrenPerParent.getOrElse(parentId, 0)

          children.size == expectedCount
        }

        allCorrect

      } catch {
        case e: Exception =>
          println(
            s"Exception in nest_join_groups_child_records_by_parent: ${e.getMessage}"
          )
          e.printStackTrace()
          false
      }
    }
  }

  /** Property 22: Nest Join Preserves Parent Columns For any parent-child join
    * with nest strategy, all parent columns should be preserved in the result.
    */
  property("nest_join_preserves_parent_columns") = forAll(
    parentDataGen,
    nestJoinConfigGen
  ) { (parentRecords, joinConfig) =>
    if (parentRecords.isEmpty) {
      true // Skip empty parent data
    } else {
      try {
        val parentIds = parentRecords.map(_.id)
        val childRecords = childDataGen(parentIds).sample.get

        val parentDF = createParentDF(parentRecords)
        val childDF = createChildDF(childRecords)

        val result = applyNestJoin(parentDF, childDF, joinConfig)

        // All parent columns should be in result
        val parentColumns = parentDF.columns.toSet
        val resultColumns = result.columns.toSet

        parentColumns.subsetOf(resultColumns)

      } catch {
        case e: Exception =>
          println(
            s"Exception in nest_join_preserves_parent_columns: ${e.getMessage}"
          )
          e.printStackTrace()
          false
      }
    }
  }

  /** Property 22: Nest Join With Empty Child Produces Empty Arrays For any
    * parent records with no matching children, the nested array should be empty
    * (not null).
    */
  property("nest_join_with_no_children_produces_empty_arrays") = forAll(
    parentDataGen
  ) { parentRecords =>
    if (parentRecords.isEmpty) {
      true // Skip empty parent data
    } else {
      try {
        val parentDF = createParentDF(parentRecords)
        // Create empty child DataFrame
        val childDF = createChildDF(Seq.empty)

        val joinConfig = JoinConfig(
          `type` = JoinType.LeftOuter,
          parent = "parent_node",
          on = Seq(JoinCondition("id", "parent_id")),
          strategy = JoinStrategy.Nest,
          nestAs = Some("children"),
          aggregations = Seq.empty
        )

        val result = applyNestJoin(parentDF, childDF, joinConfig)

        // All parents should have empty arrays (not null)
        val resultData = result.collect()

        val allHaveEmptyArrays = resultData.forall { row =>
          val children = row.getAs[Seq[Row]]("children")
          children != null && children.isEmpty
        }

        allHaveEmptyArrays

      } catch {
        case e: Exception =>
          println(
            s"Exception in nest_join_with_no_children_produces_empty_arrays: ${e.getMessage}"
          )
          e.printStackTrace()
          false
      }
    }
  }
}
