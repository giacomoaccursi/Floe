package com.etl.framework.merge

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import scala.util.Try

/**
 * Property-based tests for Append Merge
 * Feature: spark-etl-framework, Property 16: Append Merge Correctness
 * Validates: Requirements 10.2
 */
object AppendMergeProperties extends Properties("AppendMerge") {
  
  // Create a local Spark session for testing
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("AppendMergePropertiesTest")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  
  import spark.implicits._
  
  // Test record
  case class TestRecord(id: Int, name: String, value: Double)
  
  // Generator for test records
  val recordGen: Gen[TestRecord] = for {
    id <- Gen.choose(1, 1000)
    name <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    value <- Gen.choose(0.0, 1000.0)
  } yield TestRecord(id, name, value)
  
  // Generator for list of records
  val recordsGen: Gen[Seq[TestRecord]] = for {
    size <- Gen.choose(5, 30)
    records <- Gen.listOfN(size, recordGen)
  } yield records
  
  // Generator for two datasets
  val twoDatasetsGen: Gen[(Seq[TestRecord], Seq[TestRecord])] = for {
    existing <- recordsGen
    newRecords <- recordsGen
  } yield (existing, newRecords)
  
  /**
   * Property 16: Append Merge Correctness - Contains All Records
   * For any existing dataset and new dataset, append merge should result in a dataset
   * containing all records from both datasets
   */
  property("append_contains_all_records") = forAll(twoDatasetsGen) {
    case (existingRecords, newRecords) =>
      Try {
        if (existingRecords.isEmpty || newRecords.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()
          val newDf = newRecords.toDF()
          
          val merger = new AppendMerger()
          val result = merger.merge(newDf, Some(existingDf))
          
          // Result count should equal sum of both datasets
          val expectedCount = existingRecords.size + newRecords.size
          val actualCount = result.count()
          
          actualCount == expectedCount
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 16: Append Merge Correctness - Preserves Existing Records
   * For any existing dataset and new dataset, append merge should preserve all existing records
   */
  property("append_preserves_existing_records") = forAll(twoDatasetsGen) {
    case (existingRecords, newRecords) =>
      Try {
        if (existingRecords.isEmpty || newRecords.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()
          val newDf = newRecords.toDF()
          
          val merger = new AppendMerger()
          val result = merger.merge(newDf, Some(existingDf))
          
          // All existing records should be present in result
          val allExistingPresent = existingRecords.forall { record =>
            result.filter(
              $"id" === record.id &&
              $"name" === record.name &&
              $"value" === record.value
            ).count() >= 1
          }
          
          allExistingPresent
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 16: Append Merge Correctness - Preserves New Records
   * For any existing dataset and new dataset, append merge should preserve all new records
   */
  property("append_preserves_new_records") = forAll(twoDatasetsGen) {
    case (existingRecords, newRecords) =>
      Try {
        if (existingRecords.isEmpty || newRecords.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()
          val newDf = newRecords.toDF()
          
          val merger = new AppendMerger()
          val result = merger.merge(newDf, Some(existingDf))
          
          // All new records should be present in result
          val allNewPresent = newRecords.forall { record =>
            result.filter(
              $"id" === record.id &&
              $"name" === record.name &&
              $"value" === record.value
            ).count() >= 1
          }
          
          allNewPresent
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 16: Append Merge Correctness - Does Not Modify Records
   * For any existing dataset and new dataset, append merge should not modify any records
   */
  property("append_does_not_modify_records") = forAll(twoDatasetsGen) {
    case (existingRecords, newRecords) =>
      Try {
        if (existingRecords.isEmpty || newRecords.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()
          val newDf = newRecords.toDF()
          
          val merger = new AppendMerger()
          val result = merger.merge(newDf, Some(existingDf))
          
          // Collect all records from result
          val resultRecords = result.as[TestRecord].collect()
          
          // Every record in result should match either an existing or new record exactly
          val allRecordsUnmodified = resultRecords.forall { resultRecord =>
            existingRecords.exists(r =>
              r.id == resultRecord.id &&
              r.name == resultRecord.name &&
              r.value == resultRecord.value
            ) || newRecords.exists(r =>
              r.id == resultRecord.id &&
              r.name == resultRecord.name &&
              r.value == resultRecord.value
            )
          }
          
          allRecordsUnmodified
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 16: Append Merge Correctness - Allows Duplicates
   * For any datasets with duplicate keys, append merge should keep all duplicates
   */
  property("append_allows_duplicates") = forAll(Gen.choose(5, 20)) { size =>
    Try {
      // Create records with duplicate IDs
      val existingRecords = (1 to size).map { i =>
        TestRecord(i % 5, s"existing_$i", i.toDouble)
      }
      
      val newRecords = (1 to size).map { i =>
        TestRecord(i % 5, s"new_$i", (i * 10).toDouble)
      }
      
      val existingDf = existingRecords.toDF()
      val newDf = newRecords.toDF()
      
      val merger = new AppendMerger()
      val result = merger.merge(newDf, Some(existingDf))
      
      // Result should contain all records, including duplicates
      val expectedCount = existingRecords.size + newRecords.size
      val actualCount = result.count()
      
      // Result may have more duplicate IDs than unique IDs
      val uniqueIdCount = result.select("id").distinct().count()
      val allowsDuplicates = actualCount > uniqueIdCount
      
      actualCount == expectedCount && allowsDuplicates
    }.getOrElse(false)
  }
  
  /**
   * Property 16: Append Merge Correctness - First Load Returns New Data
   * For any new dataset with no existing data, append merge should return the new data as-is
   */
  property("append_first_load_returns_new_data") = forAll(recordsGen) { newRecords =>
    Try {
      if (newRecords.isEmpty) {
        true // Skip empty test cases
      } else {
        val newDf = newRecords.toDF()
        
        val merger = new AppendMerger()
        val result = merger.merge(newDf, None)
        
        // Result should be identical to new data
        val sameCount = result.count() == newDf.count()
        val sameSchema = result.schema == newDf.schema
        
        // All records should be present
        val allRecordsPresent = newRecords.forall { record =>
          result.filter(
            $"id" === record.id &&
            $"name" === record.name &&
            $"value" === record.value
          ).count() >= 1
        }
        
        sameCount && sameSchema && allRecordsPresent
      }
    }.getOrElse(false)
  }
  
  /**
   * Property 16: Append Merge Correctness - Preserves Schema
   * For any append merge, the result schema should match the input schema
   */
  property("append_preserves_schema") = forAll(twoDatasetsGen) {
    case (existingRecords, newRecords) =>
      Try {
        if (existingRecords.isEmpty || newRecords.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()
          val newDf = newRecords.toDF()
          
          val merger = new AppendMerger()
          val result = merger.merge(newDf, Some(existingDf))
          
          // Result schema should match input schema
          val sameSchema = result.schema == newDf.schema
          val sameColumns = result.columns.toSet == newDf.columns.toSet
          
          sameSchema && sameColumns
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 16: Append Merge Correctness - Empty Existing Dataset
   * For any new dataset with empty existing dataset, append should return only new data
   */
  property("append_with_empty_existing") = forAll(recordsGen) { newRecords =>
    Try {
      if (newRecords.isEmpty) {
        true // Skip empty test cases
      } else {
        val newDf = newRecords.toDF()
        val emptyDf = Seq.empty[TestRecord].toDF()
        
        val merger = new AppendMerger()
        val result = merger.merge(newDf, Some(emptyDf))
        
        // Result should contain only new records
        val sameCount = result.count() == newRecords.size
        
        // All new records should be present
        val allNewPresent = newRecords.forall { record =>
          result.filter(
            $"id" === record.id &&
            $"name" === record.name &&
            $"value" === record.value
          ).count() >= 1
        }
        
        sameCount && allNewPresent
      }
    }.getOrElse(false)
  }
  
  /**
   * Property 16: Append Merge Correctness - Empty New Dataset
   * For any empty new dataset with existing data, append should return only existing data
   */
  property("append_with_empty_new") = forAll(recordsGen) { existingRecords =>
    Try {
      if (existingRecords.isEmpty) {
        true // Skip empty test cases
      } else {
        val existingDf = existingRecords.toDF()
        val emptyDf = Seq.empty[TestRecord].toDF()
        
        val merger = new AppendMerger()
        val result = merger.merge(emptyDf, Some(existingDf))
        
        // Result should contain only existing records
        val sameCount = result.count() == existingRecords.size
        
        // All existing records should be present
        val allExistingPresent = existingRecords.forall { record =>
          result.filter(
            $"id" === record.id &&
            $"name" === record.name &&
            $"value" === record.value
          ).count() >= 1
        }
        
        sameCount && allExistingPresent
      }
    }.getOrElse(false)
  }
  
  /**
   * Property 16: Append Merge Correctness - Order Independence
   * For any two datasets, appending A to B should have same count as appending B to A
   */
  property("append_order_independence_count") = forAll(twoDatasetsGen) {
    case (datasetA, datasetB) =>
      Try {
        if (datasetA.isEmpty || datasetB.isEmpty) {
          true // Skip empty test cases
        } else {
          val dfA = datasetA.toDF()
          val dfB = datasetB.toDF()
          
          val merger = new AppendMerger()
          
          // Append B to A
          val resultAB = merger.merge(dfB, Some(dfA))
          
          // Append A to B
          val resultBA = merger.merge(dfA, Some(dfB))
          
          // Both should have the same count
          resultAB.count() == resultBA.count()
        }
      }.getOrElse(false)
  }
}
