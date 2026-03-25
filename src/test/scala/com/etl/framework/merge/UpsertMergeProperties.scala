package com.etl.framework.merge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import scala.util.Try
import java.sql.Timestamp

/**
 * Property-based tests for Upsert Merge
 * Feature: spark-etl-framework, Property 15: Upsert Merge Correctness
 * Validates: Requirements 10.1
 */
object UpsertMergeProperties extends Properties("UpsertMerge") {
  
  // Create a local Spark session for testing
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("UpsertMergePropertiesTest")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  
  import spark.implicits._
  
  // Test record with timestamp
  case class TestRecord(id: Int, name: String, value: Double, updated_at: Timestamp)
  
  // Test record without timestamp
  case class SimpleRecord(id: Int, name: String, value: Double)
  
  // Generator for timestamps
  val timestampGen: Gen[Timestamp] = for {
    millis <- Gen.choose(1000000000000L, 1700000000000L)
  } yield new Timestamp(millis)
  
  // Generator for test records with timestamp
  val recordWithTimestampGen: Gen[TestRecord] = for {
    id <- Gen.choose(1, 100)
    name <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    value <- Gen.choose(0.0, 1000.0)
    ts <- timestampGen
  } yield TestRecord(id, name, value, ts)
  
  // Generator for simple records
  val simpleRecordGen: Gen[SimpleRecord] = for {
    id <- Gen.choose(1, 100)
    name <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    value <- Gen.choose(0.0, 1000.0)
  } yield SimpleRecord(id, name, value)
  
  // Generator for existing and new datasets with overlaps
  val overlappingDatasetsGen: Gen[(Seq[SimpleRecord], Seq[SimpleRecord], Set[Int])] = for {
    existingSize <- Gen.choose(5, 20)
    newSize <- Gen.choose(5, 20)
    overlapSize <- Gen.choose(1, Math.min(existingSize, newSize))
    
    // Generate existing records with sequential IDs
    existingRecords <- Gen.listOfN(existingSize, simpleRecordGen).map { records =>
      records.zipWithIndex.map { case (rec, idx) => rec.copy(id = idx + 1) }
    }
    
    // Generate new records - some overlap with existing, some are new
    overlapIds <- Gen.pick(overlapSize, existingRecords.map(_.id))
    newOnlySize = newSize - overlapSize
    newOnlyIds = (existingSize + 1) to (existingSize + newOnlySize)
    
    newRecords <- Gen.sequence[Seq[SimpleRecord], SimpleRecord](
      (overlapIds.map(id => simpleRecordGen.map(_.copy(id = id))) ++
       newOnlyIds.map(id => simpleRecordGen.map(_.copy(id = id)))).toSeq
    )
  } yield (existingRecords, newRecords, overlapIds.toSet)
  
  // Generator for datasets with timestamps
  val overlappingDatasetsWithTimestampGen: Gen[(Seq[TestRecord], Seq[TestRecord], Set[Int])] = for {
    existingSize <- Gen.choose(5, 20)
    newSize <- Gen.choose(5, 20)
    overlapSize <- Gen.choose(1, Math.min(existingSize, newSize))
    
    baseTimestamp <- timestampGen
    
    // Generate existing records with older timestamps
    existingRecords <- Gen.listOfN(existingSize, recordWithTimestampGen).map { records =>
      records.zipWithIndex.map { case (rec, idx) =>
        rec.copy(
          id = idx + 1,
          updated_at = new Timestamp(baseTimestamp.getTime - 10000)
        )
      }
    }
    
    // Generate new records - some overlap with existing (newer timestamps), some are new
    overlapIds <- Gen.pick(overlapSize, existingRecords.map(_.id))
    newOnlySize = newSize - overlapSize
    newOnlyIds = (existingSize + 1) to (existingSize + newOnlySize)
    
    newRecords <- Gen.sequence[Seq[TestRecord], TestRecord](
      (overlapIds.map(id => recordWithTimestampGen.map(rec =>
        rec.copy(id = id, updated_at = new Timestamp(baseTimestamp.getTime + 10000))
      )) ++
       newOnlyIds.map(id => recordWithTimestampGen.map(rec =>
        rec.copy(id = id, updated_at = new Timestamp(baseTimestamp.getTime + 10000))
      ))).toSeq
    )
  } yield (existingRecords, newRecords, overlapIds.toSet)
  
  /**
   * Property 15: Upsert Merge Correctness - Updates Existing Records
   * For any existing dataset and new dataset with overlapping keys,
   * upsert merge should update all matching records
   */
  property("upsert_updates_existing_records") = forAll(overlappingDatasetsGen) {
    case (existingRecords, newRecords, overlapIds) =>
      Try {
        if (existingRecords.isEmpty || newRecords.isEmpty || overlapIds.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()
          val newDf = newRecords.toDF()
          
          val merger = new UpsertMerger(Seq("id"))
          val result = merger.merge(newDf, Some(existingDf))
          
          // For each overlapping ID, the result should contain the new record's data
          val updatesCorrect = overlapIds.forall { id =>
            val newRecord = newRecords.find(_.id == id).get
            val resultRecord = result.filter($"id" === id).as[SimpleRecord].head()
            
            resultRecord.name == newRecord.name &&
            resultRecord.value == newRecord.value
          }
          
          // Result should contain all unique IDs from both datasets
          val allIds = (existingRecords.map(_.id) ++ newRecords.map(_.id)).toSet
          val resultIds = result.select("id").as[Int].collect().toSet
          val hasAllIds = resultIds == allIds
          
          // Each ID should appear exactly once
          val noDuplicates = result.count() == allIds.size
          
          updatesCorrect && hasAllIds && noDuplicates
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 15: Upsert Merge Correctness - Inserts New Records
   * For any existing dataset and new dataset, upsert merge should insert all new records
   */
  property("upsert_inserts_new_records") = forAll(overlappingDatasetsGen) {
    case (existingRecords, newRecords, overlapIds) =>
      Try {
        if (existingRecords.isEmpty || newRecords.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()
          val newDf = newRecords.toDF()
          
          val merger = new UpsertMerger(Seq("id"))
          val result = merger.merge(newDf, Some(existingDf))
          
          // Find IDs that are only in new records
          val existingIds = existingRecords.map(_.id).toSet
          val newOnlyIds = newRecords.map(_.id).toSet -- existingIds
          
          // All new-only IDs should be in the result
          val allNewIdsPresent = newOnlyIds.forall { id =>
            result.filter($"id" === id).count() == 1
          }
          
          // For each new-only ID, the data should match the new record
          val newDataCorrect = newOnlyIds.forall { id =>
            val newRecord = newRecords.find(_.id == id).get
            val resultRecord = result.filter($"id" === id).as[SimpleRecord].head()
            
            resultRecord.name == newRecord.name &&
            resultRecord.value == newRecord.value
          }
          
          allNewIdsPresent && newDataCorrect
        }
      }.getOrElse(false)
  }
  
  /**
   * Property: Upsert Merge Idempotency
   * Re-merging the same data into an existing dataset leaves the result unchanged
   */
  property("upsert_idempotent_on_same_data") = forAll(overlappingDatasetsGen) {
    case (existingRecords, _, _) =>
      Try {
        if (existingRecords.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()

          val merger = new UpsertMerger(Seq("id"))
          // Merge existing with itself — semantically a no-op
          val result = merger.merge(existingDf, Some(existingDf))

          // Count and key set must be unchanged
          val sameCount = result.count() == existingDf.count()
          val allIds = existingRecords.map(_.id).toSet
          val resultIds = result.select("id").as[Int].collect().toSet
          sameCount && resultIds == allIds
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 15: Upsert Merge Correctness - First Load Returns New Data
   * For any new dataset with no existing data, upsert merge should return the new data as-is
   */
  property("upsert_first_load_returns_new_data") = forAll(Gen.choose(5, 20)) { size =>
    Try {
      // Generate records with unique IDs
      val newRecords = (1 to size).map { i =>
        SimpleRecord(i, s"name_$i", i.toDouble * 10)
      }
      
      val newDf = newRecords.toDF()
      
      val merger = new UpsertMerger(Seq("id"))
      val result = merger.merge(newDf, None)
      
      // Result should be identical to new data
      val sameCount = result.count() == newDf.count()
      val sameSchema = result.schema == newDf.schema
      
      // All records should be present
      val allRecordsPresent = newRecords.forall { record =>
        result.filter($"id" === record.id).count() == 1
      }
      
      sameCount && sameSchema && allRecordsPresent
    }.getOrElse(false)
  }
  
  /**
   * Property 15: Upsert Merge Correctness - Preserves Non-Key Columns
   * For any upsert merge, all non-key columns should be preserved correctly
   */
  property("upsert_preserves_non_key_columns") = forAll(overlappingDatasetsGen) {
    case (existingRecords, newRecords, overlapIds) =>
      Try {
        if (existingRecords.isEmpty || newRecords.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()
          val newDf = newRecords.toDF()
          
          val merger = new UpsertMerger(Seq("id"))
          val result = merger.merge(newDf, Some(existingDf))
          
          // Result should have all columns from input
          val hasAllColumns = result.columns.toSet == newDf.columns.toSet
          
          // For updated records, non-key columns should match new data
          val updatedColumnsCorrect = overlapIds.forall { id =>
            val newRecord = newRecords.find(_.id == id).get
            val resultRecord = result.filter($"id" === id).as[SimpleRecord].head()
            
            resultRecord.name == newRecord.name &&
            resultRecord.value == newRecord.value
          }
          
          hasAllColumns && updatedColumnsCorrect
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 15: Upsert Merge Correctness - Composite Key Support
   * For any datasets with composite keys, upsert merge should handle them correctly
   */
  property("upsert_handles_composite_keys") = forAll(Gen.choose(5, 20)) { size =>
    Try {
      // Create records with composite key (id, name) - ensure unique combinations
      val existingRecords = (1 to size).map { i =>
        SimpleRecord(i, s"name_$i", i.toDouble)
      }
      
      // Create new records - some overlap, some new
      val overlapSize = size / 2
      val newRecords = (1 to size).map { i =>
        if (i <= overlapSize) {
          // Overlap with existing - same keys, different values
          SimpleRecord(i, s"name_$i", (i * 10).toDouble)
        } else {
          // New records
          SimpleRecord(size + i, s"name_${size + i}", (i * 10).toDouble)
        }
      }
      
      val existingDf = existingRecords.toDF()
      val newDf = newRecords.toDF()
      
      val merger = new UpsertMerger(Seq("id", "name"))
      val result = merger.merge(newDf, Some(existingDf))
      
      // Each composite key should appear exactly once
      val noDuplicates = {
        val totalCount = result.count()
        val distinctCount = result.select("id", "name").distinct().count()
        totalCount == distinctCount
      }
      
      // Result should contain all unique composite keys from both datasets
      val allCompositeKeys = (existingRecords.map(r => (r.id, r.name)) ++
                              newRecords.map(r => (r.id, r.name))).toSet
      val resultCompositeKeys = result.select("id", "name")
        .collect()
        .map(row => (row.getInt(0), row.getString(1)))
        .toSet
      
      val hasAllKeys = resultCompositeKeys == allCompositeKeys
      
      noDuplicates && hasAllKeys
    }.getOrElse(false)
  }
  
  /**
   * Property 15: Upsert Merge Correctness - Empty Existing Dataset
   * For any new dataset with empty existing dataset, upsert should behave like first load
   */
  property("upsert_with_empty_existing_behaves_like_first_load") = forAll(Gen.choose(5, 20)) { size =>
    Try {
      // Generate records with unique IDs
      val newRecords = (1 to size).map { i =>
        SimpleRecord(i, s"name_$i", i.toDouble * 10)
      }
      
      val newDf = newRecords.toDF()
      val emptyDf = Seq.empty[SimpleRecord].toDF()
      
      val merger = new UpsertMerger(Seq("id"))
      val result = merger.merge(newDf, Some(emptyDf))
      
      // Result should be identical to new data
      val sameCount = result.count() == newDf.count()
      
      // All records should be present
      val allRecordsPresent = newRecords.forall { record =>
        result.filter($"id" === record.id).count() == 1
      }
      
      sameCount && allRecordsPresent
    }.getOrElse(false)
  }
  
  /**
   * Property 15: Upsert Merge Correctness - Record Count Invariant
   * For any upsert merge, result count should equal unique keys from both datasets
   */
  property("upsert_record_count_invariant") = forAll(overlappingDatasetsGen) {
    case (existingRecords, newRecords, _) =>
      Try {
        if (existingRecords.isEmpty || newRecords.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()
          val newDf = newRecords.toDF()
          
          val merger = new UpsertMerger(Seq("id"))
          val result = merger.merge(newDf, Some(existingDf))
          
          // Count unique IDs from both datasets
          val uniqueIds = (existingRecords.map(_.id) ++ newRecords.map(_.id)).toSet
          val expectedCount = uniqueIds.size
          
          // Result count should match unique ID count
          result.count() == expectedCount
        }
      }.getOrElse(false)
  }
}
