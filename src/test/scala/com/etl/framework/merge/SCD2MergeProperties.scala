package com.etl.framework.merge

import com.etl.framework.TestConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters
import scala.util.Try
import java.sql.Timestamp

/**
 * Property-based tests for SCD2 Merge
 * Feature: spark-etl-framework, Property 17: SCD2 Historicization
 * Validates: Requirements 10.4
 * 
 * Test configuration: Uses expensiveTestCount (10 cases) due to complex Spark operations
 */
object SCD2MergeProperties extends Properties("SCD2Merge") {
  
  // Configure test parameters for expensive SCD2 operations
  override def overrideParameters(p: Parameters): Parameters = TestConfig.expensiveParams
  
  // Create a local Spark session for testing
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("SCD2MergePropertiesTest")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  
  import spark.implicits._
  
  // Test record for SCD2
  case class SCD2Record(
    id: Int,
    name: String,
    value: Double,
    valid_from: Timestamp,
    valid_to: Timestamp,
    is_current: Boolean
  )
  
  // Simple record without SCD2 fields
  case class SimpleRecord(id: Int, name: String, value: Double)
  
  // Generator for timestamps
  val timestampGen: Gen[Timestamp] = for {
    millis <- Gen.choose(1000000000000L, 1700000000000L)
  } yield new Timestamp(millis)
  
  // Generator for simple records
  val simpleRecordGen: Gen[SimpleRecord] = for {
    id <- Gen.choose(1, 100)
    name <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    value <- Gen.choose(0.0, 1000.0)
  } yield SimpleRecord(id, name, value)
  
  // Generator for existing SCD2 records (all current)
  val existingSCD2RecordsGen: Gen[Seq[SCD2Record]] = for {
    size <- Gen.choose(5, 20)
    baseTimestamp <- timestampGen
    records <- Gen.listOfN(size, simpleRecordGen).map { simpleRecords =>
      simpleRecords.zipWithIndex.map { case (rec, idx) =>
        SCD2Record(
          id = idx + 1,
          name = rec.name,
          value = rec.value,
          valid_from = new Timestamp(baseTimestamp.getTime - 100000),
          valid_to = null,
          is_current = true
        )
      }
    }
  } yield records
  
  // Generator for new records with some changes
  val newRecordsWithChangesGen: Gen[(Seq[SCD2Record], Seq[SimpleRecord], Set[Int])] = for {
    existingRecords <- existingSCD2RecordsGen
    changeCount <- Gen.choose(1, Math.min(5, existingRecords.size))
    indicesToChange <- Gen.pick(changeCount, existingRecords.indices)
    
    // Create new records with changes
    newRecords = existingRecords.map { existing =>
      if (indicesToChange.contains(existingRecords.indexOf(existing))) {
        // Changed record - modify the value
        SimpleRecord(existing.id, s"${existing.name}_updated", existing.value * 2)
      } else {
        // Unchanged record
        SimpleRecord(existing.id, existing.name, existing.value)
      }
    }
    
    changedIds = indicesToChange.map(idx => existingRecords(idx).id).toSet
  } yield (existingRecords, newRecords, changedIds)
  
  /**
   * Property 17: SCD2 Historicization - First Load Creates Current Records
   * For any new dataset with no existing data, SCD2 merge should create all records as current
   */
  property("scd2_first_load_creates_current_records") = forAll(Gen.listOfN(10, simpleRecordGen)) { newRecords =>
    Try {
      if (newRecords.isEmpty) {
        true // Skip empty test cases
      } else {
        val newDf = newRecords.toDF()
        
        val merger = new SCD2Merger(
          keyColumns = Seq("id"),
          compareColumns = Seq("name", "value"),
          validFromCol = "valid_from",
          validToCol = "valid_to",
          isCurrentCol = "is_current"
        )
        
        val result = merger.merge(newDf, None)
        
        // All records should be current
        val allCurrent = result.filter($"is_current" === true).count() == newRecords.size
        
        // All records should have valid_from set
        val allHaveValidFrom = result.filter($"valid_from".isNotNull).count() == newRecords.size
        
        // All records should have valid_to as null
        val allHaveNullValidTo = result.filter($"valid_to".isNull).count() == newRecords.size
        
        // Should have SCD2 columns
        val hasSCD2Columns = result.columns.contains("valid_from") &&
                             result.columns.contains("valid_to") &&
                             result.columns.contains("is_current")
        
        allCurrent && allHaveValidFrom && allHaveNullValidTo && hasSCD2Columns
      }
    }.getOrElse(false)
  }
  
  /**
   * Property 17: SCD2 Historicization - Changed Records Create New Versions
   * For any changed records, SCD2 merge should create new versions with is_current=true
   */
  property("scd2_changed_records_create_new_versions") = forAll(newRecordsWithChangesGen) {
    case (existingRecords, newRecords, changedIds) =>
      Try {
        if (existingRecords.isEmpty || newRecords.isEmpty || changedIds.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()
          val newDf = newRecords.toDF()
          
          val merger = new SCD2Merger(
            keyColumns = Seq("id"),
            compareColumns = Seq("name", "value"),
            validFromCol = "valid_from",
            validToCol = "valid_to",
            isCurrentCol = "is_current"
          )
          
          val result = merger.merge(newDf, Some(existingDf))
          
          // For each changed ID, there should be a new current version
          val newVersionsExist = changedIds.forall { id =>
            val currentVersions = result.filter($"id" === id && $"is_current" === true)
            currentVersions.count() == 1
          }
          
          // New versions should have updated data
          val newVersionsHaveUpdatedData = changedIds.forall { id =>
            val newRecord = newRecords.find(_.id == id).get
            val currentVersion = result
              .filter($"id" === id && $"is_current" === true)
              .as[SCD2Record]
              .head()
            
            currentVersion.name == newRecord.name &&
            currentVersion.value == newRecord.value
          }
          
          newVersionsExist && newVersionsHaveUpdatedData
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 17: SCD2 Historicization - Old Versions Are Closed
   * For any changed records, SCD2 merge should close old versions (set valid_to, is_current=false)
   */
  property("scd2_old_versions_are_closed") = forAll(newRecordsWithChangesGen) {
    case (existingRecords, newRecords, changedIds) =>
      Try {
        if (existingRecords.isEmpty || newRecords.isEmpty || changedIds.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()
          val newDf = newRecords.toDF()
          
          val merger = new SCD2Merger(
            keyColumns = Seq("id"),
            compareColumns = Seq("name", "value"),
            validFromCol = "valid_from",
            validToCol = "valid_to",
            isCurrentCol = "is_current"
          )
          
          val result = merger.merge(newDf, Some(existingDf))
          
          // For each changed ID, there should be a closed old version
          val oldVersionsClosed = changedIds.forall { id =>
            val closedVersions = result.filter(
              $"id" === id &&
              $"is_current" === false &&
              $"valid_to".isNotNull
            )
            closedVersions.count() >= 1
          }
          
          // Closed versions should have valid_to set
          val closedVersionsHaveValidTo = {
            val closedRecords = result.filter($"is_current" === false)
            if (closedRecords.count() > 0) {
              closedRecords.filter($"valid_to".isNotNull).count() == closedRecords.count()
            } else {
              true
            }
          }
          
          oldVersionsClosed && closedVersionsHaveValidTo
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 17: SCD2 Historicization - Unchanged Records Remain Current
   * For any unchanged records, SCD2 merge should keep them as current without modification
   */
  property("scd2_unchanged_records_remain_current") = forAll(newRecordsWithChangesGen) {
    case (existingRecords, newRecords, changedIds) =>
      Try {
        if (existingRecords.isEmpty || newRecords.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()
          val newDf = newRecords.toDF()
          
          val merger = new SCD2Merger(
            keyColumns = Seq("id"),
            compareColumns = Seq("name", "value"),
            validFromCol = "valid_from",
            validToCol = "valid_to",
            isCurrentCol = "is_current"
          )
          
          val result = merger.merge(newDf, Some(existingDf))
          
          // Find unchanged IDs
          val unchangedIds = existingRecords.map(_.id).toSet -- changedIds
          
          // For each unchanged ID, there should be exactly one current version
          val unchangedRemainCurrent = unchangedIds.forall { id =>
            val currentVersions = result.filter($"id" === id && $"is_current" === true)
            currentVersions.count() == 1
          }
          
          // Unchanged records should have valid_to as null
          val unchangedHaveNullValidTo = unchangedIds.forall { id =>
            val currentVersion = result.filter($"id" === id && $"is_current" === true)
            currentVersion.filter($"valid_to".isNull).count() == 1
          }
          
          unchangedRemainCurrent && unchangedHaveNullValidTo
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 17: SCD2 Historicization - Each Key Has Exactly One Current Version
   * For any SCD2 merge result, each key should have exactly one current version
   */
  property("scd2_each_key_has_one_current_version") = forAll(newRecordsWithChangesGen) {
    case (existingRecords, newRecords, _) =>
      Try {
        if (existingRecords.isEmpty || newRecords.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()
          val newDf = newRecords.toDF()
          
          val merger = new SCD2Merger(
            keyColumns = Seq("id"),
            compareColumns = Seq("name", "value"),
            validFromCol = "valid_from",
            validToCol = "valid_to",
            isCurrentCol = "is_current"
          )
          
          val result = merger.merge(newDf, Some(existingDf))
          
          // Get all unique IDs
          val allIds = result.select("id").distinct().as[Int].collect()
          
          // Each ID should have exactly one current version
          allIds.forall { id =>
            result.filter($"id" === id && $"is_current" === true).count() == 1
          }
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 17: SCD2 Historicization - Historical Records Are Preserved
   * For any SCD2 merge, all historical (non-current) records should be preserved
   */
  property("scd2_historical_records_preserved") = forAll(Gen.choose(5, 15)) { size =>
    Try {
      // Create existing records with some historical versions
      val baseTimestamp = new Timestamp(System.currentTimeMillis() - 1000000)
      
      val existingRecords = (1 to size).flatMap { i =>
        Seq(
          // Historical version
          SCD2Record(
            id = i,
            name = s"old_name_$i",
            value = i.toDouble,
            valid_from = new Timestamp(baseTimestamp.getTime - 200000),
            valid_to = new Timestamp(baseTimestamp.getTime - 100000),
            is_current = false
          ),
          // Current version
          SCD2Record(
            id = i,
            name = s"current_name_$i",
            value = (i * 10).toDouble,
            valid_from = new Timestamp(baseTimestamp.getTime - 100000),
            valid_to = null,
            is_current = true
          )
        )
      }
      
      val historicalCount = existingRecords.count(!_.is_current)
      
      // Create new records (some changed, some unchanged)
      val newRecords = (1 to size).map { i =>
        if (i % 3 == 0) {
          // Changed record
          SimpleRecord(i, s"new_name_$i", (i * 100).toDouble)
        } else {
          // Unchanged record
          SimpleRecord(i, s"current_name_$i", (i * 10).toDouble)
        }
      }
      
      val existingDf = existingRecords.toDF()
      val newDf = newRecords.toDF()
      
      val merger = new SCD2Merger(
        keyColumns = Seq("id"),
        compareColumns = Seq("name", "value"),
        validFromCol = "valid_from",
        validToCol = "valid_to",
        isCurrentCol = "is_current"
      )
      
      val result = merger.merge(newDf, Some(existingDf))
      
      // Historical records should still be present
      val historicalPreserved = result.filter($"is_current" === false).count() >= historicalCount
      
      historicalPreserved
    }.getOrElse(false)
  }
  
  /**
   * Property 17: SCD2 Historicization - New Records Are Added As Current
   * For any new records not in existing data, SCD2 merge should add them as current
   */
  property("scd2_new_records_added_as_current") = forAll(existingSCD2RecordsGen, Gen.listOfN(5, simpleRecordGen)) {
    (existingRecords, additionalRecords) =>
      Try {
        if (existingRecords.isEmpty || additionalRecords.isEmpty) {
          true // Skip empty test cases
        } else {
          // Ensure additional records have IDs not in existing
          val existingIds = existingRecords.map(_.id).toSet
          val maxId = existingIds.max
          val newRecords = additionalRecords.zipWithIndex.map { case (rec, idx) =>
            rec.copy(id = maxId + idx + 1)
          }
          
          val existingDf = existingRecords.toDF()
          val newDf = (existingRecords.map(r => SimpleRecord(r.id, r.name, r.value)) ++ newRecords).toDF()
          
          val merger = new SCD2Merger(
            keyColumns = Seq("id"),
            compareColumns = Seq("name", "value"),
            validFromCol = "valid_from",
            validToCol = "valid_to",
            isCurrentCol = "is_current"
          )
          
          val result = merger.merge(newDf, Some(existingDf))
          
          // All new IDs should be present and current
          val newIds = newRecords.map(_.id).toSet
          val allNewIdsPresent = newIds.forall { id =>
            result.filter($"id" === id && $"is_current" === true).count() == 1
          }
          
          // New records should have valid_to as null
          val newRecordsHaveNullValidTo = newIds.forall { id =>
            result.filter($"id" === id && $"is_current" === true && $"valid_to".isNull).count() == 1
          }
          
          allNewIdsPresent && newRecordsHaveNullValidTo
        }
      }.getOrElse(false)
  }
  
  /**
   * Property 17: SCD2 Historicization - Composite Key Support
   * For any datasets with composite keys, SCD2 merge should handle them correctly
   */
  property("scd2_handles_composite_keys") = forAll(Gen.choose(5, 15)) { size =>
    Try {
      import org.apache.spark.sql.Row
      import org.apache.spark.sql.types._
      
      val baseTimestamp = new Timestamp(System.currentTimeMillis() - 100000)
      
      // Define schema for existing records
      val existingSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("value", DoubleType, nullable = false),
        StructField("valid_from", TimestampType, nullable = false),
        StructField("valid_to", TimestampType, nullable = true),
        StructField("is_current", BooleanType, nullable = false)
      ))
      
      // Define schema for new records
      val newSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("value", DoubleType, nullable = false)
      ))
      
      val existingRows = (1 to size).map { i =>
        Row(
          i % 5,
          s"name_${i % 3}",
          i.toDouble,
          baseTimestamp,
          null,
          true
        )
      }
      
      val newRows = (1 to size).map { i =>
        Row(
          i % 5,
          s"name_${i % 3}",
          (i * 10).toDouble
        )
      }
      
      val existingDf = spark.createDataFrame(
        spark.sparkContext.parallelize(existingRows),
        existingSchema
      )
      val newDf = spark.createDataFrame(
        spark.sparkContext.parallelize(newRows),
        newSchema
      )
      
      val merger = new SCD2Merger(
        keyColumns = Seq("id", "name"),
        compareColumns = Seq("value"),
        validFromCol = "valid_from",
        validToCol = "valid_to",
        isCurrentCol = "is_current"
      )
      
      val result = merger.merge(newDf, Some(existingDf))
      
      // Each composite key should have exactly one current version
      val compositeKeys = result
        .select("id", "name")
        .distinct()
        .collect()
        .map(row => (row.getInt(0), row.getString(1)))
      
      compositeKeys.forall { case (id, name) =>
        result.filter(
          $"id" === id &&
          $"name" === name &&
          $"is_current" === true
        ).count() == 1
      }
    }.getOrElse(false)
  }
  
  /**
   * Property 17: SCD2 Historicization - Valid From/To Consistency
   * For any closed record, valid_to should be after valid_from
   */
  property("scd2_valid_from_to_consistency") = forAll(newRecordsWithChangesGen) {
    case (existingRecords, newRecords, _) =>
      Try {
        if (existingRecords.isEmpty || newRecords.isEmpty) {
          true // Skip empty test cases
        } else {
          val existingDf = existingRecords.toDF()
          val newDf = newRecords.toDF()

          val merger = new SCD2Merger(
            keyColumns = Seq("id"),
            compareColumns = Seq("name", "value"),
            validFromCol = "valid_from",
            validToCol = "valid_to",
            isCurrentCol = "is_current"
          )

          val result = merger.merge(newDf, Some(existingDf))

          // For all closed records, valid_to should be after valid_from
          val closedRecords = result.filter($"is_current" === false && $"valid_to".isNotNull)

          if (closedRecords.count() > 0) {
            closedRecords
              .filter($"valid_to" > $"valid_from")
              .count() == closedRecords.count()
          } else {
            true
          }
        }
      }.getOrElse(false)
  }

  // SCD2 record with is_active for detectDeletes tests
  case class SCD2RecordActive(
    id: Int,
    name: String,
    value: Double,
    valid_from: Timestamp,
    valid_to: Timestamp,
    is_current: Boolean,
    is_active: Boolean
  )

  /**
   * Property: SCD2 Detect Deletes - Deleted Records Are Closed
   * When detectDeletes=true, records present in existing but missing from new data
   * should be closed with is_current=false, is_active=false
   */
  property("scd2_deleted_records_are_closed") = forAll(existingSCD2RecordsGen) { existingRecords =>
    Try {
      if (existingRecords.size < 2) {
        true // Skip too small test cases
      } else {
        // Create existing records with is_active column
        val existingWithActive = existingRecords.map(r =>
          SCD2RecordActive(r.id, r.name, r.value, r.valid_from, r.valid_to, r.is_current, true)
        )
        val existingDf = existingWithActive.toDF()

        // Remove some records from the new data (simulate deletes)
        val toKeep = existingRecords.take(existingRecords.size / 2)
        val toDelete = existingRecords.drop(existingRecords.size / 2)
        val newDf = toKeep.map(r => SimpleRecord(r.id, r.name, r.value)).toDF()

        val merger = new SCD2Merger(
          keyColumns = Seq("id"),
          compareColumns = Seq("name", "value"),
          validFromCol = "valid_from",
          validToCol = "valid_to",
          isCurrentCol = "is_current",
          detectDeletes = true,
          isActiveCol = Some("is_active")
        )

        val result = merger.merge(newDf, Some(existingDf))

        // Kept records should still be current and active
        val keptIds = toKeep.map(_.id).toSet
        val keptCorrect = keptIds.forall { id =>
          val current = result.filter($"id" === id && $"is_current" === true)
          current.count() == 1
        }

        // Deleted records should be closed and inactive
        val deletedIds = toDelete.map(_.id).toSet
        val deletedCorrect = deletedIds.forall { id =>
          val closed = result.filter($"id" === id && $"is_current" === false && $"is_active" === false)
          closed.count() >= 1
        }

        keptCorrect && deletedCorrect
      }
    }.getOrElse(false)
  }
}
