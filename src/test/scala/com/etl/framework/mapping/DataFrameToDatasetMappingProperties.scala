package com.etl.framework.mapping

import com.etl.framework.exceptions.{DataFrameToDatasetMappingException, MappingExpressionException}
import com.etl.framework.TestConfig
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * Property-based tests for DataFrame to Dataset mapping
 * Feature: spark-etl-framework, Property 24: DataFrame to Dataset Mapping
 * Validates: Requirements 16.1
 * 
 * Test configuration: Uses standardTestCount (20 cases)
 */
object DataFrameToDatasetMappingProperties extends Properties("DataFrameToDatasetMapping") {
  
  // Configure test parameters
  override def overrideParameters(p: Parameters): Parameters = TestConfig.standardParams
  
  // Create a minimal Spark session for testing
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("DataFrameToDatasetMappingPropertiesTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  
  import spark.implicits._
  
  // Test case classes for mapping
  case class SimpleBatchModel(
    id: String,
    name: String,
    value: Double
  )
  
  case class ComplexBatchModel(
    customerId: String,
    fullName: String,
    totalAmount: Double,
    recordCount: Long
  )
  
  case class RenamedBatchModel(
    customerId: String,
    fullName: String,
    totalAmount: Double,
    recordCount: Long
  )
  
  case class RenamedSimpleModel(
    customerId: String,
    fullName: String,
    totalAmount: Double
  )
  
  // Generator for simple DataFrame with columns that match SimpleBatchModel
  val simpleDataFrameGen: Gen[DataFrame] = for {
    rowCount <- Gen.choose(1, 20)
    ids <- Gen.listOfN(rowCount, Gen.alphaNumStr.suchThat(_.nonEmpty))
    names <- Gen.listOfN(rowCount, Gen.alphaNumStr.suchThat(_.nonEmpty))
    values <- Gen.listOfN(rowCount, Gen.choose(0.0, 1000.0))
  } yield {
    val data = ids.zip(names).zip(values).map { case ((id, name), value) =>
      (id, name, value)
    }
    data.toDF("id", "name", "value")
  }
  
  // Generator for DataFrame with columns that need mapping to ComplexBatchModel
  val complexDataFrameGen: Gen[DataFrame] = for {
    rowCount <- Gen.choose(1, 20)
    customerIds <- Gen.listOfN(rowCount, Gen.alphaNumStr.suchThat(_.nonEmpty))
    firstNames <- Gen.listOfN(rowCount, Gen.alphaNumStr.suchThat(_.nonEmpty))
    lastNames <- Gen.listOfN(rowCount, Gen.alphaNumStr.suchThat(_.nonEmpty))
    amounts <- Gen.listOfN(rowCount, Gen.choose(0.0, 10000.0))
    counts <- Gen.listOfN(rowCount, Gen.choose(1L, 100L))
  } yield {
    val data = customerIds.zip(firstNames).zip(lastNames).zip(amounts).zip(counts).map {
      case ((((customerId, firstName), lastName), amount), count) =>
        (customerId, firstName, lastName, amount, count)
    }
    data.toDF("customer_id", "first_name", "last_name", "amount", "count")
  }
  
  // Generator for simple mapping configuration (no expressions)
  val simpleMappingConfigGen: Gen[MappingConfig] = Gen.const(
    MappingConfig(
      mappings = Seq(
        FieldMapping("id", "id", None),
        FieldMapping("name", "name", None),
        FieldMapping("value", "value", None)
      )
    )
  )
  
  // Generator for complex mapping configuration (with expressions)
  val complexMappingConfigGen: Gen[MappingConfig] = Gen.const(
    MappingConfig(
      mappings = Seq(
        FieldMapping("customer_id", "customerId", None),
        FieldMapping("first_name", "fullName", Some("concat(first_name, ' ', last_name)")),
        FieldMapping("amount", "totalAmount", None),
        FieldMapping("count", "recordCount", None)
      )
    )
  )
  
  /**
   * Property 24: DataFrame to Dataset Mapping - Simple Case
   * For any DataFrame with columns matching the target case class,
   * the mapper should successfully convert it to a typed Dataset.
   */
  property("dataframe_to_dataset_mapping_simple") = forAll(simpleDataFrameGen, simpleMappingConfigGen) {
    (df, mappingConfig) =>
      try {
        val mapper = new BatchModelMapper[SimpleBatchModel](mappingConfig)
        val dataset = mapper.map(df)
        
        // Verify the dataset has the correct type
        val isCorrectType = dataset.isInstanceOf[org.apache.spark.sql.Dataset[SimpleBatchModel]]
        
        // Verify the record count is preserved
        val countPreserved = dataset.count() == df.count()
        
        // Verify we can access typed fields
        val canAccessFields = try {
          dataset.map(_.id).collect()
          dataset.map(_.name).collect()
          dataset.map(_.value).collect()
          true
        } catch {
          case _: Exception => false
        }
        
        isCorrectType && countPreserved && canAccessFields
        
      } catch {
        case e: Exception =>
          println(s"Mapping failed: ${e.getMessage}")
          false
      }
  }
  
  /**
   * Property 24: DataFrame to Dataset Mapping - With Expressions
   * For any DataFrame with columns that need transformation,
   * the mapper should apply expressions and convert to typed Dataset.
   */
  property("dataframe_to_dataset_mapping_with_expressions") = forAll(complexDataFrameGen, complexMappingConfigGen) {
    (df, mappingConfig) =>
      try {
        val mapper = new BatchModelMapper[ComplexBatchModel](mappingConfig)
        val dataset = mapper.map(df)
        
        // Verify the dataset has the correct type
        val isCorrectType = dataset.isInstanceOf[org.apache.spark.sql.Dataset[ComplexBatchModel]]
        
        // Verify the record count is preserved
        val countPreserved = dataset.count() == df.count()
        
        // Verify expressions were applied correctly
        val expressionsApplied = try {
          val records = dataset.collect()
          // Check that fullName is a concatenation of first_name and last_name
          val originalData = df.select("first_name", "last_name").collect()
          records.zip(originalData).forall { case (record, original) =>
            val expectedFullName = s"${original.getString(0)} ${original.getString(1)}"
            record.fullName == expectedFullName
          }
        } catch {
          case _: Exception => false
        }
        
        isCorrectType && countPreserved && expressionsApplied
        
      } catch {
        case e: Exception =>
          println(s"Mapping with expressions failed: ${e.getMessage}")
          false
      }
  }
  
  /**
   * Property 24: DataFrame to Dataset Mapping - Column Renaming
   * For any DataFrame with columns that need renaming,
   * the mapper should rename columns correctly.
   */
  property("dataframe_to_dataset_mapping_column_renaming") = forAll(simpleDataFrameGen) {
    df =>
      try {
        // Create mapping that renames columns
        val mappingConfig = MappingConfig(
          mappings = Seq(
            FieldMapping("id", "customerId", None),
            FieldMapping("name", "fullName", None),
            FieldMapping("value", "totalAmount", None)
          )
        )
        
        val mapper = new BatchModelMapper[RenamedSimpleModel](mappingConfig)
        val dataset = mapper.map(df)
        
        // Verify the record count is preserved
        val countPreserved = dataset.count() == df.count()
        
        // Verify columns were renamed
        val columnsRenamed = dataset.columns.toSet == Set("customerId", "fullName", "totalAmount")
        
        // Verify data is preserved with new column names
        val dataPreserved = try {
          val originalData = df.collect().map { row =>
            (row.getString(0), row.getString(1), row.getDouble(2))
          }.toSet
          
          val renamedData = dataset.collect().map { model =>
            (model.customerId, model.fullName, model.totalAmount)
          }.toSet
          
          originalData == renamedData
        } catch {
          case _: Exception => false
        }
        
        countPreserved && columnsRenamed && dataPreserved
        
      } catch {
        case e: Exception =>
          println(s"Column renaming failed: ${e.getMessage}")
          false
      }
  }
  
  /**
   * Property 24: DataFrame to Dataset Mapping - Empty DataFrame
   * For any empty DataFrame with correct schema,
   * the mapper should produce an empty Dataset.
   */
  property("dataframe_to_dataset_mapping_empty_dataframe") = forAll(simpleMappingConfigGen) {
    mappingConfig =>
      try {
        // Create empty DataFrame with correct schema
        val emptyDf = Seq.empty[(String, String, Double)].toDF("id", "name", "value")
        
        val mapper = new BatchModelMapper[SimpleBatchModel](mappingConfig)
        val dataset = mapper.map(emptyDf)
        
        // Verify the dataset is empty
        dataset.count() == 0
        
      } catch {
        case e: Exception =>
          println(s"Empty DataFrame mapping failed: ${e.getMessage}")
          false
      }
  }
  
  /**
   * Property 24: DataFrame to Dataset Mapping - Schema Mismatch Detection
   * For any DataFrame with schema that doesn't match the target case class,
   * the mapper should throw an exception.
   */
  property("dataframe_to_dataset_mapping_schema_mismatch") = {
    // Create DataFrame with wrong schema
    val wrongSchemaDf = Seq(
      ("id1", "name1"),  // Missing 'value' column
      ("id2", "name2")
    ).toDF("id", "name")
    
    val mappingConfig = MappingConfig(
      mappings = Seq(
        FieldMapping("id", "id", None),
        FieldMapping("name", "name", None),
        FieldMapping("value", "value", None)  // This column doesn't exist
      )
    )
    
    try {
      val mapper = new BatchModelMapper[SimpleBatchModel](mappingConfig)
      mapper.map(wrongSchemaDf)
      // Should have thrown an exception
      false
    } catch {
      case _: DataFrameToDatasetMappingException =>
        // Expected
        true
      case _: org.apache.spark.sql.AnalysisException =>
        // Also acceptable - Spark throws this for missing columns
        true
      case e: Exception =>
        println(s"Unexpected exception: ${e.getClass.getName}: ${e.getMessage}")
        false
    }
  }
  
  /**
   * Property 24: DataFrame to Dataset Mapping - Invalid Expression Detection
   * For any mapping with invalid SQL expression,
   * the mapper should throw an exception.
   */
  property("dataframe_to_dataset_mapping_invalid_expression") = forAll(simpleDataFrameGen) {
    df =>
      val invalidMappingConfig = MappingConfig(
        mappings = Seq(
          FieldMapping("id", "id", None),
          FieldMapping("name", "name", None),
          FieldMapping("value", "value", Some("invalid_function(value)"))  // Invalid function
        )
      )
      
      try {
        val mapper = new BatchModelMapper[SimpleBatchModel](invalidMappingConfig)
        mapper.map(df)
        // Should have thrown an exception
        false
      } catch {
        case _: MappingExpressionException =>
          // Expected
          true
        case _: org.apache.spark.sql.AnalysisException =>
          // Also acceptable - Spark throws this for invalid expressions
          true
        case e: Exception =>
          println(s"Unexpected exception: ${e.getClass.getName}: ${e.getMessage}")
          false
      }
  }
  
  /**
   * Property 24: DataFrame to Dataset Mapping - Data Preservation
   * For any DataFrame mapped to Dataset,
   * all data values should be preserved correctly.
   */
  property("dataframe_to_dataset_mapping_data_preservation") = forAll(simpleDataFrameGen, simpleMappingConfigGen) {
    (df, mappingConfig) =>
      try {
        val mapper = new BatchModelMapper[SimpleBatchModel](mappingConfig)
        val dataset = mapper.map(df)
        
        // Collect original data
        val originalData = df.collect().map { row =>
          (row.getString(0), row.getString(1), row.getDouble(2))
        }.toSet
        
        // Collect mapped data
        val mappedData = dataset.collect().map { model =>
          (model.id, model.name, model.value)
        }.toSet
        
        // Verify all data is preserved
        originalData == mappedData
        
      } catch {
        case e: Exception =>
          println(s"Data preservation check failed: ${e.getMessage}")
          false
      }
  }
}
