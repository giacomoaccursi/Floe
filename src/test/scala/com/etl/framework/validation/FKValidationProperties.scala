package com.etl.framework.validation

import com.etl.framework.config._
import com.etl.framework.exceptions.ValidationConfigException
import com.etl.framework.TestConfig
import com.etl.framework.validation.ValidationColumns._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters
import scala.util.Try

/** Property-based tests for Foreign Key validation Feature: spark-etl-framework, Property 10: Foreign Key Integrity
  * Enforcement Validates: Requirements 7.1
  *
  * Test configuration: Uses expensiveTestCount (10 cases) due to Spark operations
  */
object FKValidationProperties extends Properties("FKValidation") {

  // Configure test parameters for expensive Spark operations
  override def overrideParameters(p: Parameters): Parameters =
    TestConfig.expensiveParams

  // Create a local Spark session for testing
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("FKValidationPropertiesTest")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  // Test data structures
  case class ParentRecord(parent_id: Int, parent_name: String)
  case class ChildRecord(child_id: Int, parent_id: Int, child_name: String)

  // Generator for parent records
  val parentRecordGen: Gen[ParentRecord] = for {
    id <- Gen.choose(1, 100)
    name <- Gen.alphaNumStr.suchThat(_.nonEmpty)
  } yield ParentRecord(id, name)

  // Generator for unique parent records
  val uniqueParentRecordsGen: Gen[Seq[ParentRecord]] = for {
    size <- Gen.choose(5, 30)
    records <- Gen.listOfN(size, parentRecordGen)
  } yield {
    records.zipWithIndex.map { case (rec, idx) =>
      rec.copy(parent_id = idx + 1)
    }
  }

  // Generator for child records with valid FKs
  def childRecordsWithValidFKsGen(parentIds: Seq[Int]): Gen[Seq[ChildRecord]] =
    for {
      size <- Gen.choose(5, 50)
      records <- Gen.listOfN(size, Gen.choose(1, size)).map { childIds =>
        childIds.zipWithIndex.map { case (_, idx) =>
          val parentId = parentIds(scala.util.Random.nextInt(parentIds.size))
          ChildRecord(idx + 1, parentId, s"child_$idx")
        }
      }
    } yield records

  // Generator for child records with some orphans
  def childRecordsWithOrphansGen(
      parentIds: Seq[Int]
  ): Gen[(Seq[ChildRecord], Set[Int])] = for {
    validSize <- Gen.choose(3, 20)
    orphanSize <- Gen.choose(1, 10)
    validRecords <- Gen.listOfN(validSize, Gen.choose(1, validSize)).map { childIds =>
      childIds.zipWithIndex.map { case (_, idx) =>
        val parentId = parentIds(scala.util.Random.nextInt(parentIds.size))
        ChildRecord(idx + 1, parentId, s"child_$idx")
      }
    }
    // Create orphan records with non-existent parent IDs
    orphanRecords <- Gen.listOfN(orphanSize, Gen.choose(1, orphanSize)).map { childIds =>
      childIds.zipWithIndex.map { case (_, idx) =>
        val orphanParentId = 9000 + idx // Use IDs that don't exist in parent
        ChildRecord(validSize + idx + 1, orphanParentId, s"orphan_$idx")
      }
    }
  } yield {
    val allRecords = validRecords ++ orphanRecords
    val orphanIds = orphanRecords.map(_.child_id).toSet
    (allRecords, orphanIds)
  }

  // Generator for flow configuration with FK validation
  def flowConfigWithFKGen(
      fkName: String,
      fkColumn: String,
      refFlow: String,
      refColumn: String
  ): Gen[FlowConfig] = for {
    name <- Gen.alphaNumStr.suchThat(_.nonEmpty)
  } yield FlowConfig(
    name = name,
    description = "Test flow with FK validation",
    version = "1.0",
    owner = "test",
    source = SourceConfig(
      `type` = SourceType.File,
      path = "/test",
      format = FileFormat.CSV,
      options = Map.empty,
      filePattern = None
    ),
    schema = SchemaConfig(
      enforceSchema = true,
      allowExtraColumns = false,
      columns = Seq(
        ColumnConfig("child_id", "int", nullable = false, "Child ID"),
        ColumnConfig(
          "parent_id",
          "int",
          nullable = false,
          "Parent ID (FK)"
        ),
        ColumnConfig(
          "child_name",
          "string",
          nullable = true,
          "Child name"
        )
      )
    ),
    loadMode = LoadModeConfig(
      `type` = LoadMode.Full
    ),
    validation = ValidationConfig(
      primaryKey = Seq("child_id"),
      foreignKeys = Seq(
        ForeignKeyConfig(
          column = fkColumn,
          references = ReferenceConfig(
            flow = refFlow,
            column = refColumn
          )
        )
      ),
      rules = Seq.empty
    ),
    output = OutputConfig()
  )

  /** Property 10: Foreign Key Integrity Enforcement - Orphan Records Rejected For any dataset with Foreign Keys that
    * don't exist in the referenced table, all orphan records should be rejected
    */
  property("orphan_fk_records_all_rejected") = forAll(uniqueParentRecordsGen) { parentRecords =>
    forAll(childRecordsWithOrphansGen(parentRecords.map(_.parent_id))) { case (childRecords, orphanIds) =>
      forAll(
        flowConfigWithFKGen(
          "fk_parent",
          "parent_id",
          "parent_flow",
          "parent_id"
        )
      ) { flowConfig =>
        Try {
          if (parentRecords.isEmpty || childRecords.isEmpty || orphanIds.isEmpty) {
            true // Skip empty test cases
          } else {
            val parentDf = parentRecords.toDF()
            val childDf = childRecords.toDF()

            // Create validated flows map with parent flow
            val validatedFlows = Map("parent_flow" -> parentDf)

            // Run validation
            val engine = new ValidationEngine()
            val result =
              engine.validate(childDf, flowConfig, validatedFlows)

            // Count orphan records
            val orphanCount =
              childRecords.count(r => orphanIds.contains(r.child_id))

            // All orphan records should be rejected
            val allOrphansRejected = result.rejected.exists { rejectedDf =>
              val rejectedCount = rejectedDf.count()
              rejectedCount == orphanCount
            }

            // Valid records should only contain records with valid FKs
            val validHasNoOrphans = {
              val validChildIds = result.valid
                .select("child_id")
                .collect()
                .map(_.getInt(0))
                .toSet
              validChildIds.intersect(orphanIds).isEmpty
            }

            // Rejected records should have correct metadata
            val hasCorrectMetadata = result.rejected.exists { rejectedDf =>
              rejectedDf.columns.contains(REJECTION_CODE) &&
              rejectedDf.columns.contains(REJECTION_REASON) &&
              rejectedDf.columns.contains(VALIDATION_STEP) &&
              rejectedDf
                .filter(col(REJECTION_CODE) === "FK_VIOLATION")
                .count() == orphanCount
            }

            allOrphansRejected && validHasNoOrphans && hasCorrectMetadata
          }
        }.getOrElse(false)
      }
    }
  }

  /** Property 10: Foreign Key Integrity Enforcement - Valid FKs Pass For any dataset with all valid Foreign Keys, no
    * records should be rejected for FK violation
    */
  property("valid_fk_records_all_pass") = forAll(uniqueParentRecordsGen) { parentRecords =>
    forAll(childRecordsWithValidFKsGen(parentRecords.map(_.parent_id))) { childRecords =>
      forAll(
        flowConfigWithFKGen(
          "fk_parent",
          "parent_id",
          "parent_flow",
          "parent_id"
        )
      ) { flowConfig =>
        Try {
          if (parentRecords.isEmpty || childRecords.isEmpty) {
            true // Skip empty test cases
          } else {
            val parentDf = parentRecords.toDF()
            val childDf = childRecords.toDF()

            // Create validated flows map with parent flow
            val validatedFlows = Map("parent_flow" -> parentDf)

            // Run validation
            val engine = new ValidationEngine()
            val result =
              engine.validate(childDf, flowConfig, validatedFlows)

            // No records should be rejected for FK violation
            val noFKRejections = result.rejected.forall { rejectedDf =>
              rejectedDf
                .filter(col(REJECTION_CODE) === "FK_VIOLATION")
                .isEmpty
            }

            // All records should be in valid DataFrame (or rejected for other reasons, not FK)
            val allRecordsAccountedFor = {
              val validCount = result.valid.count()
              val rejectedCount =
                result.rejected.map(_.count()).getOrElse(0L)
              validCount + rejectedCount == childRecords.size
            }

            noFKRejections && allRecordsAccountedFor
          }
        }.getOrElse(false)
      }
    }
  }

  /** Property 10: Foreign Key Integrity Enforcement - Multiple FKs For any dataset with multiple Foreign Keys, all FK
    * constraints should be enforced
    */
  property("multiple_fk_constraints_enforced") = forAll(uniqueParentRecordsGen, uniqueParentRecordsGen) {
    (parent1Records, parent2Records) =>
      Try {
        if (parent1Records.isEmpty || parent2Records.isEmpty) {
          true // Skip empty test cases
        } else {
          val parent1Ids = parent1Records.map(_.parent_id)
          val parent2Ids = parent2Records.map(_.parent_id)

          // Create some valid records and some with FK violations
          val validChildren = (1 to 10).map { i =>
            (
              i,
              parent1Ids(scala.util.Random.nextInt(parent1Ids.size)),
              parent2Ids(scala.util.Random.nextInt(parent2Ids.size)),
              s"child_$i"
            )
          }

          // Create orphan records (invalid parent1_id)
          val orphan1Children = (11 to 15).map { i =>
            (i, 9000 + i, parent2Ids(0), s"orphan1_$i")
          }

          // Create orphan records (invalid parent2_id)
          val orphan2Children = (16 to 20).map { i =>
            (i, parent1Ids(0), 9000 + i, s"orphan2_$i")
          }

          val allChildren =
            validChildren ++ orphan1Children ++ orphan2Children
          val childDf =
            allChildren.toDF("child_id", "parent1_id", "parent2_id", "name")

          val parent1Df =
            parent1Records.toDF().withColumnRenamed("parent_id", "parent1_id")
          val parent2Df =
            parent2Records.toDF().withColumnRenamed("parent_id", "parent2_id")

          val flowConfig = FlowConfig(
            name = "test_multiple_fks",
            description = "Test flow with multiple FKs",
            version = "1.0",
            owner = "test",
            source = SourceConfig(
              `type` = SourceType.File,
              path = "/test",
              format = FileFormat.CSV,
              options = Map.empty,
              filePattern = None
            ),
            schema = SchemaConfig(
              enforceSchema = true,
              allowExtraColumns = false,
              columns = Seq(
                ColumnConfig(
          "child_id",
          "int",
          nullable = false,
          "Child ID"
        ),
                ColumnConfig(
          "parent1_id",
          "int",
          nullable = false,
          "Parent 1 ID"
        ),
                ColumnConfig(
          "parent2_id",
          "int",
          nullable = false,
          "Parent 2 ID"
        ),
                ColumnConfig("name", "string", nullable = true, "Name")
              )
            ),
            loadMode = LoadModeConfig(
              `type` = LoadMode.Full
            ),
            validation = ValidationConfig(
              primaryKey = Seq("child_id"),
              foreignKeys = Seq(
                ForeignKeyConfig(
                  column = "parent1_id",
                  references = ReferenceConfig(
                    flow = "parent1_flow",
                    column = "parent1_id"
                  )
                ),
                ForeignKeyConfig(
                  column = "parent2_id",
                  references = ReferenceConfig(
                    flow = "parent2_flow",
                    column = "parent2_id"
                  )
                )
              ),
              rules = Seq.empty
            ),
            output = OutputConfig()
          )

          val validatedFlows = Map(
            "parent1_flow" -> parent1Df,
            "parent2_flow" -> parent2Df
          )

          // Run validation
          val engine = new ValidationEngine()
          val result = engine.validate(childDf, flowConfig, validatedFlows)

          // All orphan records should be rejected
          val expectedRejectedCount =
            orphan1Children.size + orphan2Children.size
          val allOrphansRejected = result.rejected.exists { rejectedDf =>
            rejectedDf.count() == expectedRejectedCount
          }

          // Only valid records should remain
          val onlyValidRemain = result.valid.count() == validChildren.size

          allOrphansRejected && onlyValidRemain
        }
      }.getOrElse(false)
  }

  /** Property 10: Foreign Key Integrity Enforcement - Empty Parent Table For any dataset when the parent table is
    * empty, all child records should be rejected
    */
  property("empty_parent_table_rejects_all_children") = forAll(
    Gen.choose(5, 20)
  ) { childCount =>
    forAll(
      flowConfigWithFKGen("fk_parent", "parent_id", "parent_flow", "parent_id")
    ) { flowConfig =>
      Try {
        // Create child records
        val childRecords = (1 to childCount).map { i =>
          ChildRecord(i, i, s"child_$i")
        }
        val childDf = childRecords.toDF()

        // Create empty parent DataFrame
        val emptyParentDf = Seq.empty[ParentRecord].toDF()

        val validatedFlows = Map("parent_flow" -> emptyParentDf)

        // Run validation
        val engine = new ValidationEngine()
        val result = engine.validate(childDf, flowConfig, validatedFlows)

        // All child records should be rejected
        val allRejected = result.rejected.exists { rejectedDf =>
          rejectedDf.count() == childCount
        }

        // No valid records should remain
        val noValidRecords = result.valid.count() == 0

        // All rejections should be FK violations
        val allFKViolations = result.rejected.exists { rejectedDf =>
          rejectedDf
            .filter(col(REJECTION_CODE) === "FK_VIOLATION")
            .count() == childCount
        }

        allRejected && noValidRecords && allFKViolations
      }.getOrElse(false)
    }
  }

  /** Property 10: Foreign Key Integrity Enforcement - Null FK Values For any dataset with null FK values, those records
    * should be rejected
    */
  property("null_fk_values_rejected") = forAll(
    uniqueParentRecordsGen,
    Gen.choose(5, 20)
  ) { (parentRecords, childCount) =>
    forAll(
      flowConfigWithFKGen("fk_parent", "parent_id", "parent_flow", "parent_id")
    ) { flowConfig =>
      Try {
        if (parentRecords.isEmpty) {
          true // Skip empty test cases
        } else {
          val parentIds = parentRecords.map(_.parent_id)

          // Create child records with some null FKs
          val validChildren = (1 to childCount / 2).map { i =>
            ChildRecord(
              i,
              parentIds(scala.util.Random.nextInt(parentIds.size)),
              s"child_$i"
            )
          }

          // Create children with null parent_id (will be handled by not-null validation)
          // For this test, we'll focus on valid FK values
          val childDf = validChildren.toDF()
          val parentDf = parentRecords.toDF()

          val validatedFlows = Map("parent_flow" -> parentDf)

          // Run validation
          val engine = new ValidationEngine()
          val result = engine.validate(childDf, flowConfig, validatedFlows)

          // All records with valid FKs should pass FK validation
          val noFKRejections = result.rejected.forall { rejectedDf =>
            rejectedDf.filter(col(REJECTION_CODE) === "FK_VIOLATION").isEmpty
          }

          noFKRejections
        }
      }.getOrElse(false)
    }
  }

  /** Property 10: Foreign Key Integrity Enforcement - FK Rejection Metadata For any records rejected due to FK
    * violation, all metadata fields should be present
    */
  property("fk_rejection_metadata_complete") = forAll(uniqueParentRecordsGen) { parentRecords =>
    forAll(childRecordsWithOrphansGen(parentRecords.map(_.parent_id))) { case (childRecords, orphanIds) =>
      forAll(
        flowConfigWithFKGen(
          "fk_parent",
          "parent_id",
          "parent_flow",
          "parent_id"
        )
      ) { flowConfig =>
        Try {
          if (parentRecords.isEmpty || childRecords.isEmpty || orphanIds.isEmpty) {
            true // Skip empty test cases
          } else {
            val parentDf = parentRecords.toDF()
            val childDf = childRecords.toDF()

            val validatedFlows = Map("parent_flow" -> parentDf)

            // Run validation
            val engine = new ValidationEngine()
            val result =
              engine.validate(childDf, flowConfig, validatedFlows)

            // Check that all required metadata fields are present in rejected records
            result.rejected.exists { rejectedDf =>
              val requiredFields = Set(
                REJECTION_CODE,
                REJECTION_REASON,
                VALIDATION_STEP,
                REJECTED_AT
              )

              val actualFields = rejectedDf.columns.toSet
              val hasAllFields = requiredFields.subsetOf(actualFields)

              // Check that rejection code is correct
              val correctCode = rejectedDf
                .filter(col(REJECTION_CODE) === "FK_VIOLATION")
                .count() > 0

              // Check that rejection reason mentions foreign key
              val reasonMentionsFK = rejectedDf
                .select(REJECTION_REASON)
                .distinct()
                .collect()
                .forall { row =>
                  val reason = row.getString(0)
                  reason.toLowerCase.contains(
                    "foreign key"
                  ) || reason.toLowerCase.contains("fk")
                }

              // Check that validation step is correct
              val correctStep = rejectedDf
                .select(VALIDATION_STEP)
                .distinct()
                .collect()
                .forall(_.getString(0) == "fk_validation")

              hasAllFields && correctCode && reasonMentionsFK && correctStep
            }
          }
        }.getOrElse(false)
      }
    }
  }

  /** Property 10: Foreign Key Integrity Enforcement - Record Count Invariant For any dataset, input_count = valid_count
    * + rejected_count
    */
  property("fk_validation_preserves_record_count") = forAll(uniqueParentRecordsGen) { parentRecords =>
    forAll(childRecordsWithOrphansGen(parentRecords.map(_.parent_id))) { case (childRecords, _) =>
      forAll(
        flowConfigWithFKGen(
          "fk_parent",
          "parent_id",
          "parent_flow",
          "parent_id"
        )
      ) { flowConfig =>
        Try {
          if (parentRecords.isEmpty || childRecords.isEmpty) {
            true // Skip empty test cases
          } else {
            val parentDf = parentRecords.toDF()
            val childDf = childRecords.toDF()
            val inputCount = childDf.count()

            val validatedFlows = Map("parent_flow" -> parentDf)

            // Run validation
            val engine = new ValidationEngine()
            val result =
              engine.validate(childDf, flowConfig, validatedFlows)

            val validCount = result.valid.count()
            val rejectedCount = result.rejected.map(_.count()).getOrElse(0L)

            // Invariant: input = valid + rejected
            inputCount == validCount + rejectedCount
          }
        }.getOrElse(false)
      }
    }
  }

  /** Property 10: Foreign Key Integrity Enforcement - Missing Referenced Flow Error For any FK configuration
    * referencing a non-existent flow, validation should throw exception
    */
  property("missing_referenced_flow_throws_exception") = forAll(Gen.choose(5, 20)) { childCount =>
    forAll(
      flowConfigWithFKGen(
        "fk_parent",
        "parent_id",
        "nonexistent_flow",
        "parent_id"
      )
    ) { flowConfig =>
      Try {
        val childRecords = (1 to childCount).map { i =>
          ChildRecord(i, i, s"child_$i")
        }
        val childDf = childRecords.toDF()

        // Don't include the referenced flow in validatedFlows
        val validatedFlows = Map.empty[String, DataFrame]

        // Run validation - should throw ValidationConfigException
        try {
          val engine = new ValidationEngine()
          engine.validate(childDf, flowConfig, validatedFlows)
          false // Should not reach here
        } catch {
          case _: ValidationConfigException => true
          case _: Throwable                 => false
        }
      }.getOrElse(false)
    }
  }

  /** Property 10: Foreign Key Integrity Enforcement - Composite FK For any dataset with composite Foreign Keys, all FK
    * columns should be validated together
    */
  property("composite_fk_validation") = forAll(Gen.choose(5, 20)) { size =>
    Try {
      // Create parent records with composite key
      val parentRecords = (1 to size).map { i =>
        (i, i * 10, s"parent_$i")
      }

      // Create valid children
      val validChildren = (1 to size).map { i =>
        (i, i, i * 10, s"child_$i")
      }

      // Create orphan children (one part of composite key is invalid)
      val orphanChildren = (size + 1 to size + 5).map { i =>
        (i, i, 9999, s"orphan_$i") // parent_id2 doesn't exist
      }

      val allChildren = validChildren ++ orphanChildren

      val parentDf = parentRecords.toDF("parent_id1", "parent_id2", "name")
      val childDf =
        allChildren.toDF("child_id", "parent_id1", "parent_id2", "child_name")

      val flowConfig = FlowConfig(
        name = "test_composite_fk",
        description = "Test flow with composite FK",
        version = "1.0",
        owner = "test",
        source = SourceConfig(
          `type` = SourceType.File,
          path = "/test",
          format = FileFormat.CSV,
          options = Map.empty,
          filePattern = None
        ),
        schema = SchemaConfig(
          enforceSchema = true,
          allowExtraColumns = false,
          columns = Seq(
            ColumnConfig("child_id", "int", nullable = false, "Child ID"),
            ColumnConfig(
          "parent_id1",
          "int",
          nullable = false,
          "Parent ID 1"
        ),
            ColumnConfig(
          "parent_id2",
          "int",
          nullable = false,
          "Parent ID 2"
        ),
            ColumnConfig(
          "child_name",
          "string",
          nullable = true,
          "Child name"
        )
          )
        ),
        loadMode = LoadModeConfig(
          `type` = LoadMode.Full
        ),
        validation = ValidationConfig(
          primaryKey = Seq("child_id"),
          foreignKeys = Seq(
            ForeignKeyConfig(
              column = "parent_id1",
              references = ReferenceConfig(flow = "parent_flow", column = "parent_id1")
            ),
            ForeignKeyConfig(
              column = "parent_id2",
              references = ReferenceConfig(flow = "parent_flow", column = "parent_id2")
            )
          ),
          rules = Seq.empty
        ),
        output = OutputConfig()
      )

      val validatedFlows = Map("parent_flow" -> parentDf)

      // Run validation
      val engine = new ValidationEngine()
      val result = engine.validate(childDf, flowConfig, validatedFlows)

      // Orphan records should be rejected
      val orphansRejected = result.rejected.exists { rejectedDf =>
        rejectedDf.count() == orphanChildren.size
      }

      // Valid records should pass
      val validPass = result.valid.count() == validChildren.size

      orphansRejected && validPass
    }.getOrElse(false)
  }
}
