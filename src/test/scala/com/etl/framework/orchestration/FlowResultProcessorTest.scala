package com.etl.framework.orchestration

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import com.etl.framework.orchestration.batch.FlowGroupExecutor
import com.etl.framework.orchestration.flow.FlowResult
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class FlowResultProcessorTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("FlowResultProcessorTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  // Mock FlowGroupExecutor for testing
  class MockFlowGroupExecutor(
      stopOnHighRejectionRate: Boolean = false,
      rejectionThreshold: Double = 0.1
  ) extends FlowGroupExecutor(
        GlobalConfig(
          paths = PathsConfig("/output", "/rejected", "/metadata"),
          processing = ProcessingConfig(
            maxRejectionRate = if (stopOnHighRejectionRate) Some(rejectionThreshold) else None
          ),
          performance = PerformanceConfig(parallelFlows = true),
          iceberg = IcebergConfig(warehouse = "/tmp/test-warehouse")
        ),
        None,
        scala.concurrent.ExecutionContext.global
      ) {
    override def shouldStopExecution(result: FlowResult, flowConfig: FlowConfig): Boolean = {
      if (!result.success) return true
      if (stopOnHighRejectionRate && result.rejectionRate > rejectionThreshold)
        return true
      false
    }
  }

  def createGlobalConfig(
      rejectionThreshold: Double = 0.1
  ): GlobalConfig =
    TestFixtures.globalConfig(
      outputPath = "/tmp/output",
      rejectedPath = "/tmp/rejected",
      metadataPath = "/tmp/metadata",
      parallelFlows = true,
      maxRejectionRate = Some(rejectionThreshold),
      iceberg = IcebergConfig(warehouse = "/tmp/test-warehouse")
    )

  def createFlowConfig(
      name: String,
      outputPath: Option[String] = None
  ): FlowConfig =
    TestFixtures.flowConfig(
      name = name,
      primaryKey = Seq.empty,
      enforceSchema = true
    )

  private val frozenSchemaJson = StructType(Seq(StructField("id", StringType, nullable = false))).json

  private def successfulResult(
      flowName: String,
      inputRecords: Long,
      mergedRecords: Long,
      validRecords: Long,
      rejectedRecords: Long,
      rejectionReasons: Map[String, Long] = Map.empty
  ): FlowResult =
    FlowResult.success(
      flowName,
      "batch1",
      inputRecords,
      mergedRecords,
      validRecords,
      rejectedRecords,
      rejectionReasons,
      resultingSchemaJson = Some(frozenSchemaJson)
    )

  "FlowResultProcessor" should "continue processing for successful flow result" in {
    val globalConfig = createGlobalConfig()
    val flowConfigs = Seq(createFlowConfig("flow_a"))
    val groupExecutor =
      new MockFlowGroupExecutor(stopOnHighRejectionRate = false)
    val processor =
      new FlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.ContinueWith

    val result = successfulResult("flow_a", 100, 100, 95, 5)
    val state = BatchState(Seq.empty, Map.empty)

    val processingResult = processor.processGroupResults(
      Seq(result),
      state,
      "batch1"
    )

    processingResult match {
      case ContinueWith(newState) =>
        newState.flowResults should have size 1
      case _ => fail("Expected ContinueWith")
    }
  }

  it should "materialize a snapshotless result from its frozen schema without reading table HEAD" in {
    val flowConfigs = Seq(createFlowConfig("never_created"))
    val processor = new FlowResultProcessor(createGlobalConfig(), flowConfigs, new MockFlowGroupExecutor())
    import processor.ContinueWith
    val schema = StructType(Seq(StructField("id", StringType, nullable = false)))
    val result = FlowResult
      .success("never_created", "batch1", 0, 0, 0, 0, Map.empty, resultingSchemaJson = Some(schema.json))

    processor.processGroupResults(Seq(result), BatchState(Seq.empty, Map.empty), "batch1") match {
      case ContinueWith(state) =>
        state.validatedFlows("never_created").schema shouldBe schema
        state.validatedFlows("never_created").count() shouldBe 0L
      case _ => fail("Expected ContinueWith")
    }
  }

  it should "stop when a successful flow's attempt-pinned state cannot be loaded" in {
    val flowName = "unreadable_snapshot"
    val flowConfigs = Seq(createFlowConfig(flowName))
    val processor = new FlowResultProcessor(createGlobalConfig(), flowConfigs, new MockFlowGroupExecutor())
    import processor.StopExecution
    val schema = StructType(Seq(StructField("id", StringType, nullable = false)))
    val result = FlowResult.success(
      flowName,
      "batch1",
      1,
      1,
      1,
      0,
      Map.empty,
      resultingSnapshotId = Some(Long.MaxValue),
      resultingSchemaJson = Some(schema.json)
    )

    processor.processGroupResults(Seq(result), BatchState(Seq.empty, Map.empty), "batch1") match {
      case StopExecution(state, error) =>
        state.flowResults should contain only result
        state.validatedFlows shouldBe empty
        error should include("attempt-pinned state")
        error should include(flowName)
      case _ => fail("Expected StopExecution")
    }
  }

  it should "stop execution for failed flow result" in {
    val globalConfig = createGlobalConfig()
    val flowConfigs = Seq(createFlowConfig("flow_a"))
    val groupExecutor = new MockFlowGroupExecutor()
    val processor =
      new FlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.StopExecution

    val result =
      FlowResult.failure("flow_a", "batch1", "Database connection failed")
    val state = BatchState(Seq.empty, Map.empty)

    val processingResult = processor.processGroupResults(
      Seq(result),
      state,
      "batch1"
    )

    processingResult match {
      case StopExecution(stoppedState, error) =>
        stoppedState.flowResults should have size 1
        error should include("flow_a")
        error should include("failed")
      case _ => fail("Expected StopExecution")
    }
  }

  it should "retain every completed result from a parallel group after one flow fails" in {
    val globalConfig = createGlobalConfig()
    val flowConfigs = Seq(createFlowConfig("flow_a"), createFlowConfig("flow_b"))
    val processor = new FlowResultProcessor(globalConfig, flowConfigs, new MockFlowGroupExecutor())
    import processor.StopExecution

    val processingResult = processor.processGroupResults(
      Seq(
        FlowResult.failure("flow_a", "batch1", "failed first"),
        successfulResult("flow_b", 1, 1, 1, 0)
      ),
      BatchState(Seq.empty, Map.empty),
      "batch1"
    )

    processingResult match {
      case StopExecution(stoppedState, _) =>
        stoppedState.flowResults.map(_.flowName) shouldBe Seq("flow_a", "flow_b")
      case _ => fail("Expected StopExecution")
    }
  }

  it should "stop execution for high rejection rate" in {
    val globalConfig = createGlobalConfig(rejectionThreshold = 0.1)
    val flowConfigs = Seq(createFlowConfig("flow_a"))
    val groupExecutor = new MockFlowGroupExecutor(
      stopOnHighRejectionRate = true,
      rejectionThreshold = 0.1
    )
    val processor =
      new FlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.StopExecution

    // Rejection rate = 30/100 = 0.3 (30%), exceeds threshold of 10%
    val result = successfulResult("flow_a", 100, 100, 70, 30)
    val state = BatchState(Seq.empty, Map.empty)

    val processingResult = processor.processGroupResults(
      Seq(result),
      state,
      "batch1"
    )

    processingResult match {
      case StopExecution(_, error) =>
        error should include("rejection threshold")
      case _ => fail("Expected StopExecution")
    }
  }

  it should "accumulate multiple successful results" in {
    val globalConfig = createGlobalConfig()
    val flowConfigs = Seq(
      createFlowConfig("flow_a"),
      createFlowConfig("flow_b"),
      createFlowConfig("flow_c")
    )
    val groupExecutor =
      new MockFlowGroupExecutor(stopOnHighRejectionRate = false)
    val processor =
      new FlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.ContinueWith

    val results = Seq(
      successfulResult("flow_a", 100, 100, 95, 5),
      successfulResult("flow_b", 200, 200, 190, 10),
      successfulResult("flow_c", 150, 150, 145, 5)
    )
    val state = BatchState(Seq.empty, Map.empty)

    val processingResult = processor.processGroupResults(
      results,
      state,
      "batch1"
    )

    processingResult match {
      case ContinueWith(newState) =>
        newState.flowResults should have size 3
        newState.flowResults.map(_.flowName) should contain allOf (
          "flow_a",
          "flow_b",
          "flow_c"
        )
      case _ => fail("Expected ContinueWith")
    }
  }

  it should "stop after a mixed group while retaining every supplied result" in {
    val globalConfig = createGlobalConfig()
    val flowConfigs = Seq(
      createFlowConfig("flow_a"),
      createFlowConfig("flow_b"),
      createFlowConfig("flow_c")
    )
    val groupExecutor = new MockFlowGroupExecutor()
    val processor =
      new FlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.StopExecution

    val results = Seq(
      successfulResult("flow_a", 100, 100, 95, 5),
      FlowResult.failure("flow_b", "batch1", "Processing error"),
      successfulResult("flow_c", 150, 150, 145, 5)
    )
    val state = BatchState(Seq.empty, Map.empty)

    val processingResult = processor.processGroupResults(
      results,
      state,
      "batch1"
    )

    processingResult match {
      case StopExecution(stoppedState, error) =>
        stoppedState.flowResults.map(_.flowName) shouldBe Seq("flow_a", "flow_b", "flow_c")
        error should include("flow_b")
      case _ => fail("Expected StopExecution")
    }
  }

  it should "handle empty group results" in {
    val globalConfig = createGlobalConfig()
    val flowConfigs = Seq.empty
    val groupExecutor = new MockFlowGroupExecutor()
    val processor =
      new FlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.ContinueWith

    val state = BatchState(Seq.empty, Map.empty)

    val processingResult = processor.processGroupResults(
      Seq.empty,
      state,
      "batch1"
    )

    processingResult match {
      case ContinueWith(newState) =>
        newState.flowResults shouldBe empty
      case _ => fail("Expected ContinueWith")
    }
  }

  it should "retain the failed flow result and error" in {
    val globalConfig = createGlobalConfig()
    val flowConfigs = Seq(createFlowConfig("flow_a"))
    val groupExecutor = new MockFlowGroupExecutor()
    val processor =
      new FlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.StopExecution

    val result = FlowResult.failure("flow_a", "batch123", "Error")
    val state = BatchState(Seq.empty, Map.empty)

    val processingResult = processor.processGroupResults(
      Seq(result),
      state,
      "batch123"
    )

    processingResult match {
      case StopExecution(stoppedState, error) =>
        stoppedState.flowResults should have size 1
        stoppedState.flowResults.head.batchId shouldBe "batch123"
        stoppedState.flowResults.head.flowName shouldBe "flow_a"
        error should include("Error")
      case _ => fail("Expected StopExecution")
    }
  }

  it should "continue when rejection rate is below threshold" in {
    val globalConfig = createGlobalConfig(rejectionThreshold = 0.2)
    val flowConfigs = Seq(createFlowConfig("flow_a"))
    val groupExecutor = new MockFlowGroupExecutor(
      stopOnHighRejectionRate = true,
      rejectionThreshold = 0.2
    )
    val processor =
      new FlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.ContinueWith

    // Rejection rate = 10/100 = 0.1 (10%), below threshold of 20%
    val result = successfulResult("flow_a", 100, 100, 90, 10)
    val state = BatchState(Seq.empty, Map.empty)

    val processingResult = processor.processGroupResults(
      Seq(result),
      state,
      "batch1"
    )

    processingResult match {
      case ContinueWith(newState) =>
        newState.flowResults should have size 1
      case _ => fail("Expected ContinueWith")
    }
  }

  it should "use factory method from companion object" in {
    val globalConfig = createGlobalConfig()
    val flowConfigs = Seq(createFlowConfig("flow_a"))
    val groupExecutor = new MockFlowGroupExecutor()

    val processor =
      FlowResultProcessor(globalConfig, flowConfigs, groupExecutor)

    processor should not be null
  }

  it should "handle results with rejection reasons" in {
    val globalConfig = createGlobalConfig()
    val flowConfigs = Seq(createFlowConfig("flow_a"))
    val groupExecutor = new MockFlowGroupExecutor()
    val processor =
      new FlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.ContinueWith

    val rejectionReasons = Map(
      "schema_validation" -> 3L,
      "not_null_validation" -> 2L
    )
    val result = successfulResult("flow_a", 100, 100, 95, 5, rejectionReasons)
    val state = BatchState(Seq.empty, Map.empty)

    val processingResult = processor.processGroupResults(
      Seq(result),
      state,
      "batch1"
    )

    processingResult match {
      case ContinueWith(newState) =>
        newState.flowResults.head.rejectionReasons shouldBe rejectionReasons
      case _ => fail("Expected ContinueWith")
    }
  }
}
