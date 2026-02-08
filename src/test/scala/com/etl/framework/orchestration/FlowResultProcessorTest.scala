package com.etl.framework.orchestration

import com.etl.framework.config._
import com.etl.framework.orchestration.batch.FlowGroupExecutor
import com.etl.framework.orchestration.flow.FlowResult
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{SparkSession, DataFrame}
import scala.collection.mutable

class FlowResultProcessorTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("FlowResultProcessorTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  // Mock FlowGroupExecutor for testing
  class MockFlowGroupExecutor(
      stopOnHighRejectionRate: Boolean = false,
      rejectionThreshold: Double = 0.1
  ) extends FlowGroupExecutor(
        GlobalConfig(
          paths = PathsConfig("/full", "/delta", "/input", "/rejected", "/metadata"),
          processing = ProcessingConfig(
            "yyyyMMdd",
            failOnValidationError = false,
            maxRejectionRate = rejectionThreshold
          ),
          performance =
            PerformanceConfig(parallelFlows = true, parallelNodes = true)
        ),
        None
      ) {
    override def shouldStopExecution(result: FlowResult): Boolean = {
      if (!result.success) return true
      if (stopOnHighRejectionRate && result.rejectionRate > rejectionThreshold)
        return true
      false
    }
  }

  // Mock FlowResultProcessor that doesn't load data from disk
  class MockFlowResultProcessor(
      globalConfig: GlobalConfig,
      flowConfigs: Seq[FlowConfig],
      groupExecutor: FlowGroupExecutor
  ) extends FlowResultProcessor(globalConfig, flowConfigs, groupExecutor) {
    override def loadValidatedData(
        result: FlowResult,
        validatedFlows: scala.collection.mutable.Map[String, DataFrame]
    ): Unit = {
      // Mock: don't actually load from disk, just create a dummy DataFrame
      validatedFlows(result.flowName) = Seq((1, "dummy")).toDF("id", "value")
    }
  }

  def createGlobalConfig(
      rejectionThreshold: Double = 0.1
  ): GlobalConfig = {
    GlobalConfig(
      paths = PathsConfig("/tmp/full", "/tmp/delta", "/tmp/input", "/tmp/rejected", "/tmp/metadata"),
      processing = ProcessingConfig(
        "yyyyMMdd",
        failOnValidationError = false,
        maxRejectionRate = rejectionThreshold
      ),
      performance =
        PerformanceConfig(parallelFlows = true, parallelNodes = true)
    )
  }

  def createFlowConfig(
      name: String,
      outputPath: Option[String] = None
  ): FlowConfig = {
    FlowConfig(
      name = name,
      description = "Test flow",
      version = "1.0",
      owner = "test",
      source =
        SourceConfig(SourceType.File, "/path", FileFormat.CSV, Map.empty, None),
      schema = SchemaConfig(true, true, Seq.empty),
      loadMode = LoadModeConfig(LoadMode.Full),
      validation = ValidationConfig(Seq.empty, Seq.empty, Seq.empty),
      output = OutputConfig(path = outputPath)
    )
  }

  "FlowResultProcessor" should "continue processing for successful flow result" in {
    val globalConfig = createGlobalConfig()
    val flowConfigs = Seq(createFlowConfig("flow_a"))
    val groupExecutor =
      new MockFlowGroupExecutor(stopOnHighRejectionRate = false)
    val processor =
      new MockFlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.{Continue, StopExecution}

    val result =
      FlowResult.success("flow_a", "batch1", 100, 100, 95, 5, Map.empty)
    val accumulatedResults = mutable.ArrayBuffer[FlowResult]()
    val validatedFlows = mutable.Map[String, DataFrame]()

    val processingResult = processor.processGroupResults(
      Seq(result),
      accumulatedResults,
      validatedFlows,
      "batch1"
    )

    processingResult shouldBe Continue
    accumulatedResults should have size 1
  }

  it should "stop execution for failed flow result" in {
    val globalConfig = createGlobalConfig()
    val flowConfigs = Seq(createFlowConfig("flow_a"))
    val groupExecutor = new MockFlowGroupExecutor()
    val processor =
      new MockFlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.{Continue, StopExecution}

    val result =
      FlowResult.failure("flow_a", "batch1", "Database connection failed")
    val accumulatedResults = mutable.ArrayBuffer[FlowResult]()
    val validatedFlows = mutable.Map[String, DataFrame]()

    val processingResult = processor.processGroupResults(
      Seq(result),
      accumulatedResults,
      validatedFlows,
      "batch1"
    )

    processingResult match {
      case StopExecution(ingestionResult) =>
        ingestionResult.success shouldBe false
        ingestionResult.error shouldBe defined
        ingestionResult.error.get should include("flow_a")
        ingestionResult.error.get should include("failed")
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
      new MockFlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.{Continue, StopExecution}

    // Rejection rate = 30/100 = 0.3 (30%), exceeds threshold of 10%
    val result =
      FlowResult.success("flow_a", "batch1", 100, 100, 70, 30, Map.empty)
    val accumulatedResults = mutable.ArrayBuffer[FlowResult]()
    val validatedFlows = mutable.Map[String, DataFrame]()

    val processingResult = processor.processGroupResults(
      Seq(result),
      accumulatedResults,
      validatedFlows,
      "batch1"
    )

    processingResult match {
      case StopExecution(ingestionResult) =>
        ingestionResult.success shouldBe false
        ingestionResult.error shouldBe defined
        ingestionResult.error.get should include("rejection threshold")
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
      new MockFlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.{Continue, StopExecution}

    val results = Seq(
      FlowResult.success("flow_a", "batch1", 100, 100, 95, 5, Map.empty),
      FlowResult.success("flow_b", "batch1", 200, 200, 190, 10, Map.empty),
      FlowResult.success("flow_c", "batch1", 150, 150, 145, 5, Map.empty)
    )
    val accumulatedResults = mutable.ArrayBuffer[FlowResult]()
    val validatedFlows = mutable.Map[String, DataFrame]()

    val processingResult = processor.processGroupResults(
      results,
      accumulatedResults,
      validatedFlows,
      "batch1"
    )

    processingResult shouldBe Continue
    accumulatedResults should have size 3
    accumulatedResults.map(_.flowName) should contain allOf (
      "flow_a",
      "flow_b",
      "flow_c"
    )
  }

  it should "stop at first failure in mixed results" in {
    val globalConfig = createGlobalConfig()
    val flowConfigs = Seq(
      createFlowConfig("flow_a"),
      createFlowConfig("flow_b"),
      createFlowConfig("flow_c")
    )
    val groupExecutor = new MockFlowGroupExecutor()
    val processor =
      new MockFlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.{Continue, StopExecution}

    val results = Seq(
      FlowResult.success("flow_a", "batch1", 100, 100, 95, 5, Map.empty),
      FlowResult.failure("flow_b", "batch1", "Processing error"),
      FlowResult.success("flow_c", "batch1", 150, 150, 145, 5, Map.empty)
    )
    val accumulatedResults = mutable.ArrayBuffer[FlowResult]()
    val validatedFlows = mutable.Map[String, DataFrame]()

    val processingResult = processor.processGroupResults(
      results,
      accumulatedResults,
      validatedFlows,
      "batch1"
    )

    processingResult match {
      case StopExecution(ingestionResult) =>
        ingestionResult.success shouldBe false
        ingestionResult.flowResults should have size 2 // flow_a and flow_b
        ingestionResult.error.get should include("flow_b")
      case _ => fail("Expected StopExecution")
    }
  }

  it should "handle empty group results" in {
    val globalConfig = createGlobalConfig()
    val flowConfigs = Seq.empty
    val groupExecutor = new MockFlowGroupExecutor()
    val processor =
      new MockFlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.{Continue, StopExecution}

    val accumulatedResults = mutable.ArrayBuffer[FlowResult]()
    val validatedFlows = mutable.Map[String, DataFrame]()

    val processingResult = processor.processGroupResults(
      Seq.empty,
      accumulatedResults,
      validatedFlows,
      "batch1"
    )

    processingResult shouldBe Continue
    accumulatedResults shouldBe empty
  }

  it should "create IngestionResult with correct batch ID and flow results" in {
    val globalConfig = createGlobalConfig()
    val flowConfigs = Seq(createFlowConfig("flow_a"))
    val groupExecutor = new MockFlowGroupExecutor()
    val processor =
      new MockFlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.{Continue, StopExecution}

    val result = FlowResult.failure("flow_a", "batch123", "Error")
    val accumulatedResults = mutable.ArrayBuffer[FlowResult]()
    val validatedFlows = mutable.Map[String, DataFrame]()

    val processingResult = processor.processGroupResults(
      Seq(result),
      accumulatedResults,
      validatedFlows,
      "batch123"
    )

    processingResult match {
      case StopExecution(ingestionResult) =>
        ingestionResult.batchId shouldBe "batch123"
        ingestionResult.flowResults should have size 1
        ingestionResult.flowResults.head.flowName shouldBe "flow_a"
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
      new MockFlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.{Continue, StopExecution}

    // Rejection rate = 10/100 = 0.1 (10%), below threshold of 20%
    val result =
      FlowResult.success("flow_a", "batch1", 100, 100, 90, 10, Map.empty)
    val accumulatedResults = mutable.ArrayBuffer[FlowResult]()
    val validatedFlows = mutable.Map[String, DataFrame]()

    val processingResult = processor.processGroupResults(
      Seq(result),
      accumulatedResults,
      validatedFlows,
      "batch1"
    )

    processingResult shouldBe Continue
    accumulatedResults should have size 1
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
      new MockFlowResultProcessor(globalConfig, flowConfigs, groupExecutor)
    import processor.{Continue, StopExecution}

    val rejectionReasons = Map(
      "schema_validation" -> 3L,
      "not_null_validation" -> 2L
    )
    val result =
      FlowResult.success("flow_a", "batch1", 100, 100, 95, 5, rejectionReasons)
    val accumulatedResults = mutable.ArrayBuffer[FlowResult]()
    val validatedFlows = mutable.Map[String, DataFrame]()

    val processingResult = processor.processGroupResults(
      Seq(result),
      accumulatedResults,
      validatedFlows,
      "batch1"
    )

    processingResult shouldBe Continue
    accumulatedResults.head.rejectionReasons shouldBe rejectionReasons
  }
}
