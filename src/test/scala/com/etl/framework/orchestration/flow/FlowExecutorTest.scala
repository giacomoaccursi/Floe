package com.etl.framework.orchestration.flow

import com.etl.framework.config._
import com.etl.framework.exceptions.InvariantViolationException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{DataFrame, SparkSession}

class FlowExecutorTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("FlowExecutorTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  def createGlobalConfig(): GlobalConfig = {
    GlobalConfig(
      paths = PathsConfig(
        "/tmp/flow_executor_test/output",
        "/tmp/flow_executor_test/rejected",
        "/tmp/flow_executor_test/metadata"
      ),
      processing = ProcessingConfig(
        "yyyyMMdd",
        failOnValidationError = false,
        maxRejectionRate = 0.1
      ),
      performance =
        PerformanceConfig(parallelFlows = false, parallelNodes = false)
    )
  }

  def createFlowConfig(
      name: String,
      sourceType: SourceType = SourceType.File,
      loadMode: LoadMode = LoadMode.Full
  ): FlowConfig = {
    FlowConfig(
      name = name,
      description = "Test flow",
      version = "1.0",
      owner = "test",
      source = SourceConfig(
        sourceType,
        "/tmp/test_source",
        FileFormat.CSV,
        Map.empty,
        None
      ),
      schema = SchemaConfig(
        enforceSchema = false,
        allowExtraColumns = true,
        Seq.empty
      ),
      loadMode = LoadModeConfig(loadMode),
      validation = ValidationConfig(Seq.empty, Seq.empty, Seq.empty),
      output =
        OutputConfig(path = Some(s"/tmp/flow_executor_test/output/$name"))
    )
  }

  // Test helper that overrides protected methods for unit testing
  class TestableFlowExecutor(
      flowConfig: FlowConfig,
      globalConfig: GlobalConfig,
      validatedFlows: Map[String, DataFrame] = Map.empty,
      domainsConfig: Option[DomainsConfig] = None
  ) extends FlowExecutor(
        flowConfig,
        globalConfig,
        validatedFlows,
        domainsConfig
      )

  "FlowExecutor" should "create FlowResult with correct flow name" in {
    val flowConfig = createFlowConfig("test_flow")
    flowConfig.name shouldBe "test_flow"
  }

  it should "use correct load mode from config" in {
    val flowConfig = createFlowConfig("test_flow", loadMode = LoadMode.Full)
    flowConfig.loadMode.`type` shouldBe LoadMode.Full

    val deltaConfig = createFlowConfig("delta_flow", loadMode = LoadMode.Delta)
    deltaConfig.loadMode.`type` shouldBe LoadMode.Delta
  }

  it should "configure output path correctly" in {
    val flowConfig = createFlowConfig("test_flow")

    flowConfig.output.path shouldBe defined
    flowConfig.output.path.get should include("test_flow")
  }

  it should "have validation config" in {
    val flowConfig = createFlowConfig("test_flow")

    flowConfig.validation should not be null
    flowConfig.validation.primaryKey shouldBe empty
    flowConfig.validation.foreignKeys shouldBe empty
    flowConfig.validation.rules shouldBe empty
  }

  it should "preserve flow config properties in executor" in {
    val flowConfig = createFlowConfig("test_flow", sourceType = SourceType.File)
    val globalConfig = createGlobalConfig()
    val executor = new TestableFlowExecutor(flowConfig, globalConfig)

    // Executor should be created successfully
    executor should not be null
  }

  it should "handle empty validated flows map" in {
    val flowConfig = createFlowConfig("test_flow")
    val globalConfig = createGlobalConfig()
    val executor =
      new TestableFlowExecutor(flowConfig, globalConfig, Map.empty, None)

    executor should not be null
  }

  it should "handle non-empty validated flows map" in {
    val flowConfig = createFlowConfig("test_flow")
    val globalConfig = createGlobalConfig()

    val validatedFlows = Map(
      "flow_a" -> Seq((1, "data")).toDF("id", "value")
    )

    val executor =
      new TestableFlowExecutor(flowConfig, globalConfig, validatedFlows, None)

    executor should not be null
  }

  it should "handle domains config" in {
    val flowConfig = createFlowConfig("test_flow")
    val globalConfig = createGlobalConfig()

    val domainsConfig = Some(DomainsConfig(Map.empty))
    val executor = new TestableFlowExecutor(
      flowConfig,
      globalConfig,
      Map.empty,
      domainsConfig
    )

    executor should not be null
  }

  "FlowResult" should "be created with success factory method" in {
    val result = FlowResult.success(
      flowName = "test_flow",
      batchId = "batch123",
      inputRecords = 100,
      mergedRecords = 100,
      validRecords = 95,
      rejectedRecords = 5,
      rejectionReasons = Map("not_null" -> 5L)
    )

    result.flowName shouldBe "test_flow"
    result.batchId shouldBe "batch123"
    result.success shouldBe true
    result.inputRecords shouldBe 100
    result.validRecords shouldBe 95
    result.rejectedRecords shouldBe 5
    result.rejectionRate shouldBe 0.05
    result.rejectionReasons should contain("not_null" -> 5L)
    result.error shouldBe None
  }

  it should "be created with failure factory method" in {
    val result = FlowResult.failure(
      flowName = "test_flow",
      batchId = "batch123",
      error = "Database connection failed"
    )

    result.flowName shouldBe "test_flow"
    result.batchId shouldBe "batch123"
    result.success shouldBe false
    result.error shouldBe defined
    result.error.get shouldBe "Database connection failed"
  }

  it should "calculate rejection rate correctly" in {
    val result = FlowResult.success(
      flowName = "test_flow",
      batchId = "batch123",
      inputRecords = 200,
      mergedRecords = 200,
      validRecords = 180,
      rejectedRecords = 20,
      rejectionReasons = Map.empty
    )

    result.rejectionRate shouldBe 0.1 // 20/200 = 0.1 (10%)
  }

  it should "handle zero input records" in {
    val result = FlowResult.success(
      flowName = "test_flow",
      batchId = "batch123",
      inputRecords = 0,
      mergedRecords = 0,
      validRecords = 0,
      rejectedRecords = 0,
      rejectionReasons = Map.empty
    )

    result.rejectionRate shouldBe 0.0
  }

  it should "preserve execution time when set" in {
    val result = FlowResult
      .success(
        flowName = "test_flow",
        batchId = "batch123",
        inputRecords = 100,
        mergedRecords = 100,
        validRecords = 100,
        rejectedRecords = 0,
        rejectionReasons = Map.empty
      )
      .copy(executionTimeMs = 5000L)

    result.executionTimeMs shouldBe 5000L
  }

  // Regression test: verifyInvariant was called with inputCount (pre-merge) instead of
  // mergedCount (post-merge) in the Parquet pipeline, causing InvariantViolationException
  // on any Delta/SCD2 load after the first execution (when mergedCount != inputCount).
  "Parquet Delta pipeline" should "not throw InvariantViolationException when mergedCount exceeds inputCount" in {
    val deltaFlowConfig = FlowConfig(
      name = "delta_invariant_test",
      description = "Delta merge invariant regression test",
      version = "1.0",
      owner = "test",
      source = SourceConfig(SourceType.File, "/tmp/test", FileFormat.CSV, Map.empty, None),
      schema = SchemaConfig(enforceSchema = false, allowExtraColumns = true, Seq.empty),
      loadMode = LoadModeConfig(LoadMode.Delta),
      validation = ValidationConfig(primaryKey = Seq("id"), foreignKeys = Seq.empty, rules = Seq.empty),
      output = OutputConfig(path = Some("/tmp/flow_executor_invariant_test/output"))
    )

    // 2 new records (id=6,7) + 5 existing (id=1..5, non-overlapping) → mergedCount=7, inputCount=2
    val newRecords      = Seq((6, "F"), (7, "G")).toDF("id", "value")
    val existingRecords = Seq((1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E")).toDF("id", "value")

    class InvariantTestExecutor extends FlowExecutor(deltaFlowConfig, createGlobalConfig()) {
      override protected def readData(): DataFrame          = newRecords
      override protected def loadExistingData(path: String) = Some(existingRecords)
    }

    val result = new InvariantTestExecutor().execute("batch_invariant_001")

    // With the old code: verifyInvariant(inputCount=2, valid=7, rejected=0) → 2 ≠ 7 → crash
    // With the fix:      verifyInvariant(mergedCount=7, valid=7, rejected=0) → 7 == 7 → ok
    result.success         shouldBe true
    result.inputRecords    shouldBe 2
    result.mergedRecords   shouldBe 7
    result.validRecords    shouldBe 7
    result.rejectedRecords shouldBe 0
  }

  "InvariantViolationException" should "be throwable with correct context" in {
    val exception = InvariantViolationException(
      flowName = "test_flow",
      inputCount = 100,
      validCount = 95,
      rejectedCount = 4
    )

    exception.errorCode should not be empty
    exception.getMessage should include("test_flow")
    exception.getMessage should include("100")
    exception.getMessage should include("95")
    exception.getMessage should include("4")
  }

  it should "provide context map with all counts" in {
    val exception = InvariantViolationException(
      flowName = "test_flow",
      inputCount = 100,
      validCount = 95,
      rejectedCount = 4
    )

    val context = exception.context
    context should contain key "flowName"
    context should contain key "inputCount"
    context should contain key "validCount"
    context should contain key "rejectedCount"
    context should contain key "difference"
  }
}
