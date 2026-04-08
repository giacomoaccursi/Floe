package com.etl.framework.orchestration

import com.etl.framework.config._
import com.etl.framework.orchestration.batch.FlowGroupExecutor
import com.etl.framework.orchestration.flow.FlowResult
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FlowGroupExecutorTest extends AnyFlatSpec with Matchers {

  private def makeConfig(maxRejectionRate: Option[Double]): GlobalConfig = GlobalConfig(
    paths = PathsConfig("/out", "/rej", "/meta"),
    processing = ProcessingConfig(maxRejectionRate = maxRejectionRate),
    performance = PerformanceConfig(parallelFlows = false),
    iceberg = IcebergConfig(warehouse = "/tmp/test-warehouse")
  )

  private val defaultFlowConfig = FlowConfig(
    name = "test_flow",
    source = SourceConfig(SourceType.File, "/path", FileFormat.CSV, Map.empty),
    schema = SchemaConfig(enforceSchema = false, allowExtraColumns = true, Seq.empty),
    loadMode = LoadModeConfig(LoadMode.Full),
    validation = ValidationConfig(Seq.empty, Seq.empty, Seq.empty),
    output = OutputConfig()
  )

  private def makeResult(
      success: Boolean,
      inputRecords: Long,
      rejectedRecords: Long
  ): FlowResult = {
    val rejectionRate = if (inputRecords > 0) rejectedRecords.toDouble / inputRecords else 0.0
    FlowResult(
      flowName = "test_flow",
      batchId = "batch_001",
      success = success,
      inputRecords = inputRecords,
      mergedRecords = inputRecords,
      validRecords = inputRecords - rejectedRecords,
      rejectedRecords = rejectedRecords,
      rejectionRate = rejectionRate,
      executionTimeMs = 100,
      rejectionReasons = Map.empty
    )
  }

  private def executor(maxRejectionRate: Option[Double]): FlowGroupExecutor = {
    implicit val spark: org.apache.spark.sql.SparkSession =
      org.apache.spark.sql.SparkSession
        .builder()
        .appName("FlowGroupExecutorTest")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()

    new FlowGroupExecutor(makeConfig(maxRejectionRate), None, scala.concurrent.ExecutionContext.global)
  }

  "shouldStopExecution" should "return true when the flow itself failed" in {
    val result = makeResult(success = false, inputRecords = 100, rejectedRecords = 0)
    executor(None).shouldStopExecution(result, defaultFlowConfig) shouldBe true
  }

  it should "return false when no threshold is set regardless of rejections" in {
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 50)
    executor(None).shouldStopExecution(result, defaultFlowConfig) shouldBe false
  }

  it should "return true when rejection rate exceeds global threshold" in {
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 15)
    executor(Some(0.1)).shouldStopExecution(result, defaultFlowConfig) shouldBe true
  }

  it should "return false when rejection rate is within global threshold" in {
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 5)
    executor(Some(0.1)).shouldStopExecution(result, defaultFlowConfig) shouldBe false
  }

  it should "NOT stop when rejection rate equals threshold exactly (strict >)" in {
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 10)
    executor(Some(0.1)).shouldStopExecution(result, defaultFlowConfig) shouldBe false
  }

  it should "use per-flow threshold over global threshold" in {
    // Global: 10%, per-flow: 20%. 15% rejections → within per-flow threshold
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 15)
    val flowWithOverride = defaultFlowConfig.copy(maxRejectionRate = Some(0.2))
    executor(Some(0.1)).shouldStopExecution(result, flowWithOverride) shouldBe false
  }

  it should "stop when per-flow threshold is exceeded even if global is higher" in {
    // Global: 50%, per-flow: 5%. 10% rejections → exceeds per-flow threshold
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 10)
    val flowWithStrictThreshold = defaultFlowConfig.copy(maxRejectionRate = Some(0.05))
    executor(Some(0.5)).shouldStopExecution(result, flowWithStrictThreshold) shouldBe true
  }

  it should "return false when there are no rejections" in {
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 0)
    executor(Some(0.1)).shouldStopExecution(result, defaultFlowConfig) shouldBe false
  }
}
