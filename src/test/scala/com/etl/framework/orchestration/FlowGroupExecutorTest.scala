package com.etl.framework.orchestration

import com.etl.framework.config._
import com.etl.framework.orchestration.batch.FlowGroupExecutor
import com.etl.framework.orchestration.flow.FlowResult
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Tests for FlowGroupExecutor.shouldStopExecution.
  *
  * Key behaviors:
  *   - Failed flow → always stop
  *   - maxRejectionRate = None → never stop for rejections
  *   - maxRejectionRate = Some(rate), rejectionRate > rate → stop
  *   - maxRejectionRate = Some(rate), rejectionRate <= rate → continue
  *   - Boundary: rejectionRate == rate uses strict >, so exactly-at-threshold does NOT stop
  */
class FlowGroupExecutorTest extends AnyFlatSpec with Matchers {

  private def makeConfig(maxRejectionRate: Option[Double]): GlobalConfig = GlobalConfig(
    paths = PathsConfig("/out", "/rej", "/meta"),
    processing = ProcessingConfig(maxRejectionRate = maxRejectionRate),
    performance = PerformanceConfig(parallelFlows = false, parallelNodes = false),
    iceberg = IcebergConfig(warehouse = "/tmp/test-warehouse")
  )

  private def makeResult(
      success: Boolean,
      inputRecords: Long,
      rejectedRecords: Long
  ): FlowResult = {
    val validRecords = inputRecords - rejectedRecords
    val rejectionRate = if (inputRecords > 0) rejectedRecords.toDouble / inputRecords else 0.0
    FlowResult(
      flowName = "test_flow",
      batchId = "batch_001",
      success = success,
      inputRecords = inputRecords,
      mergedRecords = inputRecords,
      validRecords = validRecords,
      rejectedRecords = rejectedRecords,
      rejectionRate = rejectionRate,
      executionTimeMs = 100,
      rejectionReasons = Map.empty
    )
  }

  private def executor(maxRejectionRate: Option[Double]): FlowGroupExecutor = {
    implicit val spark: org.apache.spark.sql.SparkSession =
      org.apache.spark.sql.SparkSession.builder()
        .appName("FlowGroupExecutorTest")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()

    new FlowGroupExecutor(
      makeConfig(maxRejectionRate),
      None,
      scala.concurrent.ExecutionContext.global
    )
  }

  "shouldStopExecution" should "return true when the flow itself failed" in {
    val result = makeResult(success = false, inputRecords = 100, rejectedRecords = 0)
    executor(None).shouldStopExecution(result) shouldBe true
  }

  it should "return false when maxRejectionRate is None regardless of rejections" in {
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 50)
    executor(None).shouldStopExecution(result) shouldBe false
  }

  it should "return true when rejection rate exceeds threshold" in {
    // 15 rejections out of 100 = 15% > 10%
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 15)
    executor(Some(0.1)).shouldStopExecution(result) shouldBe true
  }

  it should "return false when rejection rate is within threshold" in {
    // 5 rejections out of 100 = 5% < 10%
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 5)
    executor(Some(0.1)).shouldStopExecution(result) shouldBe false
  }

  it should "NOT stop when rejection rate equals threshold exactly (strict > comparison)" in {
    // exactly 10 out of 100 = 10.0%, threshold = 0.1 (10%) — uses >, not >=
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 10)
    executor(Some(0.1)).shouldStopExecution(result) shouldBe false
  }

  it should "return false when there are no rejections with threshold set" in {
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 0)
    executor(Some(0.1)).shouldStopExecution(result) shouldBe false
  }

  it should "return false when there are no rejections and no threshold" in {
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 0)
    executor(None).shouldStopExecution(result) shouldBe false
  }
}
