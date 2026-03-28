package com.etl.framework.orchestration

import com.etl.framework.config._
import com.etl.framework.orchestration.batch.FlowGroupExecutor
import com.etl.framework.orchestration.flow.FlowResult
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Tests for FlowGroupExecutor.shouldStopExecution semantics.
  *
  * Key documented behaviors:
  *   - failOnValidationError=false, rejectionRate > maxRejectionRate → do NOT stop
  *   - failOnValidationError=true, rejectionRate > maxRejectionRate → stop
  *   - failOnValidationError=true, rejectionRate <= maxRejectionRate but rejectedRecords > 0 → stop (any rejection
  *     fails)
  *   - Boundary: rejectionRate == maxRejectionRate uses strict >, so exactly-at-threshold does NOT stop (only >
  *     threshold stops)
  */
class FlowGroupExecutorTest extends AnyFlatSpec with Matchers {

  private def makeConfig(
      failOnValidationError: Boolean,
      maxRejectionRate: Double
  ): GlobalConfig = GlobalConfig(
    paths = PathsConfig("/out", "/rej", "/meta"),
    processing = ProcessingConfig(
      batchIdFormat = "yyyyMMdd",
      failOnValidationError = failOnValidationError,
      maxRejectionRate = maxRejectionRate
    ),
    performance = PerformanceConfig(parallelFlows = false, parallelNodes = false),
    iceberg = IcebergConfig(warehouse = "/tmp/test-warehouse")
  )

  private def makeResult(
      success: Boolean,
      inputRecords: Long,
      rejectedRecords: Long
  ): FlowResult = {
    val validRecords = inputRecords - rejectedRecords
    val rejectionRate = if (inputRecords == 0) 0.0 else rejectedRecords.toDouble / inputRecords
    FlowResult(
      flowName = "test_flow",
      batchId = "20260317",
      success = success,
      inputRecords = inputRecords,
      mergedRecords = inputRecords,
      validRecords = validRecords,
      rejectedRecords = rejectedRecords,
      rejectionRate = rejectionRate,
      executionTimeMs = 100L,
      rejectionReasons = Map.empty,
      error = None,
      icebergMetadata = None
    )
  }

  // Helper — we only test shouldStopExecution; no SparkSession needed
  private def executor(
      failOnValidationError: Boolean,
      maxRejectionRate: Double
  ): FlowGroupExecutor = {
    implicit val spark: org.apache.spark.sql.SparkSession =
      org.apache.spark.sql.SparkSession
        .builder()
        .appName("FlowGroupExecutorTest")
        .master("local")
        .config("spark.ui.enabled", "false")
        .getOrCreate()

    new FlowGroupExecutor(
      makeConfig(failOnValidationError, maxRejectionRate),
      None,
      scala.concurrent.ExecutionContext.global
    )
  }

  "shouldStopExecution" should "return true when the flow itself failed" in {
    val result = makeResult(success = false, inputRecords = 100, rejectedRecords = 0)
    executor(failOnValidationError = false, maxRejectionRate = 0.1)
      .shouldStopExecution(result) shouldBe true
  }

  it should "return false when rejection rate is within threshold and failOnValidationError=false" in {
    // 5 rejections out of 100 = 5%, threshold 10% → within limits
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 5)
    executor(failOnValidationError = false, maxRejectionRate = 0.1)
      .shouldStopExecution(result) shouldBe false
  }

  it should "return false when rejection rate exceeds threshold but failOnValidationError=false" in {
    // 15 rejections out of 100 = 15%, threshold 10%, but failOnValidationError=false → no stop
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 15)
    executor(failOnValidationError = false, maxRejectionRate = 0.1)
      .shouldStopExecution(result) shouldBe false
  }

  it should "return true when rejection rate exceeds threshold and failOnValidationError=true" in {
    // 15 rejections out of 100 = 15% > 10% → stop
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 15)
    executor(failOnValidationError = true, maxRejectionRate = 0.1)
      .shouldStopExecution(result) shouldBe true
  }

  it should "NOT stop when rejection rate equals threshold exactly (strict > comparison)" in {
    // exactly 10 out of 100 = 10.0%, threshold = 0.1 (10%) — uses >, not >=
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 10)
    executor(failOnValidationError = false, maxRejectionRate = 0.1)
      .shouldStopExecution(result) shouldBe false
  }

  it should "stop on any rejection when failOnValidationError=true even if rate is within threshold" in {
    // 5 rejections out of 100 = 5%, threshold 10% → rate OK, but failOnValidationError=true
    // Any rejected record with failOnValidationError=true stops execution
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 5)
    executor(failOnValidationError = true, maxRejectionRate = 0.1)
      .shouldStopExecution(result) shouldBe true
  }

  it should "return false when there are no rejections regardless of settings" in {
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 0)
    executor(failOnValidationError = true, maxRejectionRate = 0.1)
      .shouldStopExecution(result) shouldBe false
  }

  it should "return false when maxRejectionRate is 0 and no rejections" in {
    val result = makeResult(success = true, inputRecords = 100, rejectedRecords = 0)
    executor(failOnValidationError = false, maxRejectionRate = 0.0)
      .shouldStopExecution(result) shouldBe false
  }
}
