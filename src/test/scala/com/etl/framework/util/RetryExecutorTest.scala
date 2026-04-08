package com.etl.framework.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RetryExecutorTest extends AnyFlatSpec with Matchers {

  "RetryExecutor" should "return result on first success" in {
    var calls = 0
    val result = RetryExecutor.withRetry(maxRetries = 3, baseDelayMs = 1, operationName = "test") {
      calls += 1
      "ok"
    }
    result shouldBe "ok"
    calls shouldBe 1
  }

  it should "retry on failure and succeed" in {
    var calls = 0
    val result = RetryExecutor.withRetry(maxRetries = 3, baseDelayMs = 1, operationName = "test") {
      calls += 1
      if (calls < 3) throw new RuntimeException("transient")
      "recovered"
    }
    result shouldBe "recovered"
    calls shouldBe 3
  }

  it should "throw after exhausting all retries" in {
    var calls = 0
    val ex = intercept[RuntimeException] {
      RetryExecutor.withRetry(maxRetries = 2, baseDelayMs = 1, operationName = "test") {
        calls += 1
        throw new RuntimeException("permanent")
      }
    }
    ex.getMessage shouldBe "permanent"
    calls shouldBe 3 // initial + 2 retries
  }

  it should "not retry when isRetryable returns false" in {
    var calls = 0
    val ex = intercept[java.io.IOException] {
      RetryExecutor.withRetry(
        maxRetries = 3,
        baseDelayMs = 1,
        operationName = "test",
        isRetryable = _.isInstanceOf[RuntimeException]
      ) {
        calls += 1
        throw new java.io.IOException("not retryable")
      }
    }
    ex.getMessage shouldBe "not retryable"
    calls shouldBe 1
  }

  it should "not retry when maxRetries is 0" in {
    var calls = 0
    val ex = intercept[RuntimeException] {
      RetryExecutor.withRetry(maxRetries = 0, baseDelayMs = 1, operationName = "test") {
        calls += 1
        throw new RuntimeException("fail")
      }
    }
    ex.getMessage shouldBe "fail"
    calls shouldBe 1
  }

  "computeDelay" should "increase exponentially" in {
    val d0 = RetryExecutor.computeDelay(0, 1000)
    val d1 = RetryExecutor.computeDelay(1, 1000)
    val d2 = RetryExecutor.computeDelay(2, 1000)

    // Base delay is 1000 * 2^attempt + jitter(0..1000)
    d0 should be >= 1000L
    d0 should be < 2000L
    d1 should be >= 2000L
    d1 should be < 3000L
    d2 should be >= 4000L
    d2 should be < 5000L
  }
}
