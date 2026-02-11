package com.etl.framework

import org.scalacheck.Test.Parameters

object TestConfig {

  private val expensiveTestCount = 10
  private val criticalTestCount = 30

  val expensiveParams: Parameters = Parameters.default
    .withMinSuccessfulTests(expensiveTestCount)
    .withMaxDiscardRatio(5.0f)

  val criticalParams: Parameters = Parameters.default
    .withMinSuccessfulTests(criticalTestCount)
    .withMaxDiscardRatio(5.0f)
}
