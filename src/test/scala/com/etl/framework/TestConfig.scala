package com.etl.framework

import org.scalacheck.Test.Parameters

/**
 * Centralized configuration for property-based tests
 * 
 * Adjust minSuccessfulTests based on your needs:
 * - Development/CI: 20-25 tests
 * - Nightly/Release: 100+ tests
 */
object TestConfig {
  
  /**
   * Standard number of test cases for most properties
   */
  val standardTestCount = 20
  
  /**
   * Reduced test count for expensive tests (I/O, Spark operations)
   */
  val expensiveTestCount = 10
  
  /**
   * Increased test count for critical invariants
   * Use for tests that verify critical properties like record count invariants
   */
  val criticalTestCount = 30
  
  /**
   * Minimal test count for simple properties
   * Use for straightforward tests with limited edge cases
   */
  val minimalTestCount = 5
  
  /**
   * ScalaCheck parameters for standard tests
   */
  val standardParams: Parameters = Parameters.default
    .withMinSuccessfulTests(standardTestCount)
    .withMaxDiscardRatio(5.0f)
  
  /**
   * ScalaCheck parameters for expensive tests
   */
  val expensiveParams: Parameters = Parameters.default
    .withMinSuccessfulTests(expensiveTestCount)
    .withMaxDiscardRatio(5.0f)
  
  /**
   * ScalaCheck parameters for critical tests
   */
  val criticalParams: Parameters = Parameters.default
    .withMinSuccessfulTests(criticalTestCount)
    .withMaxDiscardRatio(5.0f)
  
  /**
   * ScalaCheck parameters for minimal tests
   */
  val minimalParams: Parameters = Parameters.default
    .withMinSuccessfulTests(minimalTestCount)
    .withMaxDiscardRatio(5.0f)
}
