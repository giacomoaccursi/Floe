package com.etl.framework.util

import org.slf4j.Logger

/** Simple utility for timing operations
  */
object TimingUtil {

  /** Execute operation with automatic timing and logging
    */
  def timed[T](logger: Logger, operation: String)(block: => T): T = {
    val startTime = System.nanoTime()
    try {
      val result = block
      val durationMs = (System.nanoTime() - startTime) / 1000000
      logger.info(s"$operation completed in ${durationMs}ms")
      result
    } catch {
      case e: Throwable =>
        val durationMs = (System.nanoTime() - startTime) / 1000000
        logger.error(s"$operation failed after ${durationMs}ms: ${e.getMessage}", e)
        throw e
    }
  }

  /** Execute operation with automatic timing and logging, returning both result and duration
    *
    * @param logger
    *   Logger instance for logging
    * @param operation
    *   Description of the operation being timed
    * @param block
    *   Code block to execute and time
    * @return
    *   Tuple of (result, durationMs)
    */
  def timedWithDuration[T](logger: Logger, operation: String)(block: => T): (T, Long) = {
    val startTime = System.nanoTime()
    try {
      val result = block
      val durationMs = (System.nanoTime() - startTime) / 1000000
      logger.info(s"$operation completed in ${durationMs}ms")
      (result, durationMs)
    } catch {
      case e: Throwable =>
        val durationMs = (System.nanoTime() - startTime) / 1000000
        logger.error(s"$operation failed after ${durationMs}ms: ${e.getMessage}", e)
        throw e
    }
  }
}
