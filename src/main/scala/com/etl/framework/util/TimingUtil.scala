package com.etl.framework.util

import org.slf4j.Logger

import scala.util.control.NonFatal

/** Simple utility for timing operations
  */
/** Utility for measuring and logging execution time of code blocks. */
object TimingUtil {

  /** Execute operation with automatic timing and logging
    */
  def timed[T](logger: Logger, operation: String)(block: => T): T = {
    val startTime = System.nanoTime()
    try {
      val result = block
      val durationMs = (System.nanoTime() - startTime) / 1000000
      logger.info(s"${SensitiveDataRedactor.redact(operation)} completed in ${durationMs}ms")
      result
    } catch {
      case NonFatal(e) =>
        val durationMs = (System.nanoTime() - startTime) / 1000000
        logger.error(
          s"${SensitiveDataRedactor.redact(operation)} failed after ${durationMs}ms: ${SensitiveDataRedactor.message(e)}",
          SensitiveDataRedactor.safeThrowable(e)
        )
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
      logger.info(s"${SensitiveDataRedactor.redact(operation)} completed in ${durationMs}ms")
      (result, durationMs)
    } catch {
      case NonFatal(e) =>
        val durationMs = (System.nanoTime() - startTime) / 1000000
        logger.error(
          s"${SensitiveDataRedactor.redact(operation)} failed after ${durationMs}ms: ${SensitiveDataRedactor.message(e)}",
          SensitiveDataRedactor.safeThrowable(e)
        )
        throw e
    }
  }
}
