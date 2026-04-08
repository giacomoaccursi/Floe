package com.etl.framework.util

import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object RetryExecutor {

  private val logger = LoggerFactory.getLogger(getClass)

  def withRetry[T](
      maxRetries: Int,
      baseDelayMs: Long,
      operationName: String,
      isRetryable: Throwable => Boolean = _ => true
  )(fn: => T): T = {

    @tailrec
    def attempt(remaining: Int): T = {
      val attemptNum = maxRetries - remaining + 1
      Try(fn) match {
        case Success(result) =>
          if (attemptNum > 1) logger.info(s"$operationName succeeded on attempt $attemptNum")
          result
        case Failure(e) if remaining > 0 && isRetryable(e) =>
          val delay = computeDelay(attemptNum - 1, baseDelayMs)
          logger.warn(
            s"$operationName failed (attempt $attemptNum/${maxRetries + 1}): ${e.getMessage}. Retrying in ${delay}ms"
          )
          Thread.sleep(delay)
          attempt(remaining - 1)
        case Failure(e) =>
          throw e
      }
    }

    attempt(maxRetries)
  }

  private[util] def computeDelay(attempt: Int, baseDelayMs: Long): Long = {
    val exponential = baseDelayMs * (1L << attempt)
    val jitter = (Math.random() * baseDelayMs).toLong
    exponential + jitter
  }
}
