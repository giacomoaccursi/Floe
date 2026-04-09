package com.etl.framework.orchestration.batch

import java.time.Instant
import java.time.format.DateTimeFormatter

object BatchIdGenerator {

  def generate(format: String): String = {
    val timestamp = Instant.now()
    format match {
      case "timestamp" =>
        timestamp.toEpochMilli.toString
      case _ =>
        val pattern = if (format == "datetime") "yyyyMMdd_HHmmss" else format
        DateTimeFormatter
          .ofPattern(pattern)
          .withZone(java.time.ZoneId.systemDefault())
          .format(timestamp)
    }
  }
}
