package com.etl.framework.orchestration.batch

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.UUID

object BatchIdGenerator {

  def generate(format: String): String = {
    val timestamp = Instant.now()
    val formattedTime = format match {
      case "timestamp" =>
        timestamp.toEpochMilli.toString
      case _ =>
        val pattern = if (format == "datetime") "yyyyMMdd_HHmmss" else format
        DateTimeFormatter
          .ofPattern(pattern)
          .withZone(java.time.ZoneId.systemDefault())
          .format(timestamp)
    }
    val entropy = UUID.randomUUID().toString.replace("-", "")
    s"${formattedTime}_$entropy"
  }
}
