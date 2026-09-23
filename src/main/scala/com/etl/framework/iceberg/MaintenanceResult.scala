package com.etl.framework.iceberg

/** Operational status of post-write maintenance for one Iceberg table.
  *
  * Maintenance is independent from data-write success, so callers can alert and retry it without replaying ingestion.
  */
case class MaintenanceResult(
    targetName: String,
    targetType: String,
    success: Boolean,
    error: Option[String] = None
)
