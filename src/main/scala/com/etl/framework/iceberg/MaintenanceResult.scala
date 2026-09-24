package com.etl.framework.iceberg

sealed abstract class MaintenanceStatus(val name: String, val terminal: Boolean)
object MaintenanceStatus {
  case object Queued extends MaintenanceStatus("QUEUED", terminal = false)
  case object Running extends MaintenanceStatus("RUNNING", terminal = false)
  case object Succeeded extends MaintenanceStatus("SUCCEEDED", terminal = true)
  case object Failed extends MaintenanceStatus("FAILED", terminal = true)

  val values: Seq[MaintenanceStatus] = Seq(Queued, Running, Succeeded, Failed)

  def fromName(name: String): MaintenanceStatus =
    values.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Unknown maintenance status: $name"))
}

/** Operational status of asynchronous maintenance for one Iceberg table. */
case class MaintenanceResult(
    targetName: String,
    targetType: String,
    status: MaintenanceStatus,
    error: Option[String] = None
) {
  def success: Boolean = status == MaintenanceStatus.Succeeded
}
