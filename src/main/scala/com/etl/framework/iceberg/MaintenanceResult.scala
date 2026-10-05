package com.etl.framework.iceberg

sealed abstract class MaintenanceStatus(val name: String)
object MaintenanceStatus {
  case object Succeeded extends MaintenanceStatus("SUCCEEDED")
  case object Failed extends MaintenanceStatus("FAILED")
  case object Unknown extends MaintenanceStatus("UNKNOWN")

  val values: Seq[MaintenanceStatus] = Seq(Succeeded, Failed, Unknown)

  def fromName(name: String): MaintenanceStatus =
    values.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Unknown maintenance status: $name"))
}

sealed abstract class MaintenanceAction(val name: String)
object MaintenanceAction {
  case object ExpireSnapshots extends MaintenanceAction("EXPIRE_SNAPSHOTS")
  case object RewriteDataFiles extends MaintenanceAction("REWRITE_DATA_FILES")
  case object RemoveOrphanFiles extends MaintenanceAction("REMOVE_ORPHAN_FILES")
  case object RewriteManifests extends MaintenanceAction("REWRITE_MANIFESTS")
}

sealed abstract class MaintenanceActionStatus(val name: String)
object MaintenanceActionStatus {
  case object Succeeded extends MaintenanceActionStatus("SUCCEEDED")
  case object Unknown extends MaintenanceActionStatus("UNKNOWN")
  case object NotAttempted extends MaintenanceActionStatus("NOT_ATTEMPTED")
}

case class MaintenanceActionResult(
    action: MaintenanceAction,
    status: MaintenanceActionStatus,
    parameters: Map[String, String],
    metrics: Map[String, Long] = Map.empty,
    error: Option[String] = None
)

/** Synchronous, single-table maintenance evidence returned to the external scheduler. */
case class MaintenanceResult(
    contractVersion: Int,
    tableName: String,
    tableUuid: Option[String],
    startingSnapshotId: Option[Long],
    status: MaintenanceStatus,
    actions: Seq[MaintenanceActionResult],
    error: Option[String] = None
) {
  def success: Boolean = status == MaintenanceStatus.Succeeded
}

object MaintenanceResult {
  val CurrentContractVersion: Int = 1
}
