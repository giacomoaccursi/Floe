package com.etl.framework.util

import com.etl.framework.iceberg.IcebergFlowMetadata

object IcebergMetadataSerializer {

  def toMap(meta: IcebergFlowMetadata): Map[String, Any] = Map(
    "table_name" -> meta.tableName,
    "snapshot_id" -> meta.snapshotId,
    "snapshot_tag" -> meta.snapshotTag.getOrElse(""),
    "parent_snapshot_id" -> meta.parentSnapshotId.getOrElse(""),
    "snapshot_timestamp_ms" -> meta.snapshotTimestampMs,
    "records_written" -> meta.recordsWritten,
    "manifest_list_location" -> meta.manifestListLocation,
    "summary" -> meta.summary
  )
}
