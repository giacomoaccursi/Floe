package com.etl.framework.iceberg

case class IcebergFlowMetadata(
    tableName: String,
    snapshotId: Long,
    snapshotTag: Option[String],
    parentSnapshotId: Option[Long],
    snapshotTimestampMs: Long,
    recordsWritten: Long,
    manifestListLocation: String,
    summary: Map[String, String]
)
