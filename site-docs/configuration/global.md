# Global Configuration

Complete reference for `global.yaml` — the framework's global settings file.

## Full example

```yaml
paths:
  outputPath: "output/data"
  rejectedPath: "output/rejected"
  metadataPath: "output/metadata"
  warningsPath: "output/warnings"   # optional, defaults to {outputPath}/warnings

processing:
  batchIdFormat: "yyyyMMdd_HHmmss"
  maxRejectionRate: 0.1

performance:
  parallelFlows: true
  parallelNodes: true

iceberg:
  catalogType: "hadoop"
  catalogName: "spark_catalog"
  warehouse: "output/warehouse"
  fileFormat: "parquet"
  enableSnapshotTagging: true
  catalogProperties: {}
  maintenance:
    enableSnapshotExpiration: true
    snapshotRetentionDays: 7
    enableCompaction: true
    targetFileSizeMb: 128
    enableOrphanCleanup: true
    orphanRetentionMinutes: 1440
    enableManifestRewrite: false
```

## paths

Base directories for all pipeline output.

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `outputPath` | yes | — | Base directory for flow output data |
| `rejectedPath` | yes | — | Directory for rejected records |
| `metadataPath` | yes | — | Directory for batch and flow metadata JSON |
| `warningsPath` | no | `{outputPath}/warnings` | Directory for validation warning records. If not set, defaults to `{outputPath}/warnings`. Each flow's warnings are written to `{warningsPath}/{flowName}/`. |

The first three paths are required. `warningsPath` is optional — if omitted, warnings go to `{outputPath}/warnings`. They can use [variable substitution](overview.md#variable-substitution):

```yaml
paths:
  outputPath: "${OUTPUT_PATH}/data"
  rejectedPath: "${OUTPUT_PATH}/rejected"
  metadataPath: "${OUTPUT_PATH}/metadata"
  warningsPath: "${OUTPUT_PATH}/warnings"   # optional
```

## processing

Controls batch execution and validation behavior. This entire section is optional — if omitted, the framework uses sensible defaults.

| Field | Default | Description |
|-------|---------|-------------|
| `batchIdFormat` | `yyyyMMdd_HHmmss` | Java `DateTimeFormatter` pattern for batch ID generation (e.g. `yyyyMMdd_HHmmss` → `20260328_150000`). Use `timestamp` for epoch millis. |
| `maxRejectionRate` | — (disabled) | If set, the batch stops when any flow's rejection rate exceeds this threshold (0.1 = 10%). If omitted, the batch never stops for rejected records. |

### Rejection behavior

| `maxRejectionRate` | Rejection rate vs threshold | Behavior |
|-------------------|---------------------------|----------|
| not set | any | Batch continues. Rejected records are written to `rejectedPath`, valid records proceed to Iceberg. |
| set | `rate > maxRejectionRate` | Batch stops. Remaining flows in the group are not executed. |
| set | `rate <= maxRejectionRate` | Batch continues. |

The comparison uses strict `>` (not `>=`): a rejection rate exactly equal to the threshold does not trigger a stop.

Individual flows can override the global threshold with their own `maxRejectionRate` field. See [Flow Configuration](../configuration/flows.md).

For the full validation pipeline, see [Validation Engine](../guides/validation.md).

## performance

Controls parallel execution of flows and DAG nodes.

| Field | Default | Description |
|-------|---------|-------------|
| `parallelFlows` | `false` | Execute independent flows (no FK dependency) in parallel |
| `parallelNodes` | `false` | Execute independent DAG nodes in parallel |

When `parallelFlows` is `true`, flows with no FK relationship between them are grouped and executed concurrently using a bounded thread pool. Flows connected by FK dependencies always execute in topological order regardless of this setting.

When `parallelNodes` is `true`, DAG nodes within the same execution group run in parallel. See [DAG Aggregation](../guides/dag-aggregation.md) for details.

## iceberg

Apache Iceberg storage layer configuration. This section is **required** — the framework fails fast at startup if it's missing or invalid.

For the complete Iceberg integration guide, see [Iceberg Integration](../guides/iceberg.md).

| Field | Default | Description |
|-------|---------|-------------|
| `catalogType` | `hadoop` | Catalog implementation: `hadoop`, `glue` |
| `catalogName` | `spark_catalog` | Catalog name used in SQL queries |
| `warehouse` | — (required) | Path to the Iceberg warehouse directory |
| `fileFormat` | `parquet` | Iceberg data file format: `parquet`, `orc`, `avro` |
| `enableSnapshotTagging` | `true` | Tag each batch snapshot for time travel |
| `catalogProperties` | `{}` | Additional key-value properties passed to the catalog provider |

### Catalog types

The framework ships with two built-in catalog providers:

| `catalogType` | Provider | Description |
|---------------|----------|-------------|
| `hadoop` | `HadoopCatalogProvider` | Local/HDFS filesystem catalog. Zero infrastructure. |
| `glue` | `GlueCatalogProvider` | AWS Glue Data Catalog. Requires S3 and Glue permissions. |

For Glue, pass additional properties via `catalogProperties`:

```yaml
iceberg:
  catalogType: "glue"
  catalogName: "spark_catalog"
  warehouse: "s3://my-bucket/warehouse"
  catalogProperties:
    glue.skip-name-validation: "true"
```

Custom catalog providers (Hive, REST, Nessie) can be registered via the [Pipeline Builder API](../guides/pipeline-builder.md#custom-catalog-providers).

### maintenance

Post-batch table maintenance settings. Maintenance runs after all flows execute successfully and after [orphan detection](../guides/orphan-detection.md).

| Field | Default | Description |
|-------|---------|-------------|
| `enableSnapshotExpiration` | `true` | Expire snapshots older than the retention period |
| `snapshotRetentionDays` | `7` | Days to retain snapshots before expiration |
| `enableCompaction` | `true` | Compact small data files into larger ones |
| `targetFileSizeMb` | `128` | Target file size after compaction |
| `enableOrphanCleanup` | `true` | Remove orphaned files left by failed operations |
| `orphanRetentionMinutes` | `1440` | Grace period before orphan files become eligible for cleanup |
| `enableManifestRewrite` | `false` | Rewrite manifest files for scan optimization |

!!!warning "Orphan cleanup minimum retention"
    Iceberg enforces a **minimum retention of 24 hours** (1440 minutes) for orphan file cleanup. Values below 1440 are automatically clamped with a warning. This prevents data corruption from concurrent operations.

!!!note "Maintenance is best-effort"
    A maintenance failure does not abort the batch. The batch result still reports SUCCESS if all flow writes completed. However, subsequent maintenance operations in the same batch may be skipped if the failure propagates.
