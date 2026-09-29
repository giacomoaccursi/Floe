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
  batchIdFormat: "yyyyMMdd"
  maxRejectionRate: 0.1
  qualityMetricsTable: "quality_metrics"

performance:
  parallelFlows: true

iceberg:
  catalogType: "hadoop"
  catalogName: "floe"
  namespace: "default"
  warehouse: "output/warehouse"
  fileFormat: "parquet"
  enableSnapshotTagging: true
  catalogProperties: {}
  maintenance:
    snapshotRetentionDays: 7
    targetFileSizeMb: 128
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
| `warningsPath` | no | `{outputPath}/warnings` | Base directory for validation warning records. Each batch is written to `{warningsPath}/{flowName}/batch_id={batchId}/`. |

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
| `batchIdFormat` | `yyyyMMdd_HHmmss` | Java `DateTimeFormatter` pattern for the readable prefix of a batch ID. Use `timestamp` for epoch millis. A random UUID suffix is always appended to avoid collisions. |
| `maxRejectionRate` | — (disabled) | If set, the batch stops when any flow's rejection rate exceeds this threshold (0.1 = 10%). |
| `qualityMetricsTable` | — (disabled) | If set, writes per-flow quality metrics to this Iceberg table after each batch. See [Quality Metrics](../guides/quality-metrics.md). |

!!!warning "Batch ID collisions"
    The timestamp pattern controls readability, not uniqueness: every generated ID also includes a UUID suffix. Full and Delta loads are **not** generally idempotent under replay (Delta without a primary key appends), so retain the generated ID and the exact source input when investigating a failed run.

### Rejection behavior

When `maxRejectionRate` is not set, the batch always continues — rejected records are written to `rejectedPath` and valid records proceed to Iceberg.

When `maxRejectionRate` is set (e.g. `0.1` = 10%), the batch stops if any flow's rejection rate exceeds the threshold. The comparison is strict `>`: a rate exactly equal to the threshold does not trigger a stop.

Individual flows can override the global threshold with their own `maxRejectionRate` field. See [Flow Configuration](../configuration/flows.md).

### Retry behavior

Floe does not retry a flow or the whole pipeline. A previous flow may already have committed, so retrying a later failure in isolation is not a safe default for the pipeline. Configure the external orchestrator with retries disabled unless the complete application and deployment have been qualified as repeatable. Iceberg and storage clients may still perform their own bounded, validated transport or commit retries.

Validation rejections are not transient. A rejection-threshold breach fails before the target-table mutation.

For the full validation pipeline, see [Validation Engine](../guides/validation.md).

## performance

Controls parallel execution of flows.

The section is optional. If omitted, `parallelFlows` remains `false`.

| Field | Default | Description |
|-------|---------|-------------|
| `parallelFlows` | `false` | Execute independent flows (no FK or `dependsOn` dependency) in parallel |

When `parallelFlows` is `true`, flows with no dependency relationship (neither FK nor `dependsOn`) are grouped and executed concurrently using a bounded thread pool. Flows connected by FK dependencies or `dependsOn` always execute in topological order regardless of this setting.

For parallel DAG node execution, see the `parallelNodes` field in [DAG Configuration](../configuration/dag.md).

## iceberg

Apache Iceberg storage layer configuration. This section is **required** — the framework fails fast at startup if it's missing or invalid.

For the complete Iceberg integration guide, see [Iceberg Integration](../guides/iceberg.md).

| Field | Default | Description |
|-------|---------|-------------|
| `catalogType` | `hadoop` | Catalog implementation: `hadoop`, `glue`, or a custom type registered via the [Pipeline Builder](../guides/pipeline-builder.md#custom-catalog-providers) |
| `catalogName` | `floe` | Catalog name used in SQL queries. The built-in providers use `SparkCatalog`, so `spark_catalog` is rejected because Spark reserves it for the session catalog. |
| `namespace` | `default` | Iceberg namespace (database) for tables. Tables are named `{catalogName}.{namespace}.{flowName}`. |
| `warehouse` | — (required) | Path to the Iceberg warehouse directory |
| `fileFormat` | `parquet` | Default data file format for Iceberg tables: `parquet`, `orc`, `avro`. Sets the `write.format.default` table property. If a flow specifies `write.format.default` in its `tableProperties`, that takes priority. |
| `enableSnapshotTagging` | `true` | Create a batch tag for the table's current snapshot after a write; a concurrent writer on the same table can invalidate attribution to this batch. |
| `catalogProperties` | `{}` | Additional key-value properties passed to the catalog provider |

### Catalog types

A catalog is the component that keeps track of which Iceberg tables exist and where their data files are stored. Think of it as a registry: when the framework writes to `floe.default.orders`, the catalog knows where to find (or create) that table.

The framework ships with two built-in catalog providers:

| `catalogType` | When to use | Description |
|---------------|-------------|-------------|
| `hadoop` | Local development, HDFS; S3 only with a suitable lock manager | Stores table metadata as files in the warehouse directory. S3 deployments need external commit locking for concurrent writes; FLOe rejects an S3 warehouse without `catalogProperties.lock-impl`. |
| `glue` | AWS with Glue Data Catalog | Registers tables in AWS Glue. Query-engine interoperability depends on each engine's Iceberg support and table features. Requires the Iceberg AWS bundle, credentials, region and suitable S3/Glue IAM permissions. |

For local development, `hadoop` is the simplest choice — it works out of the box with no infrastructure:

```yaml
iceberg:
  catalogType: "hadoop"
  warehouse: "output/warehouse"
```

For AWS production deployments with Glue:

```yaml
iceberg:
  catalogType: "glue"
  catalogName: "floe"
  warehouse: "s3://my-bucket/warehouse"
  catalogProperties:
    glue.skip-name-validation: "true"
```

`catalogProperties` is a pass-through map — any key-value pair you add is set as a Spark configuration property on the catalog (`spark.sql.catalog.{catalogName}.{key}`). Use it for catalog-specific settings that the framework doesn't expose directly.

Custom catalog providers (Hive, REST, Nessie) can be registered via the [Pipeline Builder API](../guides/pipeline-builder.md#custom-catalog-providers).

### maintenance

Asynchronous table-maintenance settings. Floe stores one `QUEUED` task per managed target while finalizing a successful run. A separate `MaintenanceWorker` applies these settings only after the run reaches `PUBLISHED` or `SUCCEEDED_WITH_WARNINGS`; ingestion does not wait for it.

| Field | Default | Description |
|-------|---------|-------------|
| `snapshotRetentionDays` | `7` | Expiration threshold for eligible snapshots and retention of new batch tags. Tags/branches can preserve older snapshots. Remove to disable expiration; new tags then have no explicit expiry. Use a positive value when tagging is enabled. |
| `targetFileSizeMb` | `128` | Target file size after compaction. Remove to disable compaction. |
| `orphanRetentionMinutes` | `1440` | Grace period before orphan files are removed (min 1440). Remove to disable. |
| `enableManifestRewrite` | `false` | Rewrite manifest files for scan optimization |

!!!warning "Orphan cleanup minimum retention"
    FLOe clamps the configured threshold to **at least 24 hours** (1440 minutes). That is not universally safe: set it above the longest running write, backup or migration, or concurrent operations may lose in-flight files.

!!!note "Maintenance state is durable"
    `IngestionResult.maintenanceResults` and `summary.json` normally report `QUEUED`, because they are produced before the worker runs. Query `RunStore.getMaintenanceTasks` for current state. A worker failure changes the durable run status to `SUCCEEDED_WITH_WARNINGS`; retry only maintenance, never ingestion. See [Recovery and Production Operations](../guides/recovery.md#asynchronous-maintenance).
