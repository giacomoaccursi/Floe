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
  maxRetries: 3
  retryBackoffMs: 2000
  qualityMetricsTable: "quality_metrics"

performance:
  parallelFlows: true

iceberg:
  catalogType: "hadoop"
  catalogName: "spark_catalog"
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
| `batchIdFormat` | `yyyyMMdd_HHmmss` | Java `DateTimeFormatter` pattern for batch ID generation. Use `timestamp` for epoch millis. |
| `maxRejectionRate` | — (disabled) | If set, the batch stops when any flow's rejection rate exceeds this threshold (0.1 = 10%). |
| `maxRetries` | `0` | Maximum number of retries per flow on failure. `0` means no retry. |
| `retryBackoffMs` | `1000` | Base delay in milliseconds for exponential backoff between retries. |
| `qualityMetricsTable` | — (disabled) | If set, writes per-flow quality metrics to this Iceberg table after each batch. See [Quality Metrics](../guides/quality-metrics.md). |

!!!warning "Batch ID collisions"
    Low-granularity formats like `yyyyMMdd` produce the same batch ID if the pipeline runs more than once in the same day. This is safe for Full and Delta loads (idempotent), but SCD2 flows may create extra versions and snapshot tagging will fail on the duplicate tag. Use the default `yyyyMMdd_HHmmss` or `timestamp` to avoid collisions.

### Rejection behavior

When `maxRejectionRate` is not set, the batch always continues — rejected records are written to `rejectedPath` and valid records proceed to Iceberg.

When `maxRejectionRate` is set (e.g. `0.1` = 10%), the batch stops if any flow's rejection rate exceeds the threshold. The comparison is strict `>`: a rate exactly equal to the threshold does not trigger a stop.

Individual flows can override the global threshold with their own `maxRejectionRate` field. See [Flow Configuration](../configuration/flows.md).

### Retry behavior

When `maxRetries` is greater than 0, the framework retries failed flows using exponential backoff with jitter:

| Attempt | Delay |
|---------|-------|
| 1st retry | `retryBackoffMs` + jitter |
| 2nd retry | `retryBackoffMs × 2` + jitter |
| 3rd retry | `retryBackoffMs × 4` + jitter |

Jitter is a random value between 0 and `retryBackoffMs`, added to prevent thundering herd when multiple flows retry simultaneously.

A flow is retried only when it throws an exception (e.g. network timeout, Iceberg commit conflict). Validation failures (rejected records) are not retried — they produce a `FlowResult` with `success = true` and the rejected records are written normally.

For the full validation pipeline, see [Validation Engine](../guides/validation.md).

## performance

Controls parallel execution of flows.

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
| `catalogName` | `spark_catalog` | Catalog name used in SQL queries |
| `warehouse` | — (required) | Path to the Iceberg warehouse directory |
| `fileFormat` | `parquet` | Default data file format for Iceberg tables: `parquet`, `orc`, `avro`. Sets the `write.format.default` table property. If a flow specifies `write.format.default` in its `tableProperties`, that takes priority. |
| `enableSnapshotTagging` | `true` | Tag each batch snapshot for time travel |
| `catalogProperties` | `{}` | Additional key-value properties passed to the catalog provider |

### Catalog types

A catalog is the component that keeps track of which Iceberg tables exist and where their data files are stored. Think of it as a registry: when the framework writes to `spark_catalog.default.orders`, the catalog knows where to find (or create) that table.

The framework ships with two built-in catalog providers:

| `catalogType` | When to use | Description |
|---------------|-------------|-------------|
| `hadoop` | Local development, HDFS, S3 without Glue | Stores table metadata as files in the warehouse directory. No external service needed. |
| `glue` | AWS with Glue Data Catalog | Registers tables in AWS Glue, making them queryable from Athena, Redshift Spectrum, and EMR. Requires S3 and Glue IAM permissions. |

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
  catalogName: "spark_catalog"
  warehouse: "s3://my-bucket/warehouse"
  catalogProperties:
    glue.skip-name-validation: "true"
```

`catalogProperties` is a pass-through map — any key-value pair you add is set as a Spark configuration property on the catalog (`spark.sql.catalog.{catalogName}.{key}`). Use it for catalog-specific settings that the framework doesn't expose directly.

Custom catalog providers (Hive, REST, Nessie) can be registered via the [Pipeline Builder API](../guides/pipeline-builder.md#custom-catalog-providers).

### maintenance

Post-batch table maintenance settings. Maintenance runs after all flows execute successfully and after [orphan detection](../guides/orphan-detection.md).

| Field | Default | Description |
|-------|---------|-------------|
| `snapshotRetentionDays` | `7` | Days to retain snapshots. Set to expire old snapshots after this period. Remove to disable. |
| `targetFileSizeMb` | `128` | Target file size after compaction. Remove to disable compaction. |
| `orphanRetentionMinutes` | `1440` | Grace period before orphan files are removed (min 1440). Remove to disable. |
| `enableManifestRewrite` | `false` | Rewrite manifest files for scan optimization |

!!!warning "Orphan cleanup minimum retention"
    Iceberg enforces a **minimum retention of 24 hours** (1440 minutes) for orphan file cleanup. Values below 1440 are automatically clamped with a warning. This prevents data corruption from concurrent operations.

!!!note "Maintenance is best-effort"
    A maintenance failure does not abort the batch. The batch result still reports SUCCESS if all flow writes completed. However, subsequent maintenance operations in the same batch may be skipped if the failure propagates.
