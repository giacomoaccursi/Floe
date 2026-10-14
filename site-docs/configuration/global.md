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
  maxParallelFlows: 4

iceberg:
  catalogMode: "configure"
  ddlMode: "automatic"
  catalogType: "hadoop"
  catalogName: "floe"
  namespace: "default"
  warehouse: "output/warehouse"
  fileFormat: "parquet"
  enableSnapshotTagging: true
  catalogProperties: {}
  maintenance:
    enabled: false
    snapshotRetentionDays: 7
    targetFileSizeMb: 128
    orphanRetentionMinutes: 4320
    enableManifestRewrite: false
```

## paths

Base directories for all pipeline output.

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `outputPath` | yes | — | Base directory for flow output data |
| `rejectedPath` | yes | — | Directory for rejected records |
| `metadataPath` | yes | — | Directory for batch and flow metadata JSON |
| `warningsPath` | no | `{outputPath}/warnings` | Base directory for validation warning records. Each attempt is written below `{warningsPath}/{flowName}/pipeline_id={pipelineId}/logical_run_id={logicalRunId}/attempt_id={attemptId}/`. |

The first three paths are required and must be non-blank. `warningsPath` is optional, but must also be non-blank when
present; if omitted, warnings go to `{outputPath}/warnings`. These invariants are checked by `validate()` before any
source read. Paths can use [variable substitution](overview.md#variable-substitution):

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
| `batchIdFormat` | `yyyyMMdd_HHmmss` | Non-blank Java `DateTimeFormatter` pattern for the readable prefix of a batch ID. Use `timestamp` for epoch millis or `datetime` for `yyyyMMdd_HHmmss`. Invalid patterns fail static validation. A random UUID suffix is always appended to avoid collisions. |
| `maxRejectionRate` | — (disabled) | If set, it must be finite and in `[0.0, 1.0]`; the batch stops when any flow's rejection rate exceeds it (0.1 = 10%). |
| `qualityMetricsTable` | — (disabled) | If set, it must be non-blank and writes per-flow quality metrics to this Iceberg table after each batch. Its name must not collide case-insensitively with a flow or derived table. See [Quality Metrics](../guides/quality-metrics.md). |

!!!warning "Batch ID collisions"
    The timestamp pattern controls readability, not uniqueness: every generated ID also includes a UUID suffix. Full loads are destructive replacements and Delta loads require an explicit primary key; neither constitutes a general replay guarantee. Retain the generated ID and the exact source input when investigating a failed run.

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

The section is optional. If omitted, `parallelFlows` remains `false` and `maxParallelFlows` remains `4`.

| Field | Default | Description |
|-------|---------|-------------|
| `parallelFlows` | `false` | Execute independent flows (no FK or `dependsOn` dependency) in parallel |
| `maxParallelFlows` | `4` | Positive upper bound on flow tasks submitted concurrently when parallel execution is enabled |

When `parallelFlows` is `true`, flows with no dependency relationship (neither FK nor `dependsOn`) are grouped and executed concurrently using a thread pool of exactly `maxParallelFlows` workers. Flows connected by FK dependencies or `dependsOn` always execute in topological order regardless of this setting. Size the limit against driver memory, source-system connection limits, catalog commit capacity, and the Spark scheduler; Floe never derives it from the host CPU count.

This setting applies to ingestion flows, which perform actions and Iceberg writes. DAG aggregation instead returns one lazy DataFrame; Spark owns its physical task parallelism when the caller materializes it. See [DAG Configuration](dag.md).

## iceberg

Apache Iceberg storage layer configuration. This section is **required** — the framework fails fast at startup if it's missing or invalid.

For the complete Iceberg integration guide, see [Iceberg Integration](../guides/iceberg.md).

| Field | Default | Description |
|-------|---------|-------------|
| `catalogMode` | `existing` | Catalog ownership: `existing` validates a platform-configured Spark catalog without mutating session settings; `configure` explicitly invokes a FLOe catalog provider. |
| `ddlMode` | `validate` | Table ownership: `validate` requires pre-provisioned targets and performs read-only contract checks; `automatic` allows Floe to create and evolve tables. |
| `catalogType` | `hadoop` | Catalog implementation: `hadoop`, `glue`, or a custom type registered via the [Pipeline Builder](../guides/pipeline-builder.md#custom-catalog-providers) |
| `catalogName` | `floe` | Catalog name used in SQL queries. The built-in providers use `SparkCatalog`, so `spark_catalog` is rejected because Spark reserves it for the session catalog. |
| `namespace` | `default` | Iceberg namespace (database) for tables. Tables are named `{catalogName}.{namespace}.{flowName}`. |
| `warehouse` | `""` | Warehouse path used only by `catalogMode: configure`; required by the built-in Hadoop provider. In `existing` mode it belongs to the platform's Spark catalog configuration and should be omitted here. |
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
  catalogMode: "configure"
  ddlMode: "automatic"
  catalogType: "hadoop"
  warehouse: "output/warehouse"
```

For AWS production deployments with Glue:

```yaml
iceberg:
  catalogMode: "configure"
  ddlMode: "validate"
  catalogType: "glue"
  catalogName: "floe"
  warehouse: "s3://my-bucket/warehouse"
  catalogProperties:
    glue.skip-name-validation: "true"
```

`catalogProperties` is applied only in `catalogMode: configure`. In the default `existing` mode the platform owns every `spark.sql.catalog.*` setting; FLOe verifies that the named catalog and Iceberg extensions exist and does not overwrite them.

`ddlMode` is independent from `catalogMode`. A job may bootstrap a catalog but still require tables to be provisioned separately. In `validate`, Floe checks that every primary, derived, and configured quality-metrics target exists before any flow runs; immediately before each write it also verifies the exact column names and types, current partition and sort orders, Iceberg format version, file format, and configured custom table properties. Any mismatch fails without issuing DDL.

Custom catalog providers (Hive, REST, Nessie) can be registered via the [Pipeline Builder API](../guides/pipeline-builder.md#custom-catalog-providers).
In `configure` mode, `validate()` resolves the selected built-in or custom provider and runs its configuration checks;
unsupported catalog types therefore fail before pipeline construction. Catalog names, namespaces, and catalog-property
keys must be non-blank, and the global file format must be exactly `parquet`, `orc`, or `avro`.

### maintenance

Settings consumed only by the independently scheduled `FloeRunner maintenance` command or a direct `IcebergMaintenanceRunner` call. Ingestion does not queue, run, or retry table maintenance. The command requires the explicit `enabled: true` safety gate.

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Required opt-in for the maintenance command. It does not schedule maintenance. |
| `snapshotRetentionDays` | `7` | Positive expiration threshold for eligible snapshots and retention of new batch tags. Tags/branches can preserve older snapshots. Set `null` to omit expiration; new tags then have no explicit expiry. Tag retention applies even while maintenance is disabled. |
| `targetFileSizeMb` | `128` | Positive target file size after compaction. Set `null` to omit compaction. |
| `orphanRetentionMinutes` | `4320` | Grace period before orphan files are removed (minimum 1440). Set `null` to omit orphan cleanup. The 3-day default matches Iceberg's procedure default. |
| `enableManifestRewrite` | `false` | Rewrite manifest files for scan optimization |

!!!warning "Orphan cleanup minimum retention"
    FLOe rejects, rather than silently clamps, a threshold below **24 hours** (1440 minutes). Even 3 days is not universally safe: configure a value longer than every write, checkpoint, backup, restore, migration, and delayed retry that may still own uncommitted files. Incorrect path normalization or object-store authority settings can also make orphan cleanup destructive; qualify it on the real catalog and storage deployment.

!!!note "Action selection"
    With `enabled: true`, at least one action must be selected. `null` disables an optional action; omitting a field uses its documented default. The fixed order is snapshot expiration, data-file rewrite, orphan cleanup, then manifest rewrite.

!!!note "Maintenance ownership"
    The hosting platform owns the maintenance schedule, status, retries, and mutual exclusion. A maintenance failure must retry only maintenance, never ingestion. See [Failure Handling and Production Operations](../guides/recovery.md#maintenance).
