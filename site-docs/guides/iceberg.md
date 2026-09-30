# Iceberg Integration

## Overview

The framework uses Apache Iceberg as its table format. Each table write commits atomically; a pipeline attempt that writes several tables is **not** one Iceberg transaction. Delta and SCD2 use `MERGE INTO`, while full loads overwrite table contents. FLOe returns per-target snapshot evidence and a typed attempt result. Maintenance is a separate application owned and scheduled by the hosting platform.

The `iceberg` section is required in `global.yaml`. At startup, the pipeline validates the config and configures the SparkSession with the Iceberg catalog. If the section is missing or invalid, execution stops immediately (fail-fast).

## Prerequisites

### SparkSession configuration

The Iceberg Spark extensions **must** be configured before the SparkSession is created. Spark does not allow changing `spark.sql.extensions` after session creation. The framework configures the catalog settings automatically from `global.yaml`, but the extensions must be set by the application entry point:

```scala
implicit val spark: SparkSession = SparkSession.builder()
  .appName("My ETL Pipeline")
  .master("local[*]")
  .config("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .getOrCreate()
```

All other Iceberg catalog settings (warehouse path, catalog type, catalog class) are applied automatically by the framework at pipeline startup.

!!!tip "Keep Adaptive Query Execution enabled"
    Spark 3.5 enables AQE by default (`spark.sql.adaptive.enabled = true`). It can improve MERGE and DAG joins through runtime partition coalescing, join conversion, and skew handling. Treat it as a workload-tuned Spark feature rather than a correctness requirement, and benchmark before changing its settings.

Floe pins [Spark 3.5.8](https://spark.apache.org/docs/3.5.8/), Scala 2.12.18, and [Iceberg 1.10.1](https://iceberg.apache.org/docs/1.10.1/spark-configuration/), and is built and tested on Java 17. Spark 3.5 documents support for Java 8, 11, and 17; see [Installation](../getting-started/installation.md#java-compatibility) before changing the JDK. Keep the Iceberg runtime artifact aligned with both the Spark and Scala binary versions.

## Configuring Iceberg

The `iceberg` block in `global.yaml` is required:

```yaml
iceberg:
  catalogType: "hadoop"
  catalogName: "floe"
  namespace: "default"
  warehouse: "output/warehouse"
  fileFormat: "parquet"
  enableSnapshotTagging: true
  maintenance:
    snapshotRetentionDays: 7
    targetFileSizeMb: 128
    orphanRetentionMinutes: 1440
    enableManifestRewrite: false
```

For the full field reference, see [Global Configuration — iceberg](../configuration/global.md#iceberg).

### Configuration reference

| Field | Default | Description |
|-------|---------|-------------|
| `catalogType` | `hadoop` | Iceberg catalog implementation: `hadoop`, `glue`, or a custom type registered via the [Pipeline Builder](pipeline-builder.md#custom-catalog-providers) |
| `catalogName` | `floe` | Name used in SQL queries (`catalog.namespace.table`). The built-in providers reject Spark's reserved `spark_catalog` name because they install `SparkCatalog`, not `SparkSessionCatalog`. |
| `namespace` | `default` | Iceberg namespace for tables |
| `warehouse` | *required* | Path to the Iceberg warehouse directory |
| `fileFormat` | `parquet` | Default data file format |
| `enableSnapshotTagging` | `true` | Tag each batch snapshot for time travel by batch ID |
| `catalogProperties` | `{}` | Additional key-value properties passed to the catalog provider |
| `maintenance.*` | see below | Settings consumed only when an independently scheduled application invokes `IcebergMaintenanceRunner` |

### Maintenance settings

| Field | Default | Description |
|-------|---------|-------------|
| `snapshotRetentionDays` | `7` | Expiration threshold and retention for **new** batch tags. Live references can keep older snapshots. Remove to disable expiration and create tags without an explicit expiry; positive values are required when tagging is enabled. |
| `targetFileSizeMb` | `128` | Target file size after compaction. Remove to disable. |
| `orphanRetentionMinutes` | `1440` | Grace period before orphan files are removed (min 1440). Remove to disable. |
| `enableManifestRewrite` | `false` | Rewrite manifest files for scan optimization |

!!!warning "Orphan cleanup minimum retention"
    FLOe clamps values below 24 hours (1440 minutes), but that is **not** a universal safe value: a write, checkpoint, backup or migration older than the threshold can still have in-flight files. Set the grace period above the longest expected operation and verify against your deployment before enabling cleanup.

!!!warning "Catalogs on S3"
    HadoopCatalog on S3 requires an appropriate lock manager for safe concurrent commits. FLOe rejects an S3 warehouse using the built-in Hadoop provider without `catalogProperties.lock-impl`; the [Iceberg AWS guide](https://iceberg.apache.org/docs/1.10.1/aws/) documents DynamoDB locking. GlueCatalog uses optimistic locking with supported AWS SDK versions. The built-in Glue provider also needs AWS SDK v2 client classes: the FLOe build now includes `iceberg-aws-bundle`, and deployed jobs must carry that JAR plus suitable AWS credentials, region and IAM permissions.

## Architecture

### Catalog provider

The catalog system is pluggable. The `CatalogProvider` trait defines three methods: `catalogType`, `configureCatalog`, and `validateConfig`. The built-in hadoop provider verifies that Iceberg extensions are registered on the SparkSession and configures the catalog with:

```
spark.sql.catalog.{name}          = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.{name}.type     = hadoop
spark.sql.catalog.{name}.warehouse = {path}
```

The extensions (`spark.sql.extensions`) must be set by the user before creating the SparkSession — the provider validates their presence and throws an error if missing.

The framework maps the `catalogType` string to the right provider. Adding a new catalog type (Hive, REST, Nessie) means implementing the `CatalogProvider` trait and registering it on the builder. See [Pipeline Builder — Custom catalog providers](pipeline-builder.md#custom-catalog-providers) for details.

### Table naming

Every flow maps to a single Iceberg table with the convention:

```
{catalogName}.{namespace}.{flowName}
```

For example, a flow named `customers` with catalog `floe` and namespace `default` becomes `floe.default.customers`. The namespace is configurable via `iceberg.namespace` in `global.yaml` (defaults to `default`).

### Table creation and schema

Tables are created on first write with `CREATE TABLE IF NOT EXISTS`. The schema is derived from the flow's `SchemaConfig` columns plus any system columns added by the load mode (e.g., `valid_from`, `valid_to`, `is_current` for SCD2).

### Schema evolution

Every run, the framework compares the incoming schema with the existing table schema and adds any new columns via `ALTER TABLE ADD COLUMN`. Columns present in the table but absent from the incoming schema are left untouched — the framework never drops columns.

The framework also applies safe type widening automatically. If an existing column's type can be safely widened to match the incoming schema, the change is applied via `ALTER TABLE ALTER COLUMN TYPE`:

| From | To | Safe? |
|------|----|-------|
| `int` | `long` | Yes |
| `float` | `double` | Yes |
| `decimal(p1, s)` | `decimal(p2, s)` where `p2 > p1` | Yes (same scale, wider precision) |
| Any other combination | | No — logged as warning, skipped |

Incompatible type changes (e.g. `string` → `int`, `long` → `int`) are not applied. The framework logs a warning and leaves the column unchanged. Resolve these manually with `ALTER TABLE`.

This means adding a column to a flow's `schema.columns` section takes effect at the next run without manual intervention:

1. The new column is added to the Iceberg table via ALTER TABLE
2. Existing rows have `NULL` for the new column
3. The MERGE INTO includes the new column in the change detection and update logic
4. Rows with a non-NULL value for the new column are updated; rows where the source also has NULL are skipped

Example: adding a `notes` column to an orders flow.

```yaml
# Before
columns:
  - name: "order_id"
    type: "integer"
    nullable: false
  - name: "status"
    type: "string"
    nullable: false

# After — just add the new column
columns:
  - name: "order_id"
    type: "integer"
    nullable: false
  - name: "status"
    type: "string"
    nullable: false
  - name: "notes"
    type: "string"
    nullable: true
```

At the next run, the framework logs `Added column notes (STRING) to catalog.default.orders` and the column becomes available.

!!!note "Special case: `is_active` column on existing SCD2 tables"
    When `detectDeletes` is enabled mid-stream, the `is_active` column is added via schema evolution. Existing rows will have `is_active = NULL`, which causes `WHERE is_active = true` queries to exclude them. The framework emits a specific warning for this case. See [SCD2 Guide](scd2.md#enabling-detectdeletes-on-an-existing-table) for details and the recommended backfill procedure.

### Table configuration updates

Every run, the framework compares the current table state against the flow config and applies any differences:

- **Schema**: new columns are added via `ALTER TABLE ADD COLUMN` (see above).
- **Table properties**: reads current properties via `SHOW TBLPROPERTIES` and applies only new or changed entries via `ALTER TABLE SET TBLPROPERTIES`. Existing properties not mentioned in the config are left untouched.
- **Partition spec**: attempts `ALTER TABLE ADD PARTITION FIELD` for each configured partition. If the field already exists, the operation is silently skipped.

This means adding `icebergPartitions`, `tableProperties`, or new schema columns to an existing flow config takes effect at the next run without manual intervention.

#### Partition spec on tables with existing data

Adding a partition field to a table that already contains data does **not** rewrite existing files. Iceberg applies the new spec only to files written after the change. The result is a mixed layout:

- Files written under the old unpartitioned spec have no value for the new partition field, so the new transform cannot prune them; other Iceberg statistics may still prune files for a particular query
- Files written after the change: partitioned, eligible for pruning

The framework logs a `WARN` message whenever a new partition field is applied to an existing table:

```
WARN  Partition field 'month(order_date)' was added to an existing table (catalog.default.orders).
      Data written before this change is NOT retroactively partitioned:
      partition pruning will apply only to files written from this run onwards.
```

To apply the new partition layout to all existing data, perform a one-time full reload: temporarily set `loadMode.type: full`, run the pipeline with a complete source dataset, then revert to `delta`. This rewrites all files under the new partition spec. For large tables, the equivalent Iceberg maintenance procedure is preferable:

```sql
CALL catalog.system.rewrite_data_files(
  table => 'catalog.default.orders',
  strategy => 'sort',
  sort_order => 'order_date ASC'
)
```

The `sort` strategy rewrites **selected** files and orders their rows; it does not guarantee that every historical file is selected. To migrate the physical layout, inspect the table's files and partition spec IDs, then use `rewrite-all` and `output-spec-id` where appropriate for the installed Iceberg version. Verify the resulting `files` metadata table. The framework's automatic `binpack` compaction targets small files and does not establish sort order.

For some multi-column filters, Iceberg Spark supports Z-ordering as a sort-order expression; it is **not** a separate `zorder` strategy:

```sql
CALL catalog.system.rewrite_data_files(
  table => 'catalog.default.orders',
  strategy => 'sort',
  sort_order => 'zorder(order_date,customer_id)'
)
```

Measure file bounds and bytes scanned before and after any rewrite. Layout can degrade with later writes, so this is not necessarily a one-time operation.

### Partitioning and sort order

Partitions are configured per-flow in the output section:

```yaml
output:
  icebergPartitions:
    - "month(order_date)"
    - "bucket(16, customer_id)"
  sortOrder:
    - "order_date"
    - "customer_id"
```

Supported partition transforms: `year()`, `month()`, `day()`, `hour()`, `bucket(n, col)`, `truncate(len, col)`, and identity (bare column name). Transforms are case-insensitive (`MONTH(ts)` and `month(ts)` are equivalent).

| Transform | Example | When to use |
|-----------|---------|-------------|
| `year(col)` | `year(created_at)` | Low-volume tables with multi-year history |
| `month(col)` | `month(order_date)` | Most common choice for date-based tables |
| `day(col)` | `day(event_time)` | High-volume tables with daily queries |
| `hour(col)` | `hour(event_time)` | Very high-volume tables (millions of rows/day) |
| `bucket(n, col)` | `bucket(16, customer_id)` | Distribute rows evenly across N buckets for non-temporal columns |
| `truncate(len, col)` | `truncate(2, country_code)` | Group string values by prefix length |
| identity | `region` | Column with very few distinct values (use sparingly) |

Sort order is applied with `WRITE ORDERED BY`, which controls data layout within files for better scan performance without affecting query semantics.

!!!warning "Partitioning guidelines"
    - Partition on low-cardinality temporal columns (`month(order_date)`, `year(created_at)`)
    - Do not partition on high-cardinality columns (IDs, timestamps with seconds/milliseconds)
    - Do not partition on boolean columns (`is_current`) — only 2 values, creates severely imbalanced partitions

Table properties can also be set per-flow:

```yaml
output:
  tableProperties:
    write.format.default: "parquet"
    commit.retry.num-retries: "4"
```

## Write operations

All writes select the appropriate strategy based on the flow's `loadMode.type`. For YAML configuration of load modes, see [Flow Configuration](../configuration/flows.md). This section covers the Iceberg-level behavior of each mode.

### Full load

Replaces all data atomically using `writeTo().overwrite(lit(true))`. This replaces all existing rows regardless of partitioning — even an empty source clears the table. The previous data is not deleted from disk until snapshot expiration runs; it remains accessible via time travel.

Iceberg normally records an `overwrite` snapshot with summary fields such as:

```json
{
  "added-records": "35",
  "deleted-records": "42",
  "total-records": "35"
}
```

Summary keys are produced by Iceberg and can vary by operation/version. Use the snapshot `operation` column together with the summary; Floe does not add an `overwritten-records` field.

### Delta (upsert)

Executes a single `MERGE INTO` statement with value-based change detection:

```sql
MERGE INTO catalog.default.orders AS target
USING _iceberg_merge_orders_2b58c1d40ee84aa5a67a891f174e0f47 AS source
ON target.order_id = source.order_id
WHEN MATCHED AND (
  NOT (source.status    <=> target.status)    OR
  NOT (source.total     <=> target.total)     OR
  NOT (source.order_date <=> target.order_date)
) THEN UPDATE SET
  target.status     = source.status,
  target.total      = source.total,
  target.order_date = source.order_date
WHEN NOT MATCHED THEN INSERT (order_id, status, total, order_date)
  VALUES (source.order_id, source.status, source.total, source.order_date)
```

The `WHEN MATCHED AND (...)` condition uses Spark SQL's null-safe equality operator `<=>` to correctly handle NULL comparisons:

- `NULL` vs `NULL` → **equal** (no update)
- `NULL` vs `'value'` → **different** (update)
- `'a'` vs `'a'` → **equal** (no update)

This means rows where no column has actually changed are skipped entirely by the MERGE engine.

Delta requires a non-empty primary key. FLOe rejects a Delta flow without one during configuration validation and the writer repeats the check before any table creation. This avoids an implicit append mode whose replay could duplicate rows. Use a different, explicitly designed ingestion path if append semantics are required.

Before a keyed MERGE, FLOe rejects NULL or duplicate source keys. It does not choose a winner for competing source events, nor does Iceberg enforce uniqueness in the existing target table. Resolve source duplicates using a deterministic sequence before writing and monitor target-key uniqueness separately.

#### Idempotency

For a keyed MERGE against an unchanged target, re-running the same complete source can leave the logical rows unchanged: value-based change detection skips equal matches. This is **not** a general exactly-once guarantee. Concurrent writes, missing tombstones, mutable inputs and replay order still require their own policy.

At the storage level, Iceberg may still create a new snapshot depending on the write mode (see copy-on-write vs merge-on-read below), but the data content is identical.

#### Copy-on-write vs merge-on-read

Iceberg supports two write strategies that affect how MERGE INTO behaves:

**Copy-on-write (default):**

- Rewrites data files selected for actual row-level changes; the amount of rewrite depends on the plan, file layout and Iceberg version
- A logically unchanged MERGE may still produce work or a snapshot: measure it rather than assuming either outcome
- Snapshot `added-records` and `deleted-records` describe files added/removed, not business rows changed
- Simpler and better for read-heavy workloads (no read-time merge overhead)

**Merge-on-read:**

- Can write delete files and new data files for row-level changes rather than rewriting every affected existing data file
- An unchanged MERGE may avoid file rewrites, but this is not an execution guarantee
- Can reduce write amplification when updates are frequent; compare total write, read and compaction costs
- Reads must apply delete files, and the overhead can grow until maintenance rewrites affected files

To enable merge-on-read for a specific flow:

```yaml
output:
  tableProperties:
    write.merge.mode: "merge-on-read"
```

For a fair comparison, collect snapshot summary fields (`added-data-files`, `deleted-data-files`, `added-delete-files`, `changed-partition-count`), bytes scanned, elapsed time and the cost of later compaction on the **same representative input**. No single summary field is a business-row change counter.

#### Partition pruning and MERGE INTO

The framework's `MERGE ON` uses the configured business key and does not add an explicit partition predicate. This can make target pruning weak, especially when the key does not imply the partition value. It does **not** prove that every partition is fully scanned or rewritten: Spark planning, Iceberg metadata pruning, dynamic pruning where available, and actual matches determine the work. Inspect `EXPLAIN`, bytes read and rewritten files on representative data.

The framework does not add partition pruning hints to the MERGE ON condition. This is a deliberate choice: automatically inferring partition predicates from the source data is fragile (requires knowing the partition expression semantics and the data's value range) and could silently skip rows that should be matched.

For frequent row-level changes, `write.merge.mode: merge-on-read` may reduce write amplification but adds delete-file work to reads and later compaction. Compare it with copy-on-write under the real workload; neither mode rewrites all untouched partitions by definition.

### SCD2 (Slowly Changing Dimension Type 2)

SCD2 keeps versioned business rows using `valid_from`, `valid_to`, and `is_current` columns, subject to any separate data-retention or deletion process. It is the most complex write mode.

For complete documentation including configuration, behavior per scenario, edge cases, query examples, and implementation details, see the dedicated [SCD2 Guide](scd2.md).

## Snapshot management

### Tagging

After a managed write creates a snapshot, if `enableSnapshotTagging` is true, the framework tags that snapshot. A no-change MERGE may create no snapshot and therefore no new tag:

```sql
ALTER TABLE catalog.default.customers CREATE TAG `batch_20260218_150000_2b58c1d40ee84aa5a67a891f174e0f47`
AS OF VERSION 4857209365014528 RETAIN 7 DAYS
```

This allows querying any historical batch by name:

```sql
SELECT * FROM catalog.default.customers VERSION AS OF 'batch_20260218_150000_2b58c1d40ee84aa5a67a891f174e0f47'
```

Tags are snapshot references and protect the referenced snapshots from expiration. FLOe now sets tag retention from `maintenance.snapshotRetentionDays` when it is positive (seven days by default). If snapshot expiration is disabled with `None`, tags have no explicit retention and must be governed separately. **Existing tags created by older FLOe versions without retention are not migrated automatically**: inspect `table.refs` and plan their removal or replacement before expecting `expire_snapshots` to reclaim their files. Never drop audit/legal-hold tags without an approved retention policy.

### Metadata capture

Each write produces an `IcebergFlowMetadata` object containing:

- `tableName`: fully qualified Iceberg table name
- `snapshotId`: numeric snapshot ID
- `snapshotTag`: batch tag string, or absent if tagging is disabled or the tag operation failed. When absent, use `snapshotId` for time travel instead.
- `parentSnapshotId`: the snapshot that existed before this write (used for time travel in [orphan detection](orphan-detection.md))
- `snapshotTimestampMs`: creation timestamp
- `recordsWritten`: number of records in the source DataFrame submitted to the write operation. For full loads, this equals the table's final row count. For delta and SCD2 modes, this is the number of source records processed, not the number of rows actually inserted or updated. Use the snapshot `summary` fields (`added-records`, `deleted-records`) for file-level write statistics.
- `manifestListLocation`: path to the manifest list file
- `summary`: Iceberg summary map (added/deleted records, file counts, etc.)

This metadata is written to the attempt diagnostics at `{metadataPath}/{pipelineId}/{logicalRunId}/{attemptId}/flows/{flowName}.json`.

### Commit identity and reconciliation

Every managed flow and derived-table write that creates a snapshot adds these properties to its Iceberg snapshot summary:

- `floe.pipeline-id`
- `floe.logical-run-id`
- `floe.attempt-id`
- `floe.target-name`
- `floe.operation-type`
- `floe.operation-id`
- `floe.logical-operation-id`
- `floe.contract-version`
- `floe.code-version`
- `floe.config-digest`
- `floe.effective-at`

`floe.operation-id` identifies the target mutation attempted by one driver invocation. `floe.logical-operation-id` remains stable for the same logical run, target and operation type across separately approved attempts. If a client loses the commit response, FLOe searches snapshot history for the attempt operation ID. Exactly one match is evidence that this attempt committed; multiple matches violate the single-commit invariant. No match is **not** proof that the write failed, and never authorizes a blind retry.

Snapshot tags are useful retention and time-travel references. Operation identities help incident inspection, but they are not uniqueness constraints, locks, fencing tokens, or a general resume protocol.

#### Interpreting snapshot summary

The snapshot summary contains file-level statistics, not row-level change counts. Key fields:

| Field | Meaning |
|-------|---------|
| `added-records` | Total records in newly written data files |
| `deleted-records` | Total records in replaced (old) data files |
| `total-records` | Total records in the table after this snapshot |
| `added-data-files` | Number of new data files written |
| `deleted-data-files` | Number of old data files replaced |
| `changed-partition-count` | Number of partitions with file changes |

!!!note
    A `deleted-records = 35, added-records = 35` on a delta run does **not** mean 35 rows were updated — it means the files containing those 35 rows were rewritten (copy-on-write). The actual number of changed rows can be 0.

## Post-attempt lifecycle

After all flows execute successfully, the remaining attempt lifecycle is:

### 1. Orphan detection

Uses time travel to find parent keys removed during this batch and resolves orphaned child records according to the FK's `onOrphan` action. See [Orphan Detection](orphan-detection.md) for details.

This runs inside ingestion and therefore before any separately scheduled maintenance that could expire snapshots needed for the comparison.

### 2. Derived tables and result assembly

Derived tables are committed with the same attempt identity properties. FLOe then returns an `IngestionResult` with a typed status and the observed snapshot references for every completed target. This is an execution report, not an atomic publication transaction. A consumer that reads mutable table heads can observe a partially completed multi-table attempt; a multi-table publication protocol must be designed separately when that guarantee is required.

!!! warning "Cascading orphan deletes"
    An `onOrphan: delete` cascade can perform additional commits after the ordinary flow write and is not a durable, exactly-once worklist. A partial multi-table cascade therefore requires the [manual orphan recovery runbook](orphan-detection.md#recovery-after-a-partial-batch-or-cascade). Do not infer that rerunning the normal pipeline will complete only the missing delete steps.

### 3. Diagnostic metadata

Per-flow diagnostic-output failures are returned in `FlowResult.warnings` and produce `SUCCEEDED_WITH_WARNINGS` when the functional work succeeded. Batch-summary and quality-metric failures do not negate commits already observed. Because these artifacts are best effort, they must not be used as the only durable evidence that a write did or did not occur.

### 4. Independently scheduled maintenance

Run maintenance from a separate application and schedule it with the external platform:

```scala
val runner = new IcebergMaintenanceRunner(spark, globalConfig.iceberg)
runner.run("floe.default.orders", globalConfig.iceberg.maintenance)
```

FLOe does not queue, claim, persist, or retry maintenance tasks. The hosting platform owns the table inventory, schedule, mutual exclusion, status, alerting, and retry policy. In particular, it must not run maintenance while a writer or incident investigation still needs the affected snapshots and files. Retrying maintenance must never rerun ingestion.

For each explicitly selected table, the runner executes the enabled maintenance operations:

| Operation | SQL | Purpose |
|-----------|-----|---------|
| Snapshot expiration | `CALL system.expire_snapshots(table, older_than)` | Removes eligible old snapshots and files exclusive to them; live refs and retention rules can protect older snapshots. |
| Data compaction | `CALL system.rewrite_data_files(table, target_size)` | Merges small files into larger ones (target: 128MB default). Improves scan performance. |
| Orphan file cleanup | `CALL system.remove_orphan_files(table, older_than)` | Removes data files not referenced by any snapshot. Cleans up after failed writes. |
| Manifest rewrite | `CALL system.rewrite_manifests(table)` | Consolidates manifest files for faster metadata operations. Disabled by default. |

!!!note "Maintenance state is external"
    The ingestion result contains no queued maintenance state. Record the maintenance application's status in the hosting platform and alert there. A maintenance failure says nothing about whether ingestion should be retried.

!!!tip "Metadata file cleanup"
    Every commit creates a new metadata JSON file in the table's `metadata/` directory (e.g. `v1.metadata.json`, `v2.metadata.json`). These files are small (KB) but accumulate over time. To enable automatic cleanup, add these table properties:

    ```yaml
    output:
      tableProperties:
        write.metadata.delete-after-commit.enabled: "true"
        write.metadata.previous-versions-max: "100"
    ```

    With these settings, Iceberg can delete older metadata files **still tracked in its metadata log** after commits; it is not a promise that exactly 100 total files remain. Check rollback, copied catalogs and recovery requirements before enabling cleanup. Untracked metadata files may require separate orphan cleanup.

#### File accumulation in the warehouse

Without snapshot expiration, superseded data files can accumulate across runs because older snapshots may still reference them. A successful expiration can remove files no longer needed by surviving snapshots; do not assume those files become orphans requiring a separate cleanup.

A typical warehouse directory after several delta runs:

```
orders/data/order_date_month=2024-01/
  00000-98-abc123.parquet   ← Run 1
  00000-106-def456.parquet  ← Run 2 (replaced Run 1 under copy-on-write)
  00000-106-ghi789.parquet  ← Run 3 (replaced Run 2)
```

The current snapshot can reference **many** files per partition. Old files that are exclusive to retained snapshots remain for time travel; `expire_snapshots` can delete files made unnecessary by expiring those snapshots. Orphan cleanup is mainly for files never committed or otherwise untracked, not the normal second step after every expiration.

To control file accumulation:

1. **Snapshot expiration** removes snapshots and their exclusively-referenced data files
2. **Orphan cleanup** removes sufficiently old, untracked files in the table location (for example files from failed writes), after a safe grace period
3. **Compaction** rewrites many small files into fewer, larger files

Do not infer a fixed bound such as "seven file versions" from seven days of snapshot retention: commits per day, compaction, tags/branches and file reuse all matter. Monitor `refs`, snapshot ages, file counts and storage bytes; expiration and orphan cleanup have different responsibilities.

## Pipeline data flow

Every flow follows the same pipeline:

```
Read -> Rename columns -> PreTransform -> Validate (new data only) -> PostTransform -> Write (Iceberg)
```

- Validation runs only on incoming data, not on data already in the table
- Merge happens atomically during the write phase via SQL
- Each Iceberg table write has ACID commit semantics; there is no distributed transaction or implicit atomic publication across the pipeline's tables

## Flow configuration examples

### Delta with FK validation and Iceberg partitioning

```yaml
name: orders
description: "Customer orders"
version: "1.0"
owner: data-team

source:
  type: file
  path: "data/orders.csv"
  format: csv
  options:
    header: "true"

schema:
  enforceSchema: true
  allowExtraColumns: false
  columns:
    - name: order_id
      type: integer
      nullable: false
    - name: customer_id
      type: integer
      nullable: false
    - name: status
      type: string
      nullable: false
    - name: total_amount
      type: double
      nullable: false
    - name: order_date
      type: date
      nullable: false

loadMode:
  type: delta

validation:
  primaryKey: [order_id]
  foreignKeys:
    - columns: [customer_id]
      references:
        flow: customers
        columns: [customer_id]
      onOrphan: warn
  rules:
    - type: domain
      column: status
      domainName: order_status
      onFailure: reject
    - type: range
      column: total_amount
      min: "0.01"
      onFailure: reject

output:
  icebergPartitions:
    - "month(order_date)"
  tableProperties:
    write.merge.mode: "merge-on-read"
```

### Full load (dimension table)

```yaml
name: customers
description: "Customer master data — full reload each batch"
version: "1.0"
owner: data-team

source:
  type: file
  path: "data/customers.csv"
  format: csv
  options:
    header: "true"

schema:
  enforceSchema: true
  allowExtraColumns: false
  columns:
    - name: customer_id
      type: integer
      nullable: false
    - name: email
      type: string
      nullable: false
    - name: country
      type: string
      nullable: false

loadMode:
  type: full

validation:
  primaryKey: [customer_id]
  foreignKeys: []
  rules:
    - type: regex
      column: email
      pattern: "^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"
      onFailure: reject

output: {}
```

### SCD2 with detect-deletes

See [SCD2 Guide](scd2.md) for the full SCD2 flow configuration example and all available options.

## Time travel queries

With snapshot tagging enabled, historical data is accessible via SQL:

```sql
-- Query a specific batch by tag
SELECT * FROM floe.default.customers
VERSION AS OF 'batch_20260218_150000_2b58c1d40ee84aa5a67a891f174e0f47'

-- Query by snapshot ID (from batch metadata JSON)
SELECT * FROM floe.default.customers
VERSION AS OF 4857209365014528

-- Compare two batches
SELECT curr.customer_id, curr.name AS current_name, prev.name AS previous_name
FROM floe.default.customers curr
FULL OUTER JOIN floe.default.customers VERSION AS OF 'batch_20260217_150000_f47f453593154f168ef7594432acada1' prev
  ON curr.customer_id = prev.customer_id
WHERE NOT (curr.name <=> prev.name)
   OR curr.customer_id IS NULL OR prev.customer_id IS NULL
```

!!!note
    Time travel requires the snapshot and its files to remain available. A seven-day expiration threshold does not mean that every older snapshot is gone: tags, branches and minimum retention can keep it; conversely an expired snapshot cannot be recovered just from its old ID. Check `table.refs` and `table.snapshots`.

## Limitations

### Writer coordination

FLOe has no lease or fencing service. The hosting platform must serialize every incompatible writer to the same targets, including other pipelines, manual jobs, orphan cleanup, maintenance, and administrative changes. Iceberg optimistic concurrency can reject conflicting commits, but it does not establish the business ordering of two valid writers and is not a substitute for ownership controls.

### No automatic column removal

Schema evolution only adds columns, never removes them. If a column is removed from the flow's schema config, it remains in the Iceberg table with NULL values for new rows. To remove a column, use `ALTER TABLE DROP COLUMN` manually.

### Maintenance is not transactional

If a maintenance operation fails mid-way (e.g., compaction fails on one table), subsequent maintenance operations for other tables may still run. There is no all-or-nothing guarantee for maintenance across tables. Each operation is independent.

### Orphan cascades are not durable operations

An `onOrphan: delete` statement is atomic for its table, but a cascade across tables is not represented as a durable per-FK worklist. If the process fails after deleting a child but before processing its descendants, stop new runs and follow the [orphan recovery runbook](orphan-detection.md#recovery-after-a-partial-batch-or-cascade).

## Related

- [Global Configuration — iceberg](../configuration/global.md#iceberg) — configuration reference
- [SCD2 Guide](scd2.md) — SCD2 write mode
- [Orphan Detection](orphan-detection.md) — post-batch FK integrity
- [Architecture: Design Decisions](../architecture/design-decisions.md) — why Iceberg, why MERGE INTO
- [Pipeline Builder](pipeline-builder.md) — custom catalog providers
- [Quality Metrics](quality-metrics.md) — per-flow quality metrics table
- [Failure Handling and Production Operations](recovery.md) — partial commits, unknown outcomes, retry policy, and incident response
