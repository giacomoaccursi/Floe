# Iceberg Integration

## Overview

The framework uses Apache Iceberg as its storage layer. All write operations are atomic MERGE INTO statements, every batch produces a tagged snapshot for time travel, and post-batch maintenance runs automatically.

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
    Spark 3.5 enables AQE by default (`spark.sql.adaptive.enabled = true`). AQE optimizes MERGE INTO and DAG joins at runtime by coalescing small partitions, converting to broadcast joins, and handling data skew automatically. Do not disable it — the framework relies on these optimizations for efficient execution.

### Java 18+ compatibility

On Java 18 or newer, Hadoop's `UserGroupInformation` requires the security manager to be explicitly allowed. Add this JVM option:

```
-Djava.security.manager=allow
```

In SBT:

```scala
run / javaOptions += "-Djava.security.manager=allow"
```

Without this flag, Spark fails at startup with `UnsupportedOperationException: getSubject is supported only if a security manager is allowed`.

## Configuring Iceberg

The `iceberg` block in `global.yaml` is required:

```yaml
iceberg:
  catalogType: "hadoop"
  catalogName: "spark_catalog"
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
| `catalogName` | `spark_catalog` | Name used in SQL queries (`catalog.namespace.table`) |
| `namespace` | `default` | Iceberg namespace for tables |
| `warehouse` | *required* | Path to the Iceberg warehouse directory |
| `fileFormat` | `parquet` | Default data file format |
| `enableSnapshotTagging` | `true` | Tag each batch snapshot for time travel by batch ID |
| `maintenance.*` | see below | Post-batch maintenance settings |

### Maintenance settings

| Field | Default | Description |
|-------|---------|-------------|
| `snapshotRetentionDays` | `7` | Days to retain snapshots. Remove to disable expiration. |
| `targetFileSizeMb` | `128` | Target file size after compaction. Remove to disable. |
| `orphanRetentionMinutes` | `1440` | Grace period before orphan files are removed (min 1440). Remove to disable. |
| `enableManifestRewrite` | `false` | Rewrite manifest files for scan optimization |

!!!warning "Orphan cleanup minimum retention"
    Iceberg enforces a **minimum retention of 24 hours** (1440 minutes) for orphan file cleanup. Values below this are automatically clamped with a warning. This prevents accidental data corruption from concurrent operations.

## Architecture

### Catalog provider

The catalog system is pluggable. The `CatalogProvider` trait defines three methods: `catalogType`, `configureCatalog`, and `validateConfig`. The built-in hadoop provider configures SparkSession with:

```
spark.sql.catalog.{name}          = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.{name}.type     = hadoop
spark.sql.catalog.{name}.warehouse = {path}
spark.sql.extensions               = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

The framework maps the `catalogType` string to the right provider. Adding a new catalog type (Hive, REST, Nessie) means implementing the `CatalogProvider` trait and registering it on the builder. See [Pipeline Builder — Custom catalog providers](pipeline-builder.md#custom-catalog-providers) for details.

### Table naming

Every flow maps to a single Iceberg table with the convention:

```
{catalogName}.{namespace}.{flowName}
```

For example, a flow named `customers` with catalog `spark_catalog` and namespace `default` becomes `spark_catalog.default.customers`. The namespace is configurable via `iceberg.namespace` in `global.yaml` (defaults to `default`).

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
  - name: "status"
    type: "string"

# After — just add the new column
columns:
  - name: "order_id"
    type: "integer"
  - name: "status"
    type: "string"
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

- Files written before the change: no partition metadata, always scanned
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

The `sort` strategy rewrites all data files, reorganizing rows according to the new partition layout. This is different from the default `binpack` strategy used by the framework's automatic compaction, which only merges small files into larger ones without reordering data. After running the sort compaction once, switch back to the normal batch flow — subsequent runs will write correctly partitioned files and `binpack` compaction is sufficient from that point on.

For tables with frequent queries on multiple columns, Iceberg also supports a `zorder` strategy that optimizes file layout for multi-column pruning:

```sql
CALL catalog.system.rewrite_data_files(
  table => 'catalog.default.orders',
  strategy => 'zorder',
  sort_order => 'order_date, customer_id'
)
```

Both `sort` and `zorder` are one-time operations — run them manually when needed, not as part of the regular batch cycle.

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

The snapshot summary includes `overwritten-records` to distinguish full reloads from delta writes:

```json
{
  "added-records": "35",
  "deleted-records": "35",
  "total-records": "35"
}
```

### Delta (upsert)

Executes a single `MERGE INTO` statement with value-based change detection:

```sql
MERGE INTO catalog.default.orders AS target
USING _source AS source
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

The `WHEN MATCHED AND (...)` condition uses Iceberg's null-safe equality operator `<=>`, which correctly handles NULL comparisons:

- `NULL <=> NULL` → `true` (equal, no update)
- `NULL <=> 'value'` → `false` (different, update)
- `'a' <=> 'a'` → `true` (equal, no update)

This means rows where no column has actually changed are skipped entirely by the MERGE engine.

If no primary key is defined, the write degrades to an append.

#### Idempotency

Re-running the same batch with the same source data produces no logical changes. The change detection compares every non-PK column and finds no differences, so neither the `WHEN MATCHED` (update) nor `WHEN NOT MATCHED` (insert) clause fires for any row.

At the storage level, Iceberg may still create a new snapshot depending on the write mode (see copy-on-write vs merge-on-read below), but the data content is identical.

#### Copy-on-write vs merge-on-read

Iceberg supports two write strategies that affect how MERGE INTO behaves:

**Copy-on-write (default):**

- Rewrites entire data files for any file containing a matched row, even if the row was not actually updated
- Even fully idempotent runs (zero changes) rewrite all scanned files and create a new snapshot
- Snapshot summary shows `added-records = N, deleted-records = N` — this reflects file-level rewrites, not row-level changes
- Simpler and better for read-heavy workloads (no read-time merge overhead)

**Merge-on-read:**

- Writes only delete files (position deletes) for changed rows
- Idempotent runs produce no file rewrites: `changed-partition-count = 0`
- Better for write-heavy workloads or frequent idempotent runs
- Slight read overhead: queries must merge delete files at scan time

To enable merge-on-read for a specific flow:

```yaml
output:
  tableProperties:
    write.merge.mode: "merge-on-read"
```

Comparison from actual test runs (idempotent batch, 35 records across 3 partitions):

| Metric | Copy-on-write | Merge-on-read |
|--------|--------------|---------------|
| `added-data-files` | 3 | 0 |
| `deleted-data-files` | 3 | 0 |
| `changed-partition-count` | 3 | 0 |
| File I/O | Full rewrite | None |

#### Partition pruning and MERGE INTO

The `MERGE INTO` statement scans all partitions of the target table to match rows, regardless of the source data's partition range. Even if the source only contains records for January 2024, all partitions (January, February, March...) are scanned and potentially rewritten under copy-on-write.

The framework does not add partition pruning hints to the MERGE ON condition. This is a deliberate choice: automatically inferring partition predicates from the source data is fragile (requires knowing the partition expression semantics and the data's value range) and could silently skip rows that should be matched.

For large partitioned tables, merge-on-read (`write.merge.mode: merge-on-read`) significantly reduces the I/O impact by avoiding rewrites of untouched partitions.

### SCD2 (Slowly Changing Dimension Type 2)

SCD2 maintains full history of every record using versioned rows with `valid_from`, `valid_to`, and `is_current` columns. It is the most complex write mode.

For complete documentation including configuration, behavior per scenario, edge cases, query examples, and implementation details, see the dedicated [SCD2 Guide](scd2.md).

## Snapshot management

### Tagging

After every write, if `enableSnapshotTagging` is true, the framework tags the new snapshot:

```sql
ALTER TABLE catalog.default.customers SET TAG `batch_20260218_150000`
```

This allows querying any historical batch by name:

```sql
SELECT * FROM catalog.default.customers VERSION AS OF 'batch_20260218_150000'
```

Tags are retained as long as the underlying snapshot exists. When a snapshot is expired by maintenance, its tag is also removed.

### Metadata capture

Each write produces an `IcebergFlowMetadata` object containing:

- `tableName`: fully qualified Iceberg table name
- `snapshotId`: numeric snapshot ID
- `snapshotTag`: batch tag string, or absent if tagging is disabled or the tag operation failed. When absent, use `snapshotId` for time travel instead.
- `parentSnapshotId`: the snapshot that existed before this write (used for time travel in [orphan detection](orphan-detection.md))
- `snapshotTimestampMs`: creation timestamp
- `recordsWritten`: record count
- `manifestListLocation`: path to the manifest list file
- `summary`: Iceberg summary map (added/deleted records, file counts, etc.)

This metadata is written to the batch metadata JSON at `{metadataPath}/{batchId}/flows/{flowName}.json`.

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

## Post-batch lifecycle

After all flows execute successfully, three things happen in order:

### 1. Orphan detection

Uses time travel to find parent keys removed during this batch and resolves orphaned child records according to the FK's `onOrphan` action. See [Orphan Detection](orphan-detection.md) for details.

This runs **before** maintenance because maintenance may expire the snapshots needed for time travel comparison.

### 2. Batch metadata write

The batch metadata JSON includes Iceberg snapshot details for every flow and any orphan reports generated in step 1.

### 3. Table maintenance

For each flow's table, the framework runs the enabled maintenance operations:

| Operation | SQL | Purpose |
|-----------|-----|---------|
| Snapshot expiration | `CALL system.expire_snapshots(table, older_than)` | Removes snapshots older than the retention period. Frees metadata and data files no longer referenced by any surviving snapshot. |
| Data compaction | `CALL system.rewrite_data_files(table, target_size)` | Merges small files into larger ones (target: 128MB default). Improves scan performance. |
| Orphan file cleanup | `CALL system.remove_orphan_files(table, older_than)` | Removes data files not referenced by any snapshot. Cleans up after failed writes. |
| Manifest rewrite | `CALL system.rewrite_manifests(table)` | Consolidates manifest files for faster metadata operations. Disabled by default. |

!!!note "Maintenance is best-effort"
    A maintenance failure causes the individual operation to fail but does not abort the batch. The batch result still reports SUCCESS if all flow writes completed. However, subsequent maintenance operations in the same batch may be skipped if the failure propagates.

!!!tip "Metadata file cleanup"
    Every commit creates a new metadata JSON file in the table's `metadata/` directory (e.g. `v1.metadata.json`, `v2.metadata.json`). These files are small (KB) but accumulate over time. To enable automatic cleanup, add these table properties:

    ```yaml
    output:
      tableProperties:
        write.metadata.delete-after-commit.enabled: "true"
        write.metadata.previous-versions-max: "100"
    ```

    With these settings, Iceberg deletes old metadata files after each commit, keeping only the last 100 versions. Recommended for production environments with frequent batch runs.

#### File accumulation in the warehouse

Without maintenance enabled, Iceberg data files accumulate across runs. Each delta or full load write creates new data files but does not delete old ones — they remain on disk referenced by previous snapshots (or as orphans after snapshot expiration).

A typical warehouse directory after several delta runs:

```
orders/data/order_date_month=2024-01/
  00000-98-abc123.parquet   ← Run 1
  00000-106-def456.parquet  ← Run 2 (replaced Run 1 under copy-on-write)
  00000-106-ghi789.parquet  ← Run 3 (replaced Run 2)
```

Only the latest file per partition is referenced by the current snapshot. The older files are kept for time travel (if their snapshots still exist) or are orphans (if their snapshots were expired).

To control file accumulation:

1. **Snapshot expiration** removes snapshots and their exclusively-referenced data files
2. **Orphan cleanup** removes data files that no surviving snapshot references
3. **Compaction** rewrites many small files into fewer, larger files

In production with daily runs and `snapshotRetentionDays: 7`, at most ~7 versions of each data file coexist. After expiration, orphan cleanup removes the old files.

## Pipeline data flow

Every flow follows the same pipeline:

```
Read -> PreTransform -> Validate (new data only) -> PostTransform -> Write (MERGE INTO / Iceberg)
```

- Validation runs only on incoming data, not on data already in the table
- Merge happens atomically during the write phase via SQL
- Iceberg guarantees ACID semantics

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
SELECT * FROM spark_catalog.default.customers
VERSION AS OF 'batch_20260218_150000'

-- Query by snapshot ID (from batch metadata JSON)
SELECT * FROM spark_catalog.default.customers
VERSION AS OF 4857209365014528

-- Compare two batches
SELECT curr.customer_id, curr.name AS current_name, prev.name AS previous_name
FROM spark_catalog.default.customers curr
FULL OUTER JOIN spark_catalog.default.customers VERSION AS OF 'batch_20260217_150000' prev
  ON curr.customer_id = prev.customer_id
WHERE curr.name != prev.name OR curr.customer_id IS NULL OR prev.customer_id IS NULL
```

!!!note
    Time travel queries only work for snapshots that have not been expired by maintenance. If `snapshotRetentionDays: 7`, batches older than 7 days are no longer accessible via time travel.

## Limitations

### Single writer per table

The framework assumes a single writer per table per batch. Concurrent writes to the same table can cause commit conflicts. In multi-pipeline environments, ensure flows that write to the same table are serialized.

### No automatic column removal

Schema evolution only adds columns, never removes them. If a column is removed from the flow's schema config, it remains in the Iceberg table with NULL values for new rows. To remove a column, use `ALTER TABLE DROP COLUMN` manually.

### Maintenance is not transactional

If a maintenance operation fails mid-way (e.g., compaction fails on one table), subsequent maintenance operations for other tables may still run. There is no all-or-nothing guarantee for maintenance across tables. Each operation is independent.

## Related

- [Global Configuration — iceberg](../configuration/global.md#iceberg) — configuration reference
- [SCD2 Guide](scd2.md) — SCD2 write mode
- [Orphan Detection](orphan-detection.md) — post-batch FK integrity
- [Architecture: Design Decisions](../architecture/design-decisions.md) — why Iceberg, why MERGE INTO
- [Pipeline Builder](pipeline-builder.md) — custom catalog providers
- [Quality Metrics](quality-metrics.md) — per-flow quality metrics table
