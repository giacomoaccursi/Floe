# Apache Iceberg Integration

## Overview

The framework optionally integrates with Apache Iceberg to replace the default Parquet-based storage. When enabled, all write operations become atomic MERGE INTO statements, every batch produces a tagged snapshot for time travel, and post-batch maintenance runs automatically.

Iceberg is entirely opt-in: if the `iceberg` section is absent from `global.yaml`, the framework falls back to Parquet files with in-memory merge logic. No code changes are needed to switch between the two modes.

## Enabling Iceberg

Add an `iceberg` block to `global.yaml`:

```yaml
iceberg:
  catalog-type: "hadoop"
  catalog-name: "spark_catalog"
  warehouse: "warehouse/iceberg"
  file-format: "parquet"
  format-version: 2
  enable-snapshot-tagging: true
  maintenance:
    enable-snapshot-expiration: true
    snapshot-retention-days: 7
    enable-compaction: true
    target-file-size-mb: 128
    enable-orphan-cleanup: true
    orphan-retention-minutes: 60
    enable-manifest-rewrite: false
```

At startup, the pipeline validates the config and configures the SparkSession with the Iceberg catalog. If validation fails, execution stops immediately (fail-fast).

### Configuration reference

| Field | Default | Description |
|-------|---------|-------------|
| `catalog-type` | `hadoop` | Iceberg catalog implementation. Currently only `hadoop` is supported. |
| `catalog-name` | `spark_catalog` | Name used in SQL queries (`catalog.default.table`). |
| `warehouse` | *required* | Path to the Iceberg warehouse directory. |
| `file-format` | `parquet` | Default data file format. |
| `format-version` | `2` | Iceberg format version. Version 2 is required for row-level operations (MERGE INTO, DELETE). |
| `enable-snapshot-tagging` | `true` | Tag each batch snapshot for time travel by batch ID. |
| `maintenance.*` | see below | Post-batch maintenance settings. |

### Maintenance settings

| Field | Default | Description |
|-------|---------|-------------|
| `enable-snapshot-expiration` | `true` | Expire snapshots older than the retention period. |
| `snapshot-retention-days` | `7` | Days to retain snapshots before expiration. |
| `enable-compaction` | `true` | Compact small data files into larger ones. |
| `target-file-size-mb` | `128` | Target file size after compaction. |
| `enable-orphan-cleanup` | `true` | Remove orphaned files left by failed operations. |
| `orphan-retention-minutes` | `60` | Grace period before orphan files become eligible for cleanup. |
| `enable-manifest-rewrite` | `false` | Rewrite manifest files for scan optimization. |

## Architecture

### Catalog provider

The catalog system is pluggable. `ICatalogProvider` is a trait with three methods: `catalogType`, `configureCatalog`, and `validateConfig`. The only implementation today is `HadoopCatalogProvider`, which configures SparkSession with:

```
spark.sql.catalog.{name}          = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.{name}.type     = hadoop
spark.sql.catalog.{name}.warehouse = {path}
spark.sql.extensions               = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

`CatalogFactory` maps the `catalog-type` string to the right provider. Adding a new catalog type (Hive, REST, Nessie, Glue) means implementing the trait and registering it in the factory.

### Table naming

Every flow maps to a single Iceberg table with the convention:

```
{catalogName}.default.{flowName}
```

For example, a flow named `customers` with catalog `spark_catalog` becomes `spark_catalog.default.customers`.

### Table creation and schema

Tables are created on first write with `CREATE TABLE IF NOT EXISTS`. The schema is derived from the flow's `SchemaConfig` columns plus any system columns added by the load mode (e.g., `valid_from`, `valid_to`, `is_current` for SCD2).

Schema evolution is handled by Iceberg transparently: new columns are added via ALTER TABLE if the incoming schema has columns not present in the table.

### Table configuration updates

Every run, `createOrUpdateTable` compares the current table state against the flow config and applies any differences:

- **Table properties**: reads current properties via `SHOW TBLPROPERTIES` and applies only new or changed entries via `ALTER TABLE SET TBLPROPERTIES`. Existing properties not mentioned in the config are left untouched.
- **Partition spec**: attempts `ALTER TABLE ADD PARTITION FIELD` for each configured partition. If the field already exists, the operation is silently skipped.

This means adding `icebergPartitions` or `tableProperties` to an existing flow config takes effect at the next run without manual intervention.

#### Partition spec on tables with existing data

Adding a partition field to a table that already contains data does **not** rewrite existing files. Iceberg applies the new spec only to files written after the change. The result is a mixed layout:

- Files written before the change: no partition metadata, always scanned
- Files written after the change: partitioned, eligible for pruning

The framework logs a `WARN` message whenever a new partition field is applied to an existing table, advising that partition pruning will be partial until all pre-existing data is replaced by new writes.

To apply the new partition layout to all existing data, perform a one-time full reload: temporarily set `loadMode.type: full`, run the pipeline with a complete source dataset, then revert to `delta`. This rewrites all files under the new partition spec. For large tables, the equivalent Iceberg maintenance procedure is preferable:

```sql
CALL catalog.system.rewrite_data_files(
  table => 'catalog.default.orders',
  strategy => 'sort',
  sort_order => 'order_date ASC'
)
```

### Partitioning and sort order

Partitions are configured per-flow in the output section:

```yaml
output:
  iceberg-partitions:
    - "month(order_date)"
    - "bucket(16, customer_id)"
  sort-order:
    - "order_date"
    - "customer_id"
```

Supported partition transforms: `year()`, `month()`, `day()`, `hour()`, `bucket(n, col)`, `truncate(len, col)`, and identity (bare column name).

Sort order is applied with `WRITE ORDERED BY`, which controls data layout within files for better scan performance without affecting query semantics.

Table properties can also be set per-flow:

```yaml
output:
  table-properties:
    write.format.default: "parquet"
    commit.retry.num-retries: "4"
```

## Write operations

All writes go through `IcebergTableWriter`, which selects the appropriate strategy based on the flow's load mode.

### Full load

Replaces all data atomically using `writeTo().overwritePartitions()`. The previous data is not deleted from disk until snapshot expiration runs; it remains accessible via time travel.

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

This means rows where no column has actually changed are skipped entirely by the MERGE engine. Re-running the same batch twice is safe and produces no logical changes.

#### Copy-on-write and snapshot statistics

Iceberg's default write strategy is copy-on-write. Even when the MERGE skips all updates (change condition false for every matched row), Iceberg may still rewrite the data files that were scanned and create a new snapshot. The `added-records` and `deleted-records` values in the snapshot summary reflect file-level statistics, not row-level change counts — a file rewrite shows as `deleted-records = N, added-records = N` even if no row actually changed.

This is expected behavior. The change detection guarantees **correctness** (no row is overwritten unless it actually changed) but does not prevent file rewrites at the storage layer. The practical performance benefit is visible when only a subset of rows changes: the MERGE generates fewer file rewrites than an unconditional update would, especially on partitioned tables where only touched partition files are rewritten.

If no primary key is defined, the write degrades to an append.

### SCD2 (Slowly Changing Dimension Type 2)

SCD2 is the most complex write mode. It maintains full history of every record.

#### Columns

| Column | Purpose |
|--------|---------|
| `valid_from` | Timestamp when this version became effective |
| `valid_to` | Timestamp when this version was superseded (`NULL` = still current) |
| `is_current` | `true` for the latest version of each record |
| `is_active` | `true` if the record still exists in the source (only with `detect-deletes`) |

Column names are configurable in `load-mode`:

```yaml
load-mode:
  type: scd2
  valid-from-column: valid_from
  valid-to-column: valid_to
  is-current-column: is_current
  is-active-column: is_active
  compare-columns: [name, email, status]
  detect-deletes: true
```

#### First load

All records are inserted with `valid_from = now`, `valid_to = NULL`, `is_current = true`, `is_active = true`.

#### Subsequent loads: the NULL merge-key trick

A naive SCD2 implementation would require two separate operations: a MERGE to close changed records, then an INSERT for new versions. This is not atomic — a failure between the two steps leaves the table in an inconsistent state.

The framework solves this with a single atomic MERGE INTO by using a staging view that contains each source record twice when it has changed:

1. **All source records** with merge key = primary key (for matching against existing records)
2. **Changed records only** with merge key = NULL (forces NOT MATCHED, triggering an INSERT of the new version)

The staged source is built as:

```sql
SELECT src.*, src.pk AS _mk_pk FROM source src
UNION ALL
SELECT src.*, CAST(NULL AS type) AS _mk_pk FROM source src
JOIN target tgt ON src.pk = tgt.pk AND tgt.is_current = true
WHERE NOT (src.col <=> tgt.col) OR ...   -- null-safe change detection
```

Then a single MERGE runs with three clauses:

```sql
MERGE INTO target
USING staged_source AS source
ON target.pk = source._mk_pk AND target.is_current = true

-- Close old version of changed record
WHEN MATCHED AND (change condition) THEN UPDATE SET
  valid_to = current_timestamp(),
  is_current = false

-- Insert new version (NULL _mk forces NOT MATCHED) or brand new record
WHEN NOT MATCHED THEN INSERT (cols) VALUES (source.cols, now(), NULL, true)

-- Soft-delete records missing from source (only if detectDeletes=true)
WHEN NOT MATCHED BY SOURCE AND target.is_current = true THEN UPDATE SET
  valid_to = current_timestamp(),
  is_current = false,
  is_active = false
```

The `WHEN NOT MATCHED BY SOURCE` clause (Iceberg 1.2+ / Spark 3.4+) catches records in the target that have no match in the source at all. Combined with `target.is_current = true` in the ON clause, it only affects current versions and leaves historical records untouched.

#### is_current vs is_active

These two booleans encode different information:

| is_current | is_active | Meaning |
|------------|-----------|---------|
| true | true | Latest version, record exists in source |
| false | true | Historical version, superseded by a newer one |
| false | false | Record was deleted from source (soft-delete) |
| true | false | Not possible under normal operation |

`is_current` tracks versioning. `is_active` tracks logical deletion. Separating them allows queries like "show me the last known state of all deleted customers" (`is_current = false AND is_active = false AND valid_to IS NOT NULL`).

`is_active` is only added when `detect-deletes: true` or when `is-active-column` is explicitly configured.

## Snapshot management

### Tagging

After every write, if `enable-snapshot-tagging` is true, the framework tags the new snapshot:

```sql
ALTER TABLE catalog.default.customers SET TAG `batch_20260218_150000`
RETAIN 7 DAYS
```

This allows querying any historical batch by name:

```sql
SELECT * FROM catalog.default.customers VERSION AS OF 'batch_20260218_150000'
```

### Metadata capture

Each write produces an `IcebergFlowMetadata` object containing:

- `tableName`: fully qualified Iceberg table name
- `snapshotId`: numeric snapshot ID
- `snapshotTag`: batch tag string
- `parentSnapshotId`: the snapshot that existed before this write (used for time travel in orphan detection)
- `snapshotTimestampMs`: creation timestamp
- `recordsWritten`: record count
- `manifestListLocation`: path to the manifest list file
- `summary`: Iceberg summary map (added/deleted records, file counts, etc.)

This metadata is written to the batch metadata JSON and to per-flow metadata files for audit trails.

### Rollback

The `IcebergTableManager` exposes a `rollbackToSnapshot` method that can revert a table to a previous snapshot. This is not called automatically by the framework but is available for manual recovery.

## Post-batch lifecycle

After all flows execute successfully, three things happen in order:

### 1. Orphan detection

Described in detail in [orphan-detection.md](orphan-detection.md). Uses time travel to find parent keys removed during this batch and resolves orphaned child records according to the FK's `on-orphan` action.

This runs **before** maintenance because maintenance may expire the snapshots needed for time travel comparison.

### 2. Batch metadata write

The batch metadata JSON includes Iceberg snapshot details for every flow and any orphan reports generated in step 1.

### 3. Table maintenance

For each flow's table, the framework runs the enabled maintenance operations:

| Operation | SQL | Purpose |
|-----------|-----|---------|
| Snapshot expiration | `CALL system.expire_snapshots(table, older_than)` | Removes snapshots older than the retention period. Frees metadata and data files no longer referenced. |
| Data compaction | `CALL system.rewrite_data_files(table, target_size)` | Merges small files into larger ones (target: 128MB default). Improves scan performance. |
| Orphan file cleanup | `CALL system.remove_orphan_files(table, older_than)` | Removes data files not referenced by any snapshot. Cleans up after failed writes. |
| Manifest rewrite | `CALL system.rewrite_manifests(table)` | Consolidates manifest files for faster metadata operations. Disabled by default. |

Each operation is wrapped in a try/catch: a maintenance failure is logged as a warning but does not fail the batch.

## Pipeline differences: Iceberg vs Parquet

The two storage backends result in different pipeline shapes:

### With Iceberg

```
Read -> PreTransform -> Validate (new data only) -> PostTransform -> Write (MERGE INTO)
```

- Validation runs only on incoming data, not on data already in the table
- Merge happens atomically during the write phase via SQL
- Iceberg guarantees ACID semantics

### Without Iceberg (Parquet fallback)

```
Read -> PreTransform -> Merge (in-memory) -> Validate (merged data) -> PostTransform -> Write
```

- Merge runs in-memory before validation using `DeltaMerger` implementations
- Validation runs on the full merged dataset
- Writes are `SaveMode.Overwrite` to Parquet
- No built-in atomicity beyond Spark's output committer

## Flow configuration example

### SCD2 with detect-deletes and Iceberg partitioning

```yaml
name: customers
description: "Customer master data with full history"
version: "1.0"
owner: data-team

source:
  type: file
  path: "data/customers.csv"
  format: csv
  options:
    header: "true"

schema:
  enforce-schema: true
  allow-extra-columns: false
  columns:
    - name: customer_id
      type: integer
      nullable: false
      description: "Unique customer ID"
    - name: name
      type: string
      nullable: false
      description: "Customer name"
    - name: email
      type: string
      nullable: true
      description: "Email address"
    - name: status
      type: string
      nullable: false
      description: "Account status"

load-mode:
  type: scd2
  valid-from-column: valid_from
  valid-to-column: valid_to
  is-current-column: is_current
  is-active-column: is_active
  compare-columns: [name, email, status]
  detect-deletes: true

validation:
  primary-key: [customer_id]
  foreign-keys: []
  rules:
    - type: not_null
      column: name
    - type: regex
      column: email
      pattern: "^[\\w.+-]+@[\\w-]+\\.[a-zA-Z]{2,}$"
      on-failure: warn

output:
  format: parquet
  iceberg-partitions:
    - "year(valid_from)"
  sort-order:
    - customer_id
  table-properties:
    format-version: "2"
```

### Delta with FK and orphan action

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
  enforce-schema: true
  allow-extra-columns: false
  columns:
    - name: order_id
      type: integer
      nullable: false
      description: "Unique order ID"
    - name: customer_id
      type: integer
      nullable: false
      description: "Customer reference"
    - name: total
      type: decimal(10,2)
      nullable: false
      description: "Order total"
    - name: order_date
      type: date
      nullable: false
      description: "Date of the order"

load-mode:
  type: delta

validation:
  primary-key: [order_id]
  foreign-keys:
    - name: fk_customer
      column: customer_id
      references:
        flow: customers
        column: customer_id
      on-orphan: delete
  rules:
    - type: range
      column: total
      min: "0"

output:
  format: parquet
  iceberg-partitions:
    - "month(order_date)"
    - "bucket(16, customer_id)"
  sort-order:
    - order_date
    - customer_id
```

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

## Design decisions

**Why MERGE INTO instead of DataFrame API**: Iceberg's MERGE INTO is a single atomic SQL operation. The DataFrame API would require reading, transforming, and writing in separate steps with no transactional guarantee between them.

**Why value-based change detection instead of a timestamp column**: An `update-timestamp-column` approach is fragile — it assumes the source always provides a reliable, monotonically increasing timestamp, and silently overwrites data when the timestamp is missing or stale. Value-based change detection using `<=>` makes no assumptions about the source: a row is updated if and only if at least one non-key column actually differs. This makes delta loads idempotent by default — re-running the same batch twice produces no writes and no data corruption.

**Why the NULL merge-key trick for SCD2**: A single MERGE with three clauses (MATCHED, NOT MATCHED, NOT MATCHED BY SOURCE) is atomic. The alternative — a MERGE to close records followed by an INSERT for new versions — has a window between the two operations where the table is inconsistent. The NULL merge key forces Iceberg to treat changed records as both a match (to close the old version) and a non-match (to insert the new version) in one pass.

**Why maintenance runs after orphan detection**: Snapshot expiration removes old snapshots. Orphan detection needs the previous snapshot for time travel. If maintenance ran first, it could expire the snapshot that orphan detection needs. Running orphan detection first guarantees the previous snapshot is still available.

**Why Iceberg is optional**: Not every deployment has Iceberg infrastructure. The framework degrades gracefully to Parquet with in-memory merge, keeping the same configuration model and validation pipeline. Switching to Iceberg later requires only adding the `iceberg` block to `global.yaml`.

**Why partition spec changes do not rewrite existing data**: Rewriting all existing files on a config change would be an unbounded, blocking operation — a table with years of history could take hours. The framework applies the new spec to future writes only and warns the operator. The decision to repartition existing data is an explicit operational action, not an automatic side effect of a config change.
