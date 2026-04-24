# SCD2 — Slowly Changing Dimension Type 2

## What is SCD2

SCD2 is a load strategy that maintains the **complete history** of every record. When an attribute changes, the previous version is closed (receives an end-of-validity date) and a new one is inserted. No data is ever deleted or overwritten: all versions remain in the table and are queryable.

Example: a customer moves from tier SILVER to GOLD. After loading, the table contains:

| customer_id | tier | valid_from | valid_to | is_current |
|-------------|------|------------|----------|------------|
| 1 | SILVER | 2026-03-26 10:00 | 2026-03-27 14:00 | false |
| 1 | GOLD | 2026-03-27 14:00 | NULL | true |

The SILVER row is not lost — it's a historical version queryable via time travel or by filtering on `is_current = false`.

## Configuration

### Minimal configuration

```yaml
loadMode:
  type: "scd2"
  validFromColumn: "valid_from"
  validToColumn: "valid_to"
  isCurrentColumn: "is_current"
  compareColumns:
    - "tier"
    - "credit_limit"
```

### Full configuration (with detect deletes)

```yaml
loadMode:
  type: "scd2"
  validFromColumn: "valid_from"
  validToColumn: "valid_to"
  isCurrentColumn: "is_current"
  isActiveColumn: "is_active"
  compareColumns:
    - "tier"
    - "credit_limit"
  detectDeletes: true
```

### Parameters

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `validFromColumn` | no | `valid_from` | Timestamp column: when this version became valid |
| `validToColumn` | no | `valid_to` | Timestamp column: when this version was superseded. `NULL` = current version |
| `isCurrentColumn` | no | `is_current` | Boolean column: `true` only for the most recent version of each record |
| `compareColumns` | yes | — | List of columns to compare for change detection. Only these columns are checked: if other unlisted columns change, the record is not considered modified |
| `detectDeletes` | no | `false` | If `true`, records that disappear from the source are marked as deleted (soft-delete) |
| `isActiveColumn` | no | `is_active` | Boolean column for soft-delete. Used only when `detectDeletes: true` |

### Primary key constraint

Primary key columns **must be non-nullable**. The framework validates this constraint at configuration load time and fails with an explicit error if not met. The reason is technical: SCD2 uses `NULL` as a sentinel value in merge keys during the MERGE INTO (see [How it works internally](#how-it-works-internally)).

### System columns

The SCD2 columns (`valid_from`, `valid_to`, `is_current`, `is_active`) **should not be declared in the flow schema** — the framework adds them automatically to the Iceberg table. Only declare business columns in the schema:

```yaml
schema:
  columns:
    - name: "customer_id"
      type: "integer"
      nullable: false
    - name: "tier"
      type: "string"
      nullable: false
    - name: "credit_limit"
      type: "double"
      nullable: false
```

## Behavior per scenario

### First load

All records are inserted with:

- `valid_from = current_timestamp()`
- `valid_to = NULL`
- `is_current = true`
- `is_active = true` (if `detectDeletes: true`)

### Modified record

A record is considered "modified" when at least one of the `compareColumns` has a different value compared to the current version in the table. The comparison is null-safe: `NULL` equals `NULL`, but `NULL` differs from any value.

When a modification is detected:

1. The current version is **closed**: `valid_to = now`, `is_current = false`
2. A new version is **inserted**: `valid_from = now`, `valid_to = NULL`, `is_current = true`

Both operations happen in a single atomic transaction.

### Unchanged record

If none of the `compareColumns` have changed, the record is not touched. No new version, no update.

### New record

A record with a primary key never seen in the table is inserted as the first version: `valid_from = now`, `valid_to = NULL`, `is_current = true`.

### Record deleted from source (soft-delete)

**Only if `detectDeletes: true`.** When a record present in the table (with `is_current = true`) does not appear in the source, it is marked as deleted:

- `valid_to = now`
- `is_current = false`
- `is_active = false`

The record is not physically deleted — it remains in the table as a queryable historical version. The difference from a version closed due to modification is the `is_active = false` flag.

**If `detectDeletes: false`** (default), records absent from the source are simply ignored. They remain in the table with `is_current = true` as if they were still active.

### Reactivated record

A previously soft-deleted record (`is_active = false`) can reappear in the source. In this case, a new version is inserted with `is_current = true` and `is_active = true`. The soft-deleted version remains in the history.

Example after delete and reactivation with tier change:

| customer_id | tier | valid_to | is_current | is_active |
|-------------|------|----------|------------|-----------|
| 5 | GOLD | 2026-03-26 21:25 | false | false |
| 5 | PLATINUM | NULL | true | true |

### Idempotency

Re-running the same batch with the same source data produces no new versions. The change detection compares every column in `compareColumns` and finds no differences → no action. This holds for consecutive runs without data changes.

With copy-on-write (default), Iceberg still rewrites physical files even if no row changes logically. Snapshot statistics will show `added-records = N, deleted-records = N` — this is a file-level rewrite effect, not an indicator of actual changes. To avoid physical rewrites on idempotent runs, configure `write.merge.mode: merge-on-read` in `tableProperties`.

### Composite primary key

SCD2 supports multi-column primary keys. The framework creates a merge key for each PK column:

```yaml
validation:
  primaryKey:
    - "country_code"
    - "customer_id"
```

The staging view will contain `_mk_country_code` and `_mk_customer_id`, both NULL for records in part 2 of the UNION. The MERGE matches on both columns:

```sql
ON target.country_code = source._mk_country_code
   AND target.customer_id = source._mk_customer_id
   AND target.is_current = true
```

All columns in the composite PK must be non-nullable (same constraint as single PK).

### Duplicate records in source

If the source contains two rows with the same primary key and different values, the behavior depends on Spark's processing order and is non-deterministic. The framework does not deduplicate the source before the MERGE.

!!!tip
    Ensure primary key uniqueness in the source. If not possible, add a custom validation rule or a pre-validation transformation that selects the most recent row.

### Empty source

If the source contains no records:

- **With `detectDeletes: false`**: no changes to the table.
- **With `detectDeletes: true`**: **all current records are soft-deleted** (`is_active = false`). This is semantically correct (the source declares that no records exist anymore), but could be destructive if the source is empty by mistake (e.g., file not uploaded).

!!!warning "Mitigation"
    The framework validates the `maxRejectionRate` configured in `global.yaml`. If all records are rejected (100% rejection rate), the batch stops before the MERGE. However, an empty source (0 records) has no records to reject and passes validation. An explicit minimum cardinality check should be implemented at the pipeline level if needed.

### NULL values in compareColumns

The comparison uses null-safe operators:

- `NULL` vs `NULL` → **equal** (no modification)
- `NULL` vs `'value'` → **different** (new version)
- `'value'` vs `NULL` → **different** (new version)

This means going from `credit_limit = 5000` to `credit_limit = NULL` generates a new version (correct), and re-running with `credit_limit = NULL` does not generate another version (idempotent).

### Modifying compareColumns on an existing table

Adding or removing columns from the `compareColumns` list takes effect from the next run:

- **Adding a column** (e.g., adding `email` to the list): at the next run, all records with a different `email` between source and current table will be considered "modified" and generate new versions. If many records differ on the new column, this can cause a burst of versions.
- **Removing a column** (e.g., removing `email`): differences on `email` no longer generate new versions. Historical versions created for `email` changes remain in the table.

No migration is required — the framework uses the current `compareColumns` list at each run.

## compareColumns — what to monitor

`compareColumns` defines **which columns determine if a record has "changed"**. Only these columns are compared. Fields not listed can change without generating a new version.

Example: if `compareColumns: [tier, credit_limit]` and only the email changes, no new version is created. If the `tier` changes, a new version is created.

**What to include:**

- Business attributes whose changes have historical value (tier, status, price, address)

**What to exclude:**

- Columns that change every batch without historical value (update timestamps, technical flags)
- Primary key columns (cannot change by definition)

**If `compareColumns` is empty:** the config loader rejects the configuration. `compareColumns` is required and must be non-empty for SCD2.

## is_current vs is_active

These two boolean columns encode different information:

| is_current | is_active | Meaning |
|------------|-----------|---------|
| true | true | Current version, record exists in source |
| false | true | Historical version, superseded by a newer one |
| false | false | Record deleted from source (soft-delete) |
| true | false | Not possible under normal conditions |

**`is_current`** tracks versioning. Each primary key has exactly one row with `is_current = true` (invariant guaranteed by the framework).

**`is_active`** tracks logical deletion. Enables queries like "show me the last known state of all deleted customers" (`is_current = true` in their last version, `is_active = false`).

`is_active` is present only when `detectDeletes: true` or when `isActiveColumn` is explicitly configured.

## Enabling detectDeletes on an existing table

If you enable `detectDeletes` on an SCD2 table that already contains data, the `is_active` column is added automatically via schema evolution (`ALTER TABLE ADD COLUMN`). However, existing rows will have `is_active = NULL`, not `true`.

This means queries with `WHERE is_active = true` will exclude existing records — a **data correctness problem**.

The framework emits a warning when it detects this situation:

```
WARN  Column 'is_active' (is_active) was added to an existing table (catalog.default.customers).
      Rows written before this change have is_active=NULL and will be excluded by queries
      filtering on is_active = true.
      Run a full reload to backfill is_active=true on all current records.
```

!!!warning "Recommended action"
    After enabling `detectDeletes` on an existing table, run a full reload to backfill `is_active = true` on all current rows:

    1. Temporarily change `loadMode.type` to `full`
    2. Run the pipeline with the complete source dataset
    3. Restore `loadMode.type` to `scd2`

## Complete example: flow YAML

```yaml
name: "customers_scd2"
description: "Customer tier history — SCD2 tracking tier and credit_limit changes"
version: "1.0"
owner: "data-team"

source:
  type: "file"
  path: "data/customers_scd2.csv"
  format: "csv"
  options:
    header: "true"
    delimiter: ","

schema:
  enforceSchema: true
  allowExtraColumns: false
  columns:
    - name: "customer_id"
      type: "integer"
      nullable: false
      description: "Customer identifier"
    - name: "email"
      type: "string"
      nullable: false
      description: "Customer email"
    - name: "tier"
      type: "string"
      nullable: false
      description: "Customer tier (BRONZE/SILVER/GOLD/PLATINUM)"
    - name: "credit_limit"
      type: "double"
      nullable: false
      description: "Credit limit (EUR)"

loadMode:
  type: "scd2"
  validFromColumn: "valid_from"
  validToColumn: "valid_to"
  isCurrentColumn: "is_current"
  isActiveColumn: "is_active"
  compareColumns:
    - "tier"
    - "credit_limit"
  detectDeletes: true

validation:
  primaryKey:
    - "customer_id"
  foreignKeys: []
  rules: []

output:
  icebergPartitions:
    - "year(valid_from)"
```

## Useful queries

### Current state of all customers

```sql
SELECT * FROM catalog.default.customers_scd2
WHERE is_current = true
```

### Complete history of a single customer

```sql
SELECT * FROM catalog.default.customers_scd2
WHERE customer_id = 1
ORDER BY valid_from
```

### Deleted customers (soft-delete)

```sql
SELECT * FROM catalog.default.customers_scd2
WHERE is_current = false AND is_active = false
```

### Customer state at a specific point in time

```sql
SELECT * FROM catalog.default.customers_scd2
WHERE valid_from <= TIMESTAMP '2026-03-26 12:00:00'
  AND (valid_to IS NULL OR valid_to > TIMESTAMP '2026-03-26 12:00:00')
```

### Customers who changed tier

```sql
SELECT customer_id, COUNT(*) AS versions
FROM catalog.default.customers_scd2
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY versions DESC
```

### Batch comparison (via snapshot tag)

```sql
SELECT curr.customer_id, curr.tier AS tier_now, prev.tier AS tier_before
FROM catalog.default.customers_scd2 curr
JOIN catalog.default.customers_scd2 VERSION AS OF 'batch_20260326_100000' prev
  ON curr.customer_id = prev.customer_id
  AND curr.is_current = true AND prev.is_current = true
WHERE curr.tier != prev.tier
```

## Partitioning and performance

### Recommended partitioning strategy

The most common choice for SCD2 is to partition on `valid_from`:

```yaml
output:
  icebergPartitions:
    - "year(valid_from)"
```

This ensures:

- New versions end up in the current year's partition
- Historical versions stay in the partition of the year they were created
- Point-in-time queries (`WHERE valid_from <= X AND valid_to > X`) benefit from partition pruning

!!!warning
    Do not partition on `is_current` — it has only 2 values and creates severely imbalanced partitions. Do not partition on high-cardinality columns (customer_id, email).

### MERGE performance

The MERGE INTO for SCD2 only operates on rows with `is_current = true` thanks to the condition `AND target.is_current = true` in the `ON` clause. Historical versions (potentially millions) are not touched.

This means MERGE performance scales with the number of **current** records, not the total number of versions in the table. A table with 1 million current records and 50 million historical versions has the same MERGE performance as a table with only 1 million records.

### Consistency invariant

The framework guarantees that for each primary key value there is **exactly one row** with `is_current = true`. This invariant is maintained by the atomicity of MERGE INTO: closing the old version and inserting the new one happen in the same transaction.

If the batch fails mid-way, Iceberg performs automatic rollback and the table remains in the previous state — no row with `is_current` in an inconsistent state.

## SCD2 history vs Iceberg time travel

Both mechanisms allow viewing historical data, but they answer different questions:

| | SCD2 history | Iceberg time travel |
|---|---|---|
| **What it tracks** | Business changes (tier, status, price) | Technical changes (batch, write operations) |
| **Granularity** | Per-record: each change has valid_from/valid_to | Per-snapshot: entire table state at a given batch |
| **Retention** | Permanent — versions stay in the table | Configurable — snapshots expire after N days |
| **Query** | `WHERE customer_id = 1 ORDER BY valid_from` | `VERSION AS OF 'batch_20260326'` |
| **Typical use** | Audit, trend analysis, dimensional reporting | Debug, rollback, batch comparison |

The two features are complementary. SCD2 lives in the data, time travel lives in Iceberg metadata.

## How it works internally

### The atomicity problem

A naive SCD2 implementation would require two separate operations:

1. A MERGE to close old versions of modified records
2. An INSERT to insert new versions

If the system fails between step 1 and step 2, the table is left in an inconsistent state: old versions are closed but new ones are not yet inserted.

### The solution: NULL merge-key trick

The framework solves this with a single atomic `MERGE INTO`, using a staging view that contains each modified source record **twice**:

1. **With merge key = primary key** — to match the current version in the table and close it
2. **With merge key = NULL** — to force the `NOT MATCHED` clause and insert the new version

The staging view is constructed as:

```sql
-- Part 1: all source records (merge key = real PK)
SELECT src.*, src.customer_id AS _mk_customer_id
FROM _iceberg_scd2_customers_scd2_source src

UNION ALL

-- Part 2: only MODIFIED records (merge key = NULL)
SELECT src.*, CAST(NULL AS INT) AS _mk_customer_id
FROM _iceberg_scd2_customers_scd2_source src
JOIN target tgt
  ON src.customer_id = tgt.customer_id AND tgt.is_current = true
WHERE src.tier != tgt.tier
   OR (src.tier IS NULL AND tgt.tier IS NOT NULL)
   OR (src.tier IS NOT NULL AND tgt.tier IS NULL)
   OR src.credit_limit != tgt.credit_limit
   OR (src.credit_limit IS NULL AND tgt.credit_limit IS NOT NULL)
   OR (src.credit_limit IS NOT NULL AND tgt.credit_limit IS NULL)
```

The final MERGE operates on this staging view with three clauses:

```sql
MERGE INTO catalog.default.customers_scd2 AS target
USING _iceberg_scd2_customers_scd2_staged AS source
ON target.customer_id = source._mk_customer_id
   AND target.is_current = true

-- Clause 1: close the old version of the modified record
WHEN MATCHED AND (change condition) THEN UPDATE SET
  target.valid_to = current_timestamp(),
  target.is_current = false

-- Clause 2: insert the new version (merge key NULL → NOT MATCHED)
--           or a completely new record
WHEN NOT MATCHED THEN INSERT
  (customer_id, email, tier, credit_limit, valid_from, valid_to, is_current, is_active)
  VALUES (source.customer_id, source.email, source.tier, source.credit_limit,
          current_timestamp(), CAST(NULL AS TIMESTAMP), true, true)

-- Clause 3 (optional, only with detectDeletes=true):
-- soft-delete records not present in the source
WHEN NOT MATCHED BY SOURCE AND target.is_current = true THEN UPDATE SET
  target.valid_to = current_timestamp(),
  target.is_current = false,
  target.is_active = false
```

**Why it works:**

- A modified record appears twice in the staging view: once with the real PK (matches clause 1, closes old version), and once with merge key NULL (matches clause 2, inserts new version)
- A new record appears once with the real PK, but has no match in the table → clause 2, insert
- An unchanged record appears once with the real PK, matches clause 1, but the change condition is `false` → no action
- A record absent from the source is only present in the table → clause 3 (if enabled), soft-delete

Everything happens in a single atomic SQL statement. On error, Iceberg performs automatic rollback and the table remains in the previous state.

## Temporal continuity

For each pair of consecutive versions of the same record, the `valid_to` of the old version equals the `valid_from` of the new one:

```
version 1: valid_from=T1, valid_to=T2
version 2: valid_from=T2, valid_to=NULL
```

This is guaranteed because both operations (close old, open new) use `current_timestamp()` within the same MERGE statement, which is evaluated once. There are no temporal gaps between versions.

## Known limitations

### Single writer

The framework assumes a single writer per table. Concurrent executions of the same flow can violate the `is_current` invariant (two versions marked as current for the same PK). In environments with concurrent scheduling, ensure each SCD2 flow has at most one active instance.

### Late-arriving data

SCD2 in the framework uses `current_timestamp()` as `valid_from` for new versions. It does not support inserting historical versions with `valid_from` in the past. If a record arrives late with a state that was valid yesterday, it is inserted with `valid_from = now`, not `valid_from = yesterday`.

### No hard-delete

SCD2 never physically deletes rows from the table. With `detectDeletes: true`, records are only marked (`is_active = false`). The table grows monotonically. For environments with data retention requirements (e.g., GDPR), physical deletion must be handled separately via manual operations or Iceberg procedures like `DELETE FROM ... WHERE`.

### Primary key change

If a record's primary key changes in the source (e.g., a customer_id is reassigned), the framework treats it as two separate events: a soft-delete of the old PK and an insert of the new PK. There is no correlation between the two — the old PK's history is closed and the new PK starts an independent history.

## Related

- [Flow Configuration](../configuration/flows.md) — loadMode reference
- [Iceberg Integration](iceberg.md) — write operations and snapshot management
- [Orphan Detection](orphan-detection.md) — SCD2 with detectDeletes and orphan handling
- [Architecture: Design Decisions](../architecture/design-decisions.md) — why the NULL merge-key trick
