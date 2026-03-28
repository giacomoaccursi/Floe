# Design Decisions

Key architectural decisions and the reasoning behind them.

## Why Iceberg is required

Parquet with `SaveMode.Overwrite` silently loses data on delta and SCD2 loads — a failed write mid-batch destroys the previous version with no recovery path. Without Iceberg there are no atomicity guarantees, no time travel, and no schema evolution.

Enterprise pipelines need ACID semantics, and the Iceberg hadoop catalog provides them with zero additional infrastructure (just 3 lines of YAML). Keeping a Parquet fallback would add complexity to every code path and create a false sense of safety for a mode that cannot support the framework's core load modes reliably.

## Why MERGE INTO

Iceberg's MERGE INTO is a single atomic SQL operation. The DataFrame API would require reading, transforming, and writing in separate steps with no transactional guarantee between them.

A single MERGE INTO statement handles insert, update, and (for SCD2) close operations atomically. If any part fails, Iceberg rolls back the entire operation and the table remains in the previous state.

## Why value-based change detection

An `update-timestamp-column` approach is fragile — it assumes the source always provides a reliable, monotonically increasing timestamp, and silently overwrites data when the timestamp is missing or stale.

Value-based change detection using the null-safe `<=>` operator makes no assumptions about the source: a row is updated if and only if at least one non-key column actually differs. This makes delta loads idempotent by default — re-running the same batch twice produces no writes and no data corruption.

## Why the NULL merge-key trick for SCD2

A single MERGE with three clauses (MATCHED, NOT MATCHED, NOT MATCHED BY SOURCE) is atomic. The alternative — a MERGE to close records followed by an INSERT for new versions — has a window between the two operations where the table is inconsistent.

The NULL merge key forces Iceberg to treat changed records as both a match (to close the old version) and a non-match (to insert the new version) in one pass. See [SCD2 Guide — How it works internally](../guides/scd2.md#how-it-works-internally) for the full explanation.

## Why format v2 only

Iceberg format v2 supports row-level deletes (position deletes and equality deletes), which are required for merge-on-read mode. Format v1 only supports file-level operations, making it impossible to implement efficient delta writes without full file rewrites.

Format v2 is the default in Iceberg 1.x and is required for the framework's MERGE INTO operations.

## Why copy-on-write is the default

Copy-on-write has no read-time overhead — queries scan data files directly without merging delete files. For most ETL workloads where reads outnumber writes, this is the better default.

Merge-on-read should be opted into explicitly via `tableProperties` for write-heavy flows or flows with frequent idempotent runs. The choice is per-flow, not global, because different flows have different read/write patterns.

## Why maintenance runs after orphan detection

Snapshot expiration removes old snapshots. Orphan detection needs the previous snapshot for time travel comparison (to find which parent keys were removed). If maintenance ran first, it could expire the snapshot that orphan detection needs.

Running orphan detection first guarantees the previous snapshot is still available. Maintenance then runs safely afterward.

## Why partition spec changes do not rewrite existing data

Rewriting all existing files on a config change would be an unbounded, blocking operation — a table with years of history could take hours. The framework applies the new spec to future writes only and warns the operator.

The decision to repartition existing data is an explicit operational action, not an automatic side effect of a config change. For large tables, use Iceberg's `rewrite_data_files` procedure.

## Why bounded parallelism

Using `ExecutionContext.Implicits.global` for parallel Spark operations is dangerous: the global pool has a fixed size based on available processors, and Spark jobs submitted from it compete for the same resources. This can lead to thread starvation and deadlocks.

The framework uses explicitly sized thread pools for both flow and DAG parallelism, ensuring predictable resource usage and preventing driver saturation.

## Why warn is the default for orphan detection

Automatic deletion (`onOrphan: delete`) is a destructive operation that removes data from Iceberg tables. In a production environment, it's preferable to signal the problem and let the team decide how to handle it, rather than silently deleting data.

The `warn` default ensures orphan detection is informational by default. Teams can opt into `delete` for specific FK relationships after understanding the implications and testing the cascade behavior.

## Related

- [Iceberg Integration](../guides/iceberg.md) — write operations and snapshot management
- [SCD2 Guide](../guides/scd2.md) — SCD2 implementation details
- [Orphan Detection](../guides/orphan-detection.md) — post-batch FK integrity
- [Architecture Overview](overview.md) — module diagram
