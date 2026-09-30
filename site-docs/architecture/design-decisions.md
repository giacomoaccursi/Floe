# Design Decisions

Key architectural decisions and the reasoning behind them.

## Why Iceberg is required

Parquet with `SaveMode.Overwrite` silently loses data on delta and SCD2 loads — a failed write mid-batch destroys the previous version with no recovery path. Without Iceberg there are no atomicity guarantees, no time travel, and no schema evolution.

Enterprise pipelines need per-table ACID semantics. Iceberg provides them, while the Hadoop catalog keeps local and HDFS deployments small. Object-store deployments still need the catalog-specific concurrency controls documented by Iceberg—for example a lock manager for HadoopCatalog on S3. Keeping a Parquet fallback would add complexity to every code path and create a false sense of safety for a mode that cannot support the framework's core load modes reliably.

## Why MERGE INTO

Iceberg's MERGE INTO is a single atomic SQL operation. The DataFrame API would require reading, transforming, and writing in separate steps with no transactional guarantee between them.

A single MERGE INTO statement handles insert, update, and (for SCD2) close operations atomically. If any part fails, Iceberg rolls back the entire operation and the table remains in the previous state.

## Why value-based change detection

An `update-timestamp-column` approach is fragile — it assumes the source always provides a reliable, monotonically increasing timestamp, and silently overwrites data when the timestamp is missing or stale.

Value-based change detection using the null-safe `<=>` operator makes no assumptions about the source: for keyed Delta loads, a row is updated only when at least one compared non-key column differs. A Delta flow without a primary key is rejected before target creation; implicit append is too easy to duplicate during recovery.

## Why the NULL merge-key trick for SCD2

A single MERGE with three clauses (MATCHED, NOT MATCHED, NOT MATCHED BY SOURCE) is atomic. The alternative — a MERGE to close records followed by an INSERT for new versions — has a window between the two operations where the table is inconsistent.

The NULL merge key forces Iceberg to treat changed records as both a match (to close the old version) and a non-match (to insert the new version) in one pass. See [SCD2 Guide — How it works internally](../guides/scd2.md#how-it-works-internally) for the full explanation.

## Why format v2 only

Iceberg format v2 supports row-level deletes (position deletes and equality deletes), which are required for merge-on-read mode. Format v1 only supports file-level operations, making it impossible to implement efficient delta writes without full file rewrites.

Floe explicitly creates format-v2 tables. Iceberg has defaulted new tables to v2 since release 1.4.0, but the explicit property keeps the contract visible and enables merge-on-read delete files.

## Why copy-on-write is the default

Copy-on-write has no read-time overhead — queries scan data files directly without merging delete files. For most ETL workloads where reads outnumber writes, this is the better default.

Merge-on-read should be opted into explicitly via `tableProperties` for write-heavy flows or flows with frequent idempotent runs. The choice is per-flow, not global, because different flows have different read/write patterns.

## Why maintenance is a separate job

Snapshot expiration removes old snapshots. Orphan detection needs the previous snapshot for time travel comparison (to find which parent keys were removed). If maintenance ran first, it could expire the snapshot that orphan detection needs.

Running orphan detection before returning the ingestion result preserves the previous snapshot during that attempt. Compaction, snapshot expiration, manifest rewriting, and orphan-file deletion are independently scheduled operational work: they must not lengthen ingestion or cause data replay when they fail.

## Why Floe does not claim multi-table publication

Iceberg commits are atomic per table, not across a set of tables. Floe therefore returns per-target snapshot evidence and an explicit partial/unknown status; it does not label a set of independent commits as an atomic release. Consumers needing an atomic multi-table publication protocol must implement and qualify that separate contract instead of reading mutable HEADs.

## Why retry belongs to the platform but remains constrained

A client-side exception can occur after a catalog accepted an Iceberg commit. Floe writes logical and attempt operation identities into snapshot summaries and reports uncertainty instead of retrying internally. The host may resubmit only a fully qualified repeatable application using the same immutable logical request and a new attempt ID. Ordinary JDBC queries, mutable globs, custom code, and multi-table cascades are not repeatable merely because their configuration text is unchanged.

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
