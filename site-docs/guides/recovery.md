# Recovery and Production Operations

Floe separates three concerns that are often conflated in batch pipelines:

- an Iceberg commit proves the state of one table;
- `RunStore` records the durable state of the application workflow;
- a release manifest pins the table snapshots published by one successful run.

These layers make recovery explicit. They do not turn a multi-table pipeline into an Iceberg transaction.

## Production baseline

`InMemoryRunStore` is the default so local examples remain small. Its state disappears with the JVM and it must not be used when restart recovery matters. Production jobs should share a `JdbcRunStore` between the ingestion job, recovery tooling, and maintenance worker:

```scala
import com.etl.framework.orchestration.state.JdbcRunStore
import com.etl.framework.pipeline.IngestionPipeline

val runStore = new JdbcRunStore(
  connectionFactory = () => dataSource.getConnection,
  tablePrefix = "floe_"
)

val pipeline = IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withRunStore(runStore)
  .withPipelineVersion(sys.env("APP_RELEASE_SHA"))
  .build()
```

`JdbcRunStore.initialize()` creates three tables when they do not exist: runs, target operations, and maintenance tasks. Its SQL uses primitives supported by PostgreSQL and H2; the automated suite exercises H2, so validate the chosen production database in deployment tests. Add the database driver and connection pool in the application; Floe does not bundle a production JDBC driver for the coordinator.

Use a dedicated schema/user, TLS, secret-managed credentials, backups, and database monitoring. All replicas of the same logical pipeline must use the same store and table prefix. Schema migration of an already existing coordinator database is an operator responsibility; `CREATE TABLE IF NOT EXISTS` is initialization, not a migration framework.

`withPipelineVersion` must identify deployed transformation code as well as configuration. A Git commit or immutable image digest is a good value. The default, `unversioned`, cannot detect changes to Scala transformation functions and is unsuitable for controlled production recovery.

## Run states

| State | Meaning | Normal operator action |
|-------|---------|------------------------|
| `PLANNED` | Run and target operations were registered | Start or resume execution |
| `RUNNING` | An executor owns the renewable lease | Monitor; do not start a second executor |
| `RECONCILING` | Durable state is being compared with Iceberg history | Wait for reconciliation to finish |
| `PUBLISHED` | Required targets succeeded and a release manifest was stored | Serve consumers; run queued maintenance separately |
| `SUCCEEDED_WITH_WARNINGS` | Data was published, but a per-flow diagnostic output or maintenance task failed | Repair/retry the side operation; do not replay ingestion |
| `FAILED` | Run failed before any managed target was proven committed | Fix the cause, then resume when reconciliation permits |
| `FAILED_PARTIAL` | At least one target committed before the run failed | Resume; never blindly rerun the whole batch |
| `UNKNOWN` | At least one commit outcome could not be proven | Restore catalog access and reconcile; do not guess |

`IngestionResult.success` describes the synchronous ingestion result. `IngestionResult.status` is the more precise durable state returned at publication time. A later maintenance failure can change the stored run from `PUBLISHED` to `SUCCEEDED_WITH_WARNINGS`; an earlier `IngestionResult` object and JSON summary are not retroactively updated.

## Execute, resume, or replay?

| Operation | Batch identity | Intended use | Safety condition |
|-----------|----------------|--------------|------------------|
| `execute()` | New batch ID and `effectiveAt` | Normal scheduled run | New input |
| `resume(batchId)` | Preserves original batch ID and `effectiveAt` | Continue an interrupted/partial run | Pipeline ID matches, commits reconcile, pending inputs have the same fingerprint |
| `replay(batchId)` | Creates a new batch linked through `replayOf` | Deliberate business reprocessing | Original run is terminal, pipeline ID matches, all source fingerprints still match |

Resume first classifies each managed flow and derived-table operation from its deterministic `floe.operation-id` in Iceberg snapshot summaries:

- exactly one matching snapshot proves the commit;
- no matching snapshot allows re-execution only if the input fingerprint still matches;
- multiple matches are inconsistent and block resume;
- catalog/history lookup failure remains unknown and blocks resume.

Do not wrap `execute()` in an external retry that starts a fresh run after a timeout. Capture the returned or logged batch ID and call `resume(batchId)` after the underlying catalog problem is resolved.

Replay is intentionally a new logical run. In particular, append-only Delta flows can append the same business rows again. Use replay only when that outcome is intended or when downstream deduplication is part of the design.

## Input fingerprints

File sources are fingerprinted from the expanded file inventory: path, length, modification time, format, and reader options. This detects normal replacement/addition/removal, but it is not a cryptographic hash of file contents. Object stores or ingestion systems that can replace bytes while preserving those attributes should provide their own immutable version in `source.options.replayToken`.

JDBC and custom readers always need an explicit token because Floe cannot infer whether a query will return the same rows:

```yaml
source:
  type: jdbc
  path: public.orders
  options:
    url: "jdbc:postgresql://db/app"
    query: "SELECT * FROM orders WHERE extract_id = '2026-09-24T00:00:00Z'"
    replayToken: "orders/extract/2026-09-24T00:00:00Z/v1"
```

The token must identify an immutable extract, snapshot, watermark interval, or source-system version. A constant token defeats the protection.

## Published reads

Iceberg commits are atomic per table. A successful Floe run stores a JSON `ReleaseManifest` in its `RunRecord`; it is not the diagnostic `summary.json`. Read it from the same `RunStore`, deserialize it, and pin every target:

```scala
import com.etl.framework.orchestration.state.{ReleaseManifest, SnapshotPinnedReader}

val run = runStore.getRun(batchId).getOrElse(sys.error("unknown batch"))
val manifest = ReleaseManifest.fromJson(
  run.releaseManifest.getOrElse(sys.error("batch is not published"))
)
val published = new SnapshotPinnedReader(manifest)

val orders = published.table("orders")
val customers = published.table("customers")
```

Reading mutable table heads can mix snapshots from different runs. `SnapshotPinnedReader` provides an application-level publication boundary, not a distributed transaction or isolation from code that ignores the manifest.

A target with no snapshot yet—for example a newly created table after a no-op empty write—is represented by `snapshotId = None`; `SnapshotPinnedReader` returns that table's empty schema-preserving view.

!!! warning "Orphan delete caveat"
    Flow and derived-table writes are tracked as durable operations. Individual `onOrphan: delete` commits in a multi-level cascade are not currently separate `RunStore` operations or a durable orphan-key worklist. The release target for an affected child therefore pins its tracked flow-write snapshot, not a later cleanup snapshot. If a cascade fails part-way through, stop later runs and follow the [orphan recovery runbook](orphan-detection.md#recovery-after-a-partial-batch-or-cascade). Do not claim automatic exactly-once recovery or a post-cleanup release view for that path.

## Asynchronous maintenance

Successful run finalization queues one maintenance task for every managed target. Run workers independently from ingestion; they will ignore the tasks until publication completes:

```scala
import com.etl.framework.orchestration.maintenance.MaintenanceWorker

val worker = new MaintenanceWorker(
  icebergConfig = globalConfig.iceberg,
  runStore = runStore,
  maxAttempts = 3
)

val results = worker.runPending(limit = 50)
```

Only tasks whose run is already `PUBLISHED` or `SUCCEEDED_WITH_WARNINGS` are eligible. This publication gate prevents a concurrently scheduled worker from maintaining a target while its release is still being finalized. Only one worker wins a task's compare-and-set transition. A failed task remains retryable until `maxAttempts`; retry the worker, not the pipeline. Schedule maintenance according to table size and workload instead of assuming it must run immediately after every ingestion.

Batch-summary and quality-metric write failures are currently logged but are not copied into `RunStatus`; alert on those logs separately. Per-flow rejected/warning/metadata write failures are recorded in `FlowResult.warnings` and do produce `SUCCEEDED_WITH_WARNINGS` after publication.

The batch JSON records only that tasks were queued. Query `RunStore.getMaintenanceTasks(...)` for their current authoritative state.

## Incident runbook

1. Stop new executions of the affected logical pipeline. Preserve source objects, catalog metadata, snapshots, coordinator rows, and logs.
2. Read the `RunRecord` and its operation records. Treat `UNKNOWN` and `UNKNOWN_COMMIT` as uncertainty, not failure.
3. Restore catalog/warehouse connectivity, then run `RecoveryManager.reconcileBatch(batchId)` in read-only mode if an operator needs the report before applying changes.
4. Use `pipeline.resume(batchId)` only when the same deployment/config and immutable inputs are available. It performs and applies reconciliation again under a lease.
5. After success, read consumers through the stored release manifest and run any queued maintenance independently.
6. For a partial orphan cascade, use the dedicated manual runbook; ordinary target reconciliation does not reconstruct the transient cascade key set.

## Related

- [Execution Model](../architecture/execution-model.md) — lifecycle and failure semantics
- [Iceberg Integration](iceberg.md) — commit identity, snapshots, and maintenance
- [Pipeline Builder](pipeline-builder.md) — public builder and result API
- [Orphan Detection](orphan-detection.md) — cascade behavior and manual recovery
