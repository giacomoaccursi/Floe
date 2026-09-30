# Failure Handling and Production Operations

Floe does not contain a workflow database, lease service, or general-purpose resume engine. That is intentional: Airflow, Step Functions, Argo, Databricks Jobs, Glue, and similar platforms already own job state and scheduling. Floe owns data-plane execution and reports the evidence it observed.

## The boundary of the guarantee

An Iceberg commit is atomic for one table. A pipeline that writes several tables is not one transaction. If `customers` commits and `orders` then fails, Floe returns `FAILED_PARTIAL`; it does not roll `customers` back and does not retry `orders` internally.

Likewise, a client exception does not prove that a remote catalog rejected a commit. When Floe entered the data-commit path but cannot establish the result, the target outcome remains `UNKNOWN` and the aggregate status is `UNKNOWN`. Do not start another incompatible writer until the original request and any in-flight backend operation have been investigated.

## Execution identity

Production callers should build a stable pipeline definition and persist the immutable request before job submission:

```scala
val pipeline = IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withPipelineId("orders-prod-eu")
  .withCodeVersion(sys.env("APP_IMAGE_DIGEST"))
  .build()

val request = pipeline.executionDefinition.newRequest(
  logicalRunId = "orders/2026-09-30",
  effectiveAt = Instant.parse("2026-09-30T00:00:00Z"),
  dataInterval = Some(DataInterval(
    Instant.parse("2026-09-29T00:00:00Z"),
    Instant.parse("2026-09-30T00:00:00Z")
  )),
  platformReferences = Map("airflowTask" -> "orders_daily")
)

val result = pipeline.execute(request)
if (!result.success) throw BatchFailedException(result.attemptId, result.error.getOrElse("failed"))
```

The identifiers have different meanings:

| Field | Meaning |
|---|---|
| `pipelineId` | Stable identity of the pipeline in one environment |
| `logicalRunId` | Stable identity of the business execution; keep it across an approved retry |
| `attemptId` | Unique driver invocation; every resubmission gets a new value |
| `effectiveAt` | Functional timestamp used by SCD2 and exposed to transformations |
| `codeVersion` | Immutable application artifact identity, including custom Scala code |
| `configDigest` | Canonical digest of the resolved semantic Floe/Spark configuration |

Floe rejects a request when its pipeline, code version, or config digest differs from the built pipeline. Secret values are not included in the digest; store logical secret references and their externally managed versions in the platform request if rotation affects reproducibility.

## Outcome model

`IngestionResult.success` is the functional outcome. `status` adds the data-effect classification:

| Status | Interpretation |
|---|---|
| `SUCCEEDED` | All required synchronous work completed |
| `SUCCEEDED_WITH_WARNINGS` | Functional data succeeded; a diagnostic side output failed |
| `FAILED` | Failure occurred before any known completed target operation |
| `FAILED_PARTIAL` | At least one target completed, but the pipeline did not |
| `UNKNOWN` | At least one mutation may have committed but the evidence is inconclusive |

Each flow and derived target exposes a `DataOutcome`:

- `NOT_ATTEMPTED`: Floe did not enter the data mutation;
- `NOT_COMMITTED`: the backend supplied positive proof that the attempt did not commit;
- `NO_CHANGE`: the mutation completed without creating a new data snapshot;
- `COMMITTED`: a new snapshot was identified;
- `UNKNOWN`: commit outcome cannot be established safely.

`resultingSnapshotId` is the snapshot used for downstream dependencies. Dependencies are read at that snapshot rather than mutable table HEAD. Snapshot IDs in JSON reports are decimal strings, avoiding precision loss in JavaScript consumers.

## Retry policy

Floe performs no automatic flow or whole-pipeline retry. The default contract is `manual-recovery`.

Do not configure scheduler retries merely because the failed flow says `NOT_ATTEMPTED`: an earlier target may already be committed. A whole-job retry is allowed only after the complete application recipe has been proven repeatable for its real inputs, transformations, write modes, catalog, storage, cancellation behavior, and concurrency policy. `UNKNOWN` always blocks automatic retry.

At minimum, a repeatability assessment must establish:

1. the same immutable input versions are read again;
2. `effectiveAt` and semantic configuration are unchanged;
3. transformations are deterministic and side-effect free;
4. each target write mode is content-idempotent for the tested failure points;
5. the previous driver and remote requests have terminated;
6. writers for overlapping targets are externally serialized;
7. the final table contents and invariants match a clean execution.

A mutable directory/glob, ordinary JDBC query, or opaque custom reader is not automatically repeatable. Materialize an immutable extract upstream or qualify a connector-specific snapshot mechanism.

## Incident runbook

1. Record `pipelineId`, `logicalRunId`, `attemptId`, platform job/application ID, report path, and original request.
2. Stop later writers and maintenance for the affected targets. A canceled scheduler task does not by itself prove a remote request stopped.
3. Retain the application artifact, resolved configuration, input references, logs, report, and Iceberg history.
4. Inspect the current table identity, snapshot history, snapshot summaries, and refs. Operation metadata is evidence, not a lock.
5. Classify every target. Absence of a matching snapshot is not universal proof that no delayed effect can appear.
6. Correct data with a separately reviewed operation when needed. Floe does not perform an automatic multi-table rollback.
7. Resubmit the whole job only if the repeatability conditions above are satisfied; keep the same `logicalRunId` and use a new `attemptId`.
8. Validate content and business invariants after recovery, not only the scheduler state.

## Reports and diagnostics

The attempt report is written below:

```
{metadataPath}/{pipelineId}/{logicalRunId}/{attemptId}/summary.json
```

Per-flow metadata uses the sibling `flows/` directory. Rejected rows and validation warnings use the same three-part identity under their configured roots. Identifiers are validated to prevent path traversal.

These files are diagnostic by default. A report-write failure adds a warning and does not reinterpret a known Iceberg commit. If consumers require the report as the delivery mechanism for pinned snapshots, that is a stricter application contract: treat report publication as a functional output in the surrounding application and never rerun data writers merely to recreate the report.

## Maintenance

Run `IcebergMaintenanceRunner` from a separate scheduled job. Compaction, snapshot expiration, orphan-file cleanup, and manifest rewrite must not be coupled to ingestion retries. Establish retention and concurrency policy for the actual catalog/storage deployment before enabling deletion procedures.

Do not confuse Iceberg orphan files with child rows that violate a configured foreign key. They have different causes and recovery procedures.
