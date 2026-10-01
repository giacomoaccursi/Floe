# Generic Runner

`FloeRunner` is the process boundary for pipelines defined entirely in YAML. It turns a persisted execution request into one Spark application with deterministic exit semantics, without adding a coordinator database.

Use an application-owned main class instead when the pipeline registers Scala transformations, derived tables, custom readers, validators, catalog providers, listeners, or configuration variables that are not supplied by the environment. That application should call the same `IngestionPipeline.execute(request)` API and preserve the exit mapping below.

## Deployment contract

The platform must provide:

- one immutable application artifact and a `code_version` that identifies it;
- one immutable configuration location readable through Hadoop `FileSystem`;
- one persisted request per driver invocation, created before submission;
- the compatible Spark and Iceberg runtime dependencies;
- catalog configuration, credentials, writer serialization, cancellation, and retry policy.

The request and configuration may use local paths or Hadoop-compatible URIs such as `s3://...`. The request is limited to 1 MiB, must be valid UTF-8 JSON, and rejects duplicate or unknown fields. It must never contain credentials or secret values. A non-sensitive secret-manager identifier may use a key such as `secret_ref` or `credential_version`.

## Validate a deployment artifact

Run validation with the same semantic Spark settings that production will use:

```bash
spark-submit \
  --class com.etl.framework.runner.FloeRunner \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  my-pipeline-assembly.jar \
  validate \
  --config s3://company-config/orders/v42 \
  --pipeline-id orders-prod \
  --code-version sha256:4f5c2d...
```

Success writes one structured result line:

```json
{"status":"STATIC_VALID","pipeline_id":"orders-prod","code_version":"sha256:4f5c2d...","config_digest":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
```

This command loads and validates one configuration snapshot and computes its digest from that same in-memory representation. It validates syntax, flow rules, dependency references, cycles, derived declarations registered by the entry point, and configuration invariants. It does **not** read sources, contact target tables, verify permissions, establish catalog connectivity, or prove that a later run will succeed. Runtime/catalog preflight happens during `run`, before source processing and target writes.

The digest includes selected semantic Spark settings. A digest generated under different timezone, ANSI, case-sensitivity, or legacy time-parser settings will not match the run.

## Request format

The host creates a fresh `attempt_id` for every submitted driver while retaining `logical_run_id` and `effective_at` across an operator-approved retry of the same logical execution:

```json
{
  "contract_version": 1,
  "pipeline_id": "orders-prod",
  "logical_run_id": "scheduled-2026-10-01",
  "attempt_id": "d59882b1-b84a-4ae2-92fe-2bfe5ec78e41",
  "effective_at": "2026-10-01T00:00:00Z",
  "code_version": "sha256:4f5c2d...",
  "config_digest": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "data_interval": {
    "start_inclusive": "2026-09-30T00:00:00Z",
    "end_exclusive": "2026-10-01T00:00:00Z"
  },
  "input_references": [
    {
      "dataset_id": "raw-orders",
      "version": "s3-version-id-or-upstream-snapshot",
      "read_mode": "immutable-export"
    }
  ],
  "retry_profile": "manual-recovery",
  "platform_references": {
    "airflow_dag_id": "orders_daily",
    "airflow_task_id": "load_orders",
    "credential_version": "42"
  }
}
```

`input_references` are evidence supplied by the application/platform; they do not alter a reader or make a mutable source repeatable. The deployed reader must actually consume the referenced immutable version.

Scala applications should construct requests through `PipelineDefinition.newRequest` and serialize them with `ExecutionRequestJson.toJson`. The JSON field names above are the interoperable wire contract; Scala case-class names are not.

## Run one attempt

```bash
spark-submit \
  --class com.etl.framework.runner.FloeRunner \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  my-pipeline-assembly.jar \
  run \
  --config s3://company-config/orders/v42 \
  --request s3://company-control/orders/requests/d59882b1-b84a-4ae2-92fe-2bfe5ec78e41.json
```

The runner rebuilds the pipeline, rejects pipeline/code/config identity mismatches, performs target preflight, executes exactly one attempt, attempts the normal diagnostic report, and emits a compact JSON result. It never retries a flow or the whole job. A report failure after known data success produces `SUCCEEDED_WITH_WARNINGS`.

The runner stops a `SparkSession` it created. When embedded in a process that already owns an active/default session, it leaves that session running.

## Exit codes

| Code | Meaning | Platform action |
|---:|---|---|
| `0` | `SUCCEEDED` or `SUCCEEDED_WITH_WARNINGS` | Record the JSON/report; investigate warnings without rerunning data writers |
| `2` | Invalid command-line usage | Fix submission arguments |
| `3` | Invalid request/configuration or identity mismatch | Correct the immutable input; do not retry unchanged |
| `10` | `FAILED` | Inspect the report and target evidence before deciding on a new attempt |
| `11` | `FAILED_PARTIAL` | Freeze overlapping writers and follow the recovery runbook |
| `12` | `UNKNOWN` | Freeze overlapping writers; never retry automatically |
| `13` | Inspection could not query every requested evidence source | Keep the incident open and resolve the catalog/storage access failure |
| `70` | Runner/bootstrap internal failure | Treat the execution as unobserved until backend evidence is inspected |

Exit code `0` does not mean that every diagnostic write succeeded: `SUCCEEDED_WITH_WARNINGS` deliberately remains a successful data-plane outcome. Conversely, a missing JSON line or missing report is never proof that no data commit occurred. Managed services may collapse application exit codes into a generic failed state, so retain the structured result, request, logs, and Iceberg evidence.

## Orchestrator integration

Airflow, Step Functions, Argo, Glue, EMR, and similar systems should invoke the runner as one remote Spark job and wait for the **remote application**, not only for an asynchronous submission API. Disable platform-level automatic retry for the baseline `manual-recovery` contract.

Do not split Floe's internal flow DAG into scheduler tasks: downstream reads are pinned and state is held inside one driver attempt. Do not generate a new request after a failure until the earlier application has terminated and its effects have been classified. See [Failure Handling and Production Operations](recovery.md).

## Inspect an attempt

After the earlier remote application has terminated, run the read-only inspector with the original immutable request and configuration:

```bash
spark-submit \
  --class com.etl.framework.runner.FloeRunner \
  my-pipeline-assembly.jar \
  inspect \
  --config s3://company-config/orders/v42 \
  --request s3://company-control/orders/requests/d59882b1-b84a-4ae2-92fe-2bfe5ec78e41.json
```

For every primary and registered derived target, the result includes:

- the deterministic attempt operation ID and current table UUID;
- snapshots whose summary contains that exact operation ID;
- current `main` snapshot ID;
- whether each matching snapshot is current, an ancestor of `main`, or outside its current ancestry;
- presence/absence of the diagnostic attempt report.

Target evidence is classified as `COMMIT_OBSERVED`, `NO_COMMIT_EVIDENCE`, `TABLE_MISSING`, `MULTIPLE_COMMITS_OBSERVED`, or `INSPECTION_FAILED`. Snapshot IDs are JSON strings to preserve 64-bit precision. Quality-metrics and other best-effort diagnostics are not mutation targets and are not used to infer the data outcome.

The distinction is deliberate:

- `COMMIT_OBSERVED` proves that the matching historical Iceberg commit exists; it does not prove the whole job succeeded or that its data is still current.
- `NO_COMMIT_EVIDENCE` does **not** mean `NOT_COMMITTED`. The step may not have run, may have been a legitimate no-op, its snapshot may have expired, or evidence may be otherwise insufficient.
- `NOT_ANCESTOR_OF_MAIN` can indicate rollback or another ref/history change; it does not erase the historical commit.
- `MULTIPLE_COMMITS_OBSERVED` is an anomaly requiring investigation.

`retry_safe` is therefore always `false`. Exit code `0` means only that every requested evidence query completed, not that the attempt succeeded or may be retried. Exit code `13` means inspection itself was incomplete. Missing tables and zero matching snapshots are completed observations and remain explicit in JSON rather than being converted into query failure.
