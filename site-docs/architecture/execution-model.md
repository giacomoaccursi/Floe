# Execution Model

How flows are ordered, executed in parallel, and how the batch lifecycle works.

## Flow ordering from FK dependencies

Flows are not executed in the order they appear in YAML files. The framework builds an execution plan based on foreign key dependencies:

1. **Dependency analysis** — FK references and explicit `dependsOn` declarations between flows are extracted. If `orders` has a FK referencing `customers.customer_id` (or declares `dependsOn: [customers]`), then `customers` is a dependency of `orders`.

2. **Topological sort** — flows are sorted so that every parent executes before its children. If a circular dependency is detected, a `CircularDependencyException` is thrown with the full cycle path.

3. **Grouping** — flows at the same level (no dependency between them) are grouped together. Each group can potentially execute in parallel.

Example with three flows:

```
customers (no FK)          ← Group 1
orders (FK → customers)    ← Group 2
order_items (FK → orders)  ← Group 3
```

If `products` has no FK to any other flow, it joins Group 1:

```
Group 1: customers, products  (independent, can run in parallel)
Group 2: orders               (depends on customers)
Group 3: order_items          (depends on orders)
```

## Parallel execution

### Flow parallelism

When `performance.parallelFlows` is `true` in `global.yaml`, independent flows within the same group run concurrently:

```yaml
performance:
  parallelFlows: true
```

The thread pool is explicitly sized — the framework does **not** use `ExecutionContext.Implicits.global`. This prevents unbounded parallelism from saturating the Spark driver.

Groups execute sequentially: Group 2 starts only after all flows in Group 1 complete. Within a group, flows run in parallel.

If `parallelFlows` is `false`, all flows execute sequentially in topological order.

### DAG node parallelism

When `parallelNodes` is `true` in the DAG YAML, independent DAG nodes within the same execution group run concurrently:

```yaml
# In the DAG YAML file
parallelNodes: true
```

The thread pool is sized at `availableProcessors * 2`.

If `parallelNodes` is `false` (default), all DAG nodes execute sequentially regardless of independence.

### Thread pool management

Both flow and DAG parallelism use bounded, explicitly sized thread pools:

- Flow parallelism: pool sized at `Runtime.getRuntime.availableProcessors * 2`
- DAG parallelism: pool sized at `Runtime.getRuntime.availableProcessors * 2`

This follows the Spark best practice of never using the global execution context for parallel Spark operations. Each thread submits Spark jobs independently, and Spark's internal scheduler handles resource allocation.

## Batch lifecycle

A complete batch execution follows this sequence:

```mermaid
graph TD
    subgraph Phase1["1. Build phase"]
        B1["Load configuration<br/>(YAML or programmatic)"]
        B2["Validate all configs"]
        B3["Configure Iceberg catalog<br/>on SparkSession"]
        B4["Register transformations<br/>and catalog providers"]
        B1 --> B2 --> B3 --> B4
    end

    subgraph Phase2["2. Execution phase"]
        E1["Generate batch ID<br/>(from batchIdFormat)"]
        E2["Analyze FK dependencies<br/>→ topological sort → group flows"]
        E3["For each group (sequential)"]
        E4["For each flow in group<br/>(parallel if enabled)"]
        E5["Read source data"]
        E6["Apply column renames"]
        E7["Run pre-validation transformation"]
        E8["Run validation pipeline"]
        E9["Check rejection rate<br/>against threshold"]
        E10["Run post-validation transformation"]
        E11["Write to Iceberg<br/>(MERGE INTO / overwrite / SCD2)"]
        E1 --> E2 --> E3 --> E4
        E4 --> E5 --> E6 --> E7 --> E8 --> E9 --> E10 --> E11
    end

    subgraph Phase3["3. Post-batch phase"]
        P1["Orphan detection<br/>(time travel, cascade)"]
        P2["Derived tables"]
        P3["Persist release manifest<br/>with exact snapshot IDs"]
        P4["Queue table maintenance<br/>(outside ingestion)"]
        P5["Write diagnostic metadata<br/>and quality metrics"]
        P1 --> P2 --> P3 --> P4 --> P5
    end

    subgraph Phase4["4. DAG phase (separate execution)"]
        D1["Load DAG config"]
        D3["Build dependency graph<br/>→ topological sort → group nodes"]
        D4["Execute nodes group by group"]
        D1 --> D3 --> D4
    end

    Phase1 --> Phase2 --> Phase3
    Phase3 -.->|"if configured,<br/>invoked separately"| Phase4
```

### Batch ID

The batch ID is generated from `processing.batchIdFormat` using Java's `DateTimeFormatter`:

```yaml
processing:
  batchIdFormat: "yyyyMMdd_HHmmss"
```

Example: `20260328_150000`. The batch ID is used for:

- Snapshot tagging (`batch_20260328_150000`)
- Metadata directory naming (`{metadataPath}/20260328_150000/`)
- Logging and tracing

### Failure handling

- **Flow failure**: if a flow fails, the batch stops. The failed flow is reported in `IngestionResult`.
- **Rejection threshold**: if `maxRejectionRate` is configured (globally or per-flow) and any flow's rejection rate exceeds the threshold, the batch stops. In sequential execution, remaining flows in the current group are not executed. In parallel execution, flows already running complete but subsequent groups are not started.
- **Orphan/derived failure**: orphan-detection and derived-table failures make `IngestionResult.success = false`; already committed tables are not rolled back.
- **Diagnostic-output failure**: rejected-row, warning, and per-flow metadata failures do not reinterpret an already committed Iceberg write as failed. They are exposed in `FlowResult.warnings`, and the run is published as `SUCCEEDED_WITH_WARNINGS`.
- **Maintenance failure**: ingestion only persists `QUEUED` maintenance tasks. A separate `MaintenanceWorker` claims and retries them; a failure changes the durable run status to `SUCCEEDED_WITH_WARNINGS` without replaying ingestion.
- **Iceberg commit uncertainty**: a failed client call does not prove that an Iceberg commit failed. Every mutation carries a deterministic operation ID in the snapshot summary. Recovery searches Iceberg history for that ID and classifies the operation as committed, absent, unknown, or inconsistent before any resume.

## Durable run states, resume, and replay

Production deployments should configure a `JdbcRunStore`. It stores versioned run and operation states, exact snapshot IDs, input fingerprints, leases with fencing tokens, release manifests, and maintenance tasks. `InMemoryRunStore` has the same state model but is only suitable for tests and single-process development.

`pipeline.resume(batchId)` keeps the original batch ID and `effectiveAt`. It acquires the run lease, reconciles operation IDs against Iceberg, validates that pending inputs have not changed, and executes only operations proven absent. Already committed targets are loaded at their recorded snapshots for downstream dependency checks.

`pipeline.replay(batchId)` is different: it requires a terminal source batch and immutable input fingerprints, then creates a new linked batch with new operation IDs. File inputs are fingerprinted from their file inventory; JDBC and custom readers must provide `source.options.replayToken`.

A successful run is visible to consumers through one release manifest containing the exact snapshot ID for every flow and derived table. Per-table commits remain independent—Iceberg has no cross-table transaction—but consumers using `SnapshotPinnedReader` see the application-level release boundary instead of a mixture of table heads.

## Related

- [Architecture Overview](overview.md) — module diagram
- [Data Flow](data-flow.md) — step-by-step pipeline
- [Global Configuration — performance](../configuration/global.md#performance) — parallelism settings
- [DAG Aggregation](../guides/dag-aggregation.md) — DAG execution details
- [Orphan Detection](../guides/orphan-detection.md) — post-batch FK integrity
- [Batch Listeners](../guides/batch-listeners.md) — notifications on batch completion/failure
- [Quality Metrics](../guides/quality-metrics.md) — per-flow metrics Iceberg table
