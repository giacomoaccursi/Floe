# Modules

Detailed responsibilities of each module in the framework.

## Module overview

| Module | Responsibility |
|--------|---------------|
| `config` | YAML config loading & validation |
| `validation` | Validation engine (schema, not-null, PK, FK, custom rules) |
| `merge` | *(dead code)* Pre-Iceberg in-memory merge — no longer used |
| `iceberg` | Iceberg table writes (full/delta/SCD2) |
| `io` | Data readers: file (CSV, Parquet, JSON), JDBC |
| `aggregation` | DAG execution for layered joins and aggregations |
| `orchestration` | Flow and batch orchestration, metadata, logging |
| `pipeline` | Ingestion pipeline entry point and fluent builder API |
| `exceptions` | Domain-specific exception hierarchy |

## config

Loads and validates YAML configuration files using PureConfig. Handles three config types:

- `GlobalConfig` — paths, processing, performance, Iceberg settings
- `DomainsConfig` — named value sets for domain validation
- `FlowConfig` — per-flow source, schema, loadMode, validation, output

Supports variable substitution (`${VAR_NAME}`) with resolution from explicit variables and environment variables. Reports all errors together rather than failing on the first one.

See [Configuration Overview](../configuration/overview.md).

## validation

The validation engine runs a deterministic pipeline on every incoming DataFrame:

1. Schema validation (column presence, extra columns)
2. Not-null validation (non-nullable columns)
3. Primary key uniqueness (duplicate detection)
4. Foreign key integrity (parent flow lookup)
5. Custom rules (regex, range, domain, user-defined)

Each step separates valid from rejected records. Rejected records carry metadata columns (`_rejection_code`, `_rejection_reason`, `_validation_step`, `_rejected_at`).

Includes a plugin system for custom validators loaded via reflection.

See [Validation Engine](../guides/validation.md).

## merge

**Dead code.** This module contained a pre-Iceberg in-memory merge implementation. It is no longer used — all merge operations now go through Iceberg's MERGE INTO. The module remains in the codebase but is not referenced by any active code path.

## iceberg

Handles all Iceberg table operations:

- **Table management**: CREATE TABLE IF NOT EXISTS, schema evolution (ADD COLUMN), partition spec updates, table property updates
- **Write strategies**: full load (overwrite), delta (MERGE INTO with value-based change detection), SCD2 (NULL merge-key trick)
- **Snapshot management**: tagging, metadata capture, rollback
- **Maintenance**: snapshot expiration, data compaction, orphan file cleanup, manifest rewrite
- **Orphan detection**: post-batch FK integrity using time travel
- **Catalog providers**: pluggable catalog system (Hadoop, Glue, custom)

See [Iceberg Integration](../guides/iceberg.md).

## io

Data readers for various source types:

- **File** — CSV (configurable delimiter, header, quoting, encoding), Parquet, JSON
- **JDBC** — any database with a JDBC driver (PostgreSQL, MySQL, Oracle, etc.)

Uses `DataReaderFactory` to create the appropriate reader based on `source.type`. Supports optional schema enforcement during read (converts `SchemaConfig` to Spark `StructType`).

See [Data Sources](../guides/data-sources.md).

## aggregation

DAG-based aggregation module for joining and aggregating data across multiple flows:

- **DAGOrchestrator** — entry point, loads config, delegates to builder and executor
- **DAGGraphBuilder** — validates nodes, builds dependency graph, topological sort
- **DAGExecutor** — executes the plan group by group
- **DAGNodeProcessor** — loads source data, applies filters/select, joins with parent
- **JoinStrategyExecutor** — implements Nest, Flatten, and Aggregate join strategies

See [DAG Aggregation](../guides/dag-aggregation.md).

## orchestration

Coordinates batch and flow execution:

- **FlowOrchestrator** — executes a single flow: read → transform → validate → transform → write
- **FlowGroupExecutor** — manages the batch lifecycle: flow ordering, execution, post-batch operations
- **ExecutionPlanBuilder** — analyzes FK dependencies, topological sort, groups independent flows
- **BatchMetadataWriter** — writes batch and flow metadata JSON files
- **ExecutionLogger** — structured logging for batch and flow execution
- **Flow ordering** — analyzes FK dependencies, topological sort, groups independent flows
- **Parallel execution** — bounded thread pool for concurrent flow/node execution

See [Execution Model](execution-model.md).

## pipeline

Public API entry point:

- **IngestionPipeline** — fluent builder for configuring and executing pipelines
- **TransformationContext** — immutable context passed to transformation functions
- **IngestionResult / FlowResult** — execution results with metrics and metadata

See [Pipeline Builder](../guides/pipeline-builder.md).

## exceptions

Domain-specific exception hierarchy rooted at `FrameworkException`:

- `ConfigurationException` — YAML syntax, missing fields, invalid types, circular dependencies
- `ValidationException` — schema, PK, FK, rejection rate violations
- `DataProcessingException` — source errors, write errors, merge errors
- `TransformationException` — pre/post-validation transformation failures
- `AggregationException` — DAG node execution, join failures
- `PluginException` — custom validator loading/execution failures
- `OrchestrationException` — flow execution failures

Every exception carries an error code and a context map for debugging.

See [Reference: Exceptions](../reference/exceptions.md).

## Related

- [Architecture Overview](overview.md) — module diagram and interactions
- [Data Flow](data-flow.md) — how data moves through modules
