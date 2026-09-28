# 🧊 Floe

[![CI](https://github.com/giacomoaccursi/floe/actions/workflows/ci.yml/badge.svg)](https://github.com/giacomoaccursi/floe/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Scala](https://img.shields.io/badge/Scala-2.12-red.svg)](https://www.scala-lang.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5.8-orange.svg)](https://spark.apache.org/docs/3.5.8/)
[![Iceberg](https://img.shields.io/badge/Iceberg-1.10.1-blue.svg)](https://iceberg.apache.org/docs/1.10.1/)
[![Docs](https://img.shields.io/badge/docs-online-green.svg)](https://giacomoaccursi.github.io/Floe/)

Declarative ETL framework — built on Spark and Iceberg.

Define your data flows in YAML. The framework handles ingestion, validation, incremental loads, schema evolution, and table management — so you can focus on your data, not the plumbing.

## Why this framework

Most ETL tools force you to choose between flexibility and structure. You either write everything in code (flexible but fragile) or use a rigid GUI tool (structured but limited). Floe gives you both: declarative YAML for the pipeline structure, Scala code only where you need custom logic.

Each managed table write commits atomically through Iceberg. Successful write snapshots can be tagged for time travel, and rejected records are written with their reasons as an observable side output. FK relationships are checked both during ingestion and after parent-key removal.

## Quick example

**YAML config:**

```yaml
name: orders
source:
  type: file
  path: "data/orders.csv"
  format: csv
  options: { header: "true" }

schema:
  enforceSchema: true
  columns:
    - { name: order_id, type: integer, nullable: false }
    - { name: total, type: "decimal(10,2)", nullable: false }

loadMode:
  type: delta

validation:
  primaryKey: [order_id]
  rules:
    - { type: range, column: total, min: "0.01", onFailure: reject }
```

**Scala entry point:**

```scala
val result = IngestionPipeline.builder()
  .withConfigDirectory("config")
  .build()
  .executeOrThrow()
```

That's it. Floe reads the source data, validates schema and rules, upserts into an Iceberg table, tags the snapshot, and writes metadata — all from those few lines.

## Key features

| Feature | What it does |
|---------|-------------|
| **Declarative flows** | Define sources, schemas, validation rules, and load modes in YAML |
| **Three load modes** | Full (replace), Delta (upsert), SCD2 (versioned history with soft deletes) |
| **Built-in validation** | Schema, not-null, PK uniqueness, FK integrity, regex, range, domain, custom |
| **Orphan detection** | Post-batch FK integrity check using Iceberg time travel — warn or auto-delete |
| **Multiple sources** | CSV, Parquet, JSON, Avro, ORC files and JDBC databases. Pluggable custom readers |
| **Schema evolution** | Auto-add columns, auto-widen types (int→long, float→double, decimal precision) |
| **Quality metrics** | Optional Iceberg table with per-flow rejection rates, orphan counts, execution times |
| **Batch listeners** | Pluggable notifications — Slack, SNS, email, or any custom endpoint |
| **Retry** | Configurable exponential backoff with jitter for transient failures |
| **DAG aggregation** | Join, nest, flatten, and aggregate data across flows using a declarative DAG |
| **Derived tables** | Compute post-batch tables from the complete current state of Iceberg inputs |
| **Durable recovery** | Reconcile uncertain commits, resume partial runs, and publish snapshot-pinned releases |
| **Config validation** | Lint YAML and dependency graphs without reading source data or running Spark jobs |

## Getting started

### 1. Add the dependency

```scala
libraryDependencies += "io.github.giacomoaccursi" %% "floe" % "<version>"
```

### 2. Create your config

```
config/
├── global.yaml      # paths, iceberg settings, processing options
└── flows/
    ├── customers.yaml
    └── orders.yaml
```

### 3. Run

```scala
implicit val spark: SparkSession = SparkSession.builder()
  .appName("My Pipeline")
  .master("local[*]")
  .config("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .getOrCreate()

val result = IngestionPipeline.builder()
  .withConfigDirectory("config")
  .build()
  .executeOrThrow()
```

The default run coordinator is process-local and intended for development. Production deployments should configure a shared `JdbcRunStore`, set an immutable `pipelineVersion`, and run Iceberg maintenance from a separate worker. See the [Recovery and Production Operations](https://giacomoaccursi.github.io/Floe/guides/recovery/) guide.

## Runs everywhere

Works locally, on Spark clusters, and on managed platforms:

- **AWS Glue** — pass job parameters via `withVariables()`
- **Amazon EMR** — standard spark-submit
- **Databricks** — use dbutils widgets for parameters

See the [Cloud Deployment Guide](https://giacomoaccursi.github.io/Floe/guides/cloud-deployment/) for platform-specific examples.

## Documentation

Full documentation at **[giacomoaccursi.github.io/Floe](https://giacomoaccursi.github.io/Floe/)**

- [Quickstart](https://giacomoaccursi.github.io/Floe/getting-started/quickstart/) — first pipeline in 5 minutes
- [Configuration Reference](https://giacomoaccursi.github.io/Floe/configuration/overview/) — all YAML settings
- [Validation Engine](https://giacomoaccursi.github.io/Floe/guides/validation/) — all rule types
- [Iceberg Integration](https://giacomoaccursi.github.io/Floe/guides/iceberg/) — write modes, snapshots, maintenance
- [Pipeline Builder API](https://giacomoaccursi.github.io/Floe/guides/pipeline-builder/) — programmatic configuration
- [Recovery and Production Operations](https://giacomoaccursi.github.io/Floe/guides/recovery/) — durable state, resume, replay, and release manifests

## License

[Apache License 2.0](LICENSE)

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
