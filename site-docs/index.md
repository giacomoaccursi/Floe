# Spark ETL Framework

A declarative Scala/Spark framework for building enterprise ETL pipelines with Apache Iceberg.

## What it does

Define your data flows in YAML — the framework handles ingestion, validation, incremental loads, and Iceberg table management automatically.

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
    - { name: order_id, type: integer, nullable: false, description: "PK" }
    - { name: total, type: "decimal(10, 2)", nullable: false, description: "Order total" }

loadMode:
  type: delta

validation:
  primaryKey: [order_id]
  rules:
    - { type: range, column: total, min: "0.01", onFailure: reject }
```

```scala
val result = IngestionPipeline.builder()
  .withConfigDirectory("config")
  .build()
  .execute()
```

## Key features

- **Declarative YAML configuration** — define flows, validation rules, and aggregations without code
- **Apache Iceberg writes** — atomic MERGE INTO, snapshot tagging, time travel, automatic maintenance
- **Data validation** — schema enforcement, PK/FK integrity, regex, range, domain, custom rules
- **Three load modes** — full replace, delta upsert, SCD2 with history tracking
- **DAG aggregation** — join flows with Nest, Flatten, or Aggregate strategies
- **Transformation API** — pre/post-validation hooks with `TransformationContext`
- **Orphan detection** — post-batch FK integrity with cascade support
- **Cloud ready** — `withVariables` for Glue, EMR, Databricks parameter injection

## Stack

- Scala 2.12 · Apache Spark 3.5 · Apache Iceberg 1.10
- SBT · ScalaTest + ScalaCheck · PureConfig

## Quick links

- [Installation](getting-started/installation.md) — add the dependency
- [Quickstart](getting-started/quickstart.md) — first pipeline in 5 minutes
- [Configuration Reference](reference/config-reference.md) — all YAML fields
- [Architecture Overview](architecture/overview.md) — how it works
