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

## What you get

- **Write YAML, not code** — describe your data sources, schemas, and rules in config files. The framework reads, validates, and writes the data for you.
- **Safe writes** — every write is atomic. If something fails mid-batch, your tables stay consistent. You can query any previous version of your data by batch ID.
- **Data quality built in** — check for nulls, duplicates, invalid formats, value ranges, and referential integrity between tables. Bad records are separated and saved for review.
- **Keep history** — track how records change over time with SCD2 (Slowly Changing Dimensions). The framework handles versioning, timestamps, and soft deletes automatically.
- **Combine tables** — join data from multiple flows into aggregated views using a DAG. Nest child records, flatten columns, or compute summaries.
- **Transform in code** — hook into the pipeline with Scala functions to enrich, filter, or reshape data before or after validation.
- **Detect broken references** — after a batch, the framework checks if parent records were removed and child records are now orphaned. It can warn or clean up automatically.
- **Get notified** — register batch listeners to receive notifications on completion or failure. Send to Slack, SNS, or any custom endpoint.
- **Track quality over time** — optionally write per-flow quality metrics to an Iceberg table for trend analysis and dashboards.
- **Read from anywhere** — built-in support for CSV, Parquet, JSON, Avro, ORC files and JDBC databases. Register custom readers for any other source.
- **Run anywhere** — works locally, on Spark clusters, and on managed platforms like AWS Glue, EMR, and Databricks. Pass environment-specific parameters without changing your YAML files.

## Stack

- Scala 2.12 · Apache Spark 3.5 · Apache Iceberg 1.10
- SBT · ScalaTest + ScalaCheck · PureConfig

## Quick links

- [Installation](getting-started/installation.md) — add the dependency
- [Quickstart](getting-started/quickstart.md) — first pipeline in 5 minutes
- [Configuration](configuration/overview.md) — all YAML settings
- [Architecture Overview](architecture/overview.md) — how it works
