# 🧊 Floe

[![CI](https://github.com/giacomoaccursi/floe/actions/workflows/ci.yml/badge.svg)](https://github.com/giacomoaccursi/floe/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Scala](https://img.shields.io/badge/Scala-2.12-red.svg)](https://www.scala-lang.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5.8-orange.svg)](https://spark.apache.org/docs/3.5.8/)
[![Iceberg](https://img.shields.io/badge/Iceberg-1.10.1-blue.svg)](https://iceberg.apache.org/docs/1.10.1/)
[![Docs](https://img.shields.io/badge/docs-online-green.svg)](https://giacomoaccursi.github.io/Floe/)

Declarative batch ETL framework built on Apache Spark and Apache Iceberg.

Define your data flows in YAML. The framework handles ingestion, validation, incremental loads, and Iceberg writes; table DDL is either validated against platform provisioning or explicitly delegated to Floe for local/bootstrap workflows.

## Why this framework

Most ETL tools force you to choose between flexibility and structure. You either write everything in code (flexible but fragile) or use a rigid GUI tool (structured but limited). Floe gives you both: declarative YAML for the pipeline structure, Scala code only where you need custom logic.

Each managed table write commits atomically through Iceberg. Successful write snapshots can be tagged for time travel, and rejected records are written with their reasons as an observable side output. FK relationships are checked both during ingestion and after parent-key removal.

## Quick example

A runnable configuration needs one global file and at least one flow file.

**`config/global.yaml`:**

```yaml
paths:
  outputPath: "output/data"
  rejectedPath: "output/rejected"
  metadataPath: "output/metadata"

iceberg:
  catalogMode: configure
  ddlMode: automatic
  catalogType: hadoop
  catalogName: floe
  warehouse: "output/warehouse"
```

`floe` is the logical Spark catalog name, not an external service. This local example explicitly lets Floe configure the catalog and manage table DDL. Enterprise deployments normally keep the defaults `catalogMode: existing` and `ddlMode: validate`: the platform configures the catalog and provisions tables before execution.

**`config/flows/orders.yaml`:**

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

Floe reads the source data, validates schema and rules, upserts into `floe.default.orders`, tags the committed snapshot, and writes an attempt report. Floe does not require a coordinator database.

## Key features

| Feature | What it does |
|---------|-------------|
| **Declarative flows** | Define sources, schemas, validation rules, and load modes in YAML |
| **Three load modes** | Full (replace), Delta (keyed upsert; a PK is mandatory), SCD2 (versioned history with soft deletes) |
| **Built-in validation** | Schema, not-null, PK uniqueness, FK integrity, regex, range, domain, custom |
| **Orphan detection** | Post-batch FK integrity check using pinned Iceberg snapshots — warn or ignore, never implicit deletion |
| **Multiple sources** | CSV, Parquet, JSON, Avro, ORC files and JDBC databases. Pluggable custom readers |
| **Schema evolution** | Auto-add columns, auto-widen types (int→long, float→double, decimal precision) |
| **Quality metrics** | Optional Iceberg table with per-flow rejection rates, orphan counts, execution times |
| **Batch listeners** | Pluggable notifications — Slack, SNS, email, or any custom endpoint |
| **Explicit execution contract** | Separates logical run, physical attempt, effective time, code version, and canonical config digest |
| **DAG aggregation** | Join, nest, flatten, and aggregate data across flows using a declarative DAG |
| **Derived tables** | Declare a derived DAG and read only attempt-pinned flow or derived inputs |
| **Typed outcomes** | Reports success, warnings, partial failure, and unknown commit outcomes without pretending the batch is atomic |
| **Config validation** | Lint YAML and dependency graphs without reading source data or running Spark jobs |

## Getting started

### Requirements

- Java 17 (the runtime used by CI)
- Scala 2.12
- Apache Spark 3.5.8
- SBT 1.9+

Spark dependencies are marked as `provided`: the application or cluster must supply a compatible Spark runtime. Release artifacts are configured for GitHub Packages. GitHub requires authentication even when downloading a public Maven package; use a classic personal access token with `read:packages` and keep it outside the repository. See GitHub's [Apache Maven registry documentation](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry).

### 1. Add Floe

```scala
resolvers += "GitHub Packages" at
  "https://maven.pkg.github.com/giacomoaccursi/floe"

credentials += Credentials(Path.userHome / ".sbt" / "github-packages.credentials")

libraryDependencies += "io.github.giacomoaccursi" %% "floe" % "<version>"
```

Store the credentials locally in `~/.sbt/github-packages.credentials`:

```properties
realm=GitHub Package Registry
host=maven.pkg.github.com
user=YOUR_GITHUB_USERNAME
password=YOUR_CLASSIC_PAT
```

Replace `<version>` with an existing release version. Never commit the credentials file or token.

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

The Iceberg SQL extensions must be configured before Spark creates the session. Floe then registers the catalog declared in `global.yaml`; callers do not need to duplicate catalog properties in the `SparkSession` builder.

For a platform-managed run, build the pipeline with a stable identity and immutable artifact version, persist the request before submission, and pass it to Floe:

```scala
val pipeline = IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withPipelineId("orders-prod")
  .withCodeVersion(sys.env("APP_IMAGE_DIGEST"))
  .build()

val request = pipeline.executionDefinition.newRequest(
  logicalRunId = sys.env("LOGICAL_RUN_ID"),
  effectiveAt = java.time.Instant.parse(sys.env("EFFECTIVE_AT"))
)

pipeline.executeOrThrow(request)
```

Floe performs no automatic flow or whole-job retry. Airflow, Step Functions, or another host owns scheduling and mutual exclusion; leave platform retries disabled unless the complete application has been proven repeatable. See [Failure Handling and Production Operations](https://giacomoaccursi.github.io/Floe/guides/recovery/).

## Deployment targets

Floe runs locally and on compatible Spark 3.5 clusters. Managed platforms require matching Spark, Scala, and Iceberg artifacts plus platform-specific catalog configuration:

- **AWS Glue** — supported through the built-in Glue catalog provider; align Floe's Iceberg dependencies with the Glue runtime
- **Amazon EMR** — supported through `spark-submit` with an explicitly compatible Iceberg runtime
- **Databricks** — not currently a zero-configuration target; Unity Catalog requires a dedicated provider, an explicit maintenance-ownership decision, and integration testing against the selected runtime

See the [Cloud Deployment Guide](https://giacomoaccursi.github.io/Floe/guides/cloud-deployment/) for platform-specific examples.

## Documentation

Full documentation at **[giacomoaccursi.github.io/Floe](https://giacomoaccursi.github.io/Floe/)**

- [Quickstart](https://giacomoaccursi.github.io/Floe/getting-started/quickstart/) — first pipeline in 5 minutes
- [Configuration Reference](https://giacomoaccursi.github.io/Floe/configuration/overview/) — all YAML settings
- [Validation Engine](https://giacomoaccursi.github.io/Floe/guides/validation/) — all rule types
- [Iceberg Integration](https://giacomoaccursi.github.io/Floe/guides/iceberg/) — write modes, snapshots, maintenance
- [Pipeline Builder API](https://giacomoaccursi.github.io/Floe/guides/pipeline-builder/) — programmatic configuration
- [Failure Handling and Production Operations](https://giacomoaccursi.github.io/Floe/guides/recovery/) — partial commits, unknown outcomes, retry policy, and incident response

## License

[Apache License 2.0](LICENSE)

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
