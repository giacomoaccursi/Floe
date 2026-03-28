# Configuration Reference

## Directory structure

The framework loads configuration from a directory with this layout:

```
config/
├── global.yaml          # Global settings (paths, processing, performance, Iceberg)
├── domains.yaml         # Domain value definitions for validation
└── flows/
    ├── customers.yaml   # One file per flow
    ├── orders.yaml
    └── order_items.yaml
```

Pass the directory to the pipeline builder:

```scala
IngestionPipeline.builder()
  .withConfigDirectory("config")
  .build()
```

All three sections (`global.yaml`, `domains.yaml`, `flows/`) are required when using directory-based loading. Flow files must have `.yaml` or `.yml` extension.

## Naming convention

All YAML field names use camelCase to match the Scala case class fields directly:

```yaml
# Correct
loadMode:
  type: delta
  compareColumns: [status, amount]

# Wrong — will not be recognized
load_mode:
  type: delta
  compare_columns: [status, amount]
```

## Error handling

If a YAML file has syntax errors, the framework reports the file name and error location. If a required field is missing, the error message names the field and the section. If multiple flow files fail to load, all errors are reported together.

```
[CONFIG_FILE_ERROR] Configuration file error in 'config/flows/orders.yaml':
  Missing required field 'name' at 'root'
```

## Variable substitution

YAML files support variable references with `${VAR_NAME}` or `$VAR_NAME` syntax:

```yaml
paths:
  outputPath: "${OUTPUT_PATH}/data"
  rejectedPath: "${OUTPUT_PATH}/rejected"

source:
  path: "$DATA_DIR/customers.csv"
```

### Resolution order

Variables are resolved in this order:

1. Explicit variables passed via `withVariables()` on the pipeline builder
2. System environment variables (`sys.env`)

If a variable is not found in either source, the config loader fails with an error listing all unresolved variables.

### withVariables for cloud environments

On managed platforms (AWS Glue, Databricks, EMR) where parameters don't arrive as environment variables, use `withVariables`:

```scala
// AWS Glue example
val args = GlueArgParser.getResolvedOptions(sysArgs,
  Seq("data_dir", "output_path", "warehouse_path"))

IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withVariables(Map(
    "DATA_DIR"       -> args("data_dir"),
    "OUTPUT_PATH"    -> args("output_path"),
    "WAREHOUSE_PATH" -> args("warehouse_path")
  ))
  .build()
  .execute()
```

Explicit variables take priority over environment variables with the same name.

### Escaping dollar signs

Use `$$` to produce a literal `$` in YAML values:

```yaml
description: "Amount in $$USD"   # resolves to: Amount in $USD
pattern: "^$$[0-9]+"             # resolves to: ^$[0-9]+
```

## global.yaml

```yaml
paths:
  outputPath: "output/data"
  rejectedPath: "output/rejected"
  metadataPath: "output/metadata"

processing:
  batchIdFormat: "yyyyMMdd_HHmmss"
  failOnValidationError: false
  maxRejectionRate: 0.1

performance:
  parallelFlows: true
  parallelNodes: true

iceberg:
  catalogType: "hadoop"
  catalogName: "spark_catalog"
  warehouse: "output/warehouse"
  fileFormat: "parquet"
  formatVersion: 2
  enableSnapshotTagging: true
  catalogProperties: {}
  maintenance:
    enableSnapshotExpiration: true
    snapshotRetentionDays: 7
    enableCompaction: true
    targetFileSizeMb: 128
    enableOrphanCleanup: true
    orphanRetentionMinutes: 1440
    enableManifestRewrite: false
```

### paths

| Field | Required | Description |
|-------|----------|-------------|
| `outputPath` | yes | Base directory for flow output data |
| `rejectedPath` | yes | Directory for rejected records |
| `metadataPath` | yes | Directory for batch and flow metadata JSON |

### processing

| Field | Default | Description |
|-------|---------|-------------|
| `batchIdFormat` | — | Java `DateTimeFormatter` pattern for batch ID generation (e.g. `yyyyMMdd_HHmmss` → `20260328_150000`) |
| `failOnValidationError` | `false` | Stop batch execution when any flow has rejected records |
| `maxRejectionRate` | `0.1` | Rejection rate threshold (0.1 = 10%). See [validation-engine.md](validation-engine.md#batch-level-rejection-behavior) |

### performance

| Field | Default | Description |
|-------|---------|-------------|
| `parallelFlows` | `false` | Execute independent flows (no FK dependency) in parallel |
| `parallelNodes` | `false` | Execute independent DAG nodes in parallel |

### iceberg

See [iceberg-integration.md](iceberg-integration.md) for the complete Iceberg configuration reference.

| Field | Default | Description |
|-------|---------|-------------|
| `catalogType` | `hadoop` | Catalog implementation: `hadoop`, `glue` |
| `catalogName` | `spark_catalog` | Catalog name used in SQL queries |
| `warehouse` | — (required) | Path to the Iceberg warehouse directory |
| `catalogProperties` | `{}` | Additional key-value properties passed to the catalog provider |
| `fileFormat` | `parquet` | Default data file format |
| `formatVersion` | `2` | Iceberg format version. Must be `1` or `2`. Version 2 is required for row-level operations (MERGE INTO) — delta and SCD2 load modes fail on version 1 |
| `enableSnapshotTagging` | `true` | Tag each batch snapshot for time travel |

## domains.yaml

Defines named sets of allowed values for domain validation rules.

```yaml
domains:
  order_status:
    name: order_status
    description: "Valid order statuses"
    values: ["pending", "confirmed", "shipped", "delivered", "cancelled"]
    caseSensitive: false

  country_code:
    name: country_code
    description: "ISO 3166-1 alpha-2 country codes"
    values: ["US", "GB", "DE", "FR", "IT", "ES"]
    caseSensitive: true
```

| Field | Default | Description |
|-------|---------|-------------|
| `name` | — | Domain identifier (must match the map key) |
| `description` | — | Human-readable description |
| `values` | — | List of allowed values |
| `caseSensitive` | `true` | If false, comparison is case-insensitive |

Referenced in flow validation rules via `domainName`. See [validation-engine.md](validation-engine.md#domain-validation).

## Flow YAML

Each flow file defines a single data ingestion pipeline.

```yaml
name: orders
description: "Customer orders"
version: "1.0"
owner: data-team

source:
  type: file
  path: "data/orders.csv"
  format: csv
  options:
    header: "true"
  filePattern: "orders_*.csv"    # optional glob pattern

schema:
  enforceSchema: true
  allowExtraColumns: false
  columns:
    - name: order_id
      type: integer
      nullable: false
      description: "Unique order identifier"
    - name: total_amount
      type: "decimal(10, 2)"
      nullable: false
      description: "Order total"

loadMode:
  type: delta

validation:
  primaryKey: [order_id]
  foreignKeys: []
  rules: []

output:
  icebergPartitions:
    - "month(order_date)"
  sortOrder:
    - order_date
  tableProperties:
    write.merge.mode: "merge-on-read"
```

### source

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `type` | yes | — | Source type: `file`, `jdbc` |
| `path` | yes | — | Path to source data (for file) or JDBC URL (for jdbc) |
| `format` | yes | — | File format: `csv`, `parquet`, `json`. Ignored for JDBC. |
| `options` | no | `{}` | Format-specific options passed to the Spark reader |
| `filePattern` | no | — | Glob pattern for file matching (appended to `path`) |

Common options for CSV: `header: "true"`, `delimiter: ";"`, `quote: "\""`, `escape: "\\"`, `nullValue: ""`.

JDBC source example:

```yaml
source:
  type: jdbc
  path: ""
  format: csv    # ignored for JDBC, but required by schema
  options:
    url: "jdbc:postgresql://host:5432/mydb"
    dbtable: "public.customers"
    user: "${DB_USER}"
    password: "${DB_PASSWORD}"
    driver: "org.postgresql.Driver"
```

### schema

| Field | Default | Description |
|-------|---------|-------------|
| `enforceSchema` | — | If true, validate column presence and apply Spark schema during read |
| `allowExtraColumns` | — | If false, reject data with columns not defined in `columns` |
| `columns` | — | List of column definitions |

Each column has:

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | yes | — | Column name |
| `type` | yes | — | Data type (see table below) |
| `nullable` | yes | — | Whether the column allows NULL values. `false` triggers not-null validation. |
| `default` | no | — | Default value (string). Not currently applied during read — reserved for future use. |
| `description` | yes | — | Human-readable description |

#### Column types

| Type | Spark type | Notes |
|------|-----------|-------|
| `string`, `varchar`, `text` | `StringType` | |
| `int`, `integer` | `IntegerType` | |
| `long`, `bigint` | `LongType` | |
| `float` | `FloatType` | |
| `double` | `DoubleType` | |
| `boolean`, `bool` | `BooleanType` | |
| `date` | `DateType` | |
| `timestamp`, `datetime` | `TimestampType` | |
| `decimal` | `DecimalType(38, 18)` | Default precision |
| `decimal(p, s)` | `DecimalType(p, s)` | Custom precision, e.g. `decimal(10, 2)` |
| `binary` | `BinaryType` | |
| `byte`, `tinyint` | `ByteType` | |
| `short`, `smallint` | `ShortType` | |

### loadMode

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `type` | yes | — | Load mode: `full`, `delta`, `scd2` |
| `compareColumns` | SCD2 only | — | Columns used for SCD2 change detection. For delta mode, all non-PK columns are compared automatically. |
| `validFromColumn` | no | `valid_from` | SCD2 valid-from timestamp column |
| `validToColumn` | no | `valid_to` | SCD2 valid-to timestamp column |
| `isCurrentColumn` | no | `is_current` | SCD2 current-version flag column |
| `detectDeletes` | no | `false` | SCD2: soft-delete records absent from source |
| `isActiveColumn` | no | `is_active` | SCD2: active flag column (used with `detectDeletes`) |

See [scd2.md](scd2.md) for complete SCD2 documentation.

### validation

```yaml
validation:
  primaryKey: [order_id]
  foreignKeys:
    - name: fk_customer
      column: customer_id
      references:
        flow: customers
        column: customer_id
      onOrphan: warn
  rules:
    - type: regex
      column: email
      pattern: "^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$$"
      onFailure: reject
```

`primaryKey` is required (the framework throws an error if empty). `foreignKeys` and `rules` can be empty lists. For the complete validation reference including all rule types, skipNull, onFailure behavior, and rejection metadata, see [validation-engine.md](validation-engine.md).

### output

| Field | Default | Description |
|-------|---------|-------------|
| `path` | — | Output path (optional, framework uses Iceberg table) |
| `rejectedPath` | — | Path for rejected records (overrides global `rejectedPath`) |
| `format` | `parquet` | Output format |
| `partitionBy` | `[]` | Partition columns (non-Iceberg output) |
| `compression` | `snappy` | Compression codec |
| `options` | `{}` | Additional write options |
| `sortOrder` | `[]` | Iceberg write sort order columns |
| `icebergPartitions` | `[]` | Iceberg partition expressions |
| `tableProperties` | `{}` | Iceberg table properties (e.g. `write.merge.mode: merge-on-read`) |

Supported partition transforms for `icebergPartitions`:

| Transform | Example | Description |
|-----------|---------|-------------|
| identity | `country` | Partition by raw column value |
| `year(col)` | `year(order_date)` | Partition by year |
| `month(col)` | `month(order_date)` | Partition by year-month |
| `day(col)` | `day(created_at)` | Partition by date |
| `hour(col)` | `hour(event_time)` | Partition by hour |
| `bucket(n, col)` | `bucket(16, customer_id)` | Hash-partition into n buckets |
| `truncate(len, col)` | `truncate(3, zip_code)` | Partition by truncated value |

Transforms are case-insensitive. Do not partition on high-cardinality columns (IDs, fine-grained timestamps).

## DAG YAML

See [dag-aggregation.md](dag-aggregation.md) for the complete DAG configuration reference.
