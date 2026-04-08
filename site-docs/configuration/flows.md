# Flow Configuration

Complete reference for flow YAML files. Each file defines a single data ingestion pipeline and lives in the `flows/` subdirectory of the config directory.

!!!warning "Unique flow names"
    Each flow must have a unique `name`. If two YAML files in the `flows/` directory define the same name, the framework rejects the configuration at startup.

## Full example

```yaml
name: orders
description: "Customer orders"
version: "1.0"
owner: data-team

source:
  type: file
  path: "data/orders_*.csv"
  format: csv
  options:
    header: "true"

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
    - name: order_date
      type: date
      nullable: false
      description: "Date the order was placed"

loadMode:
  type: delta

validation:
  primaryKey: [order_id]
  foreignKeys:
    - columns: [customer_id]
      references:
        flow: customers
        columns: [customer_id]
      onOrphan: warn
  rules:
    - type: regex
      column: email
      pattern: "^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"
      onFailure: reject

output:
  icebergPartitions:
    - "month(order_date)"
  sortOrder:
    - order_date
  tableProperties:
    write.merge.mode: "merge-on-read"
```

## Top-level fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | yes | — | Flow name — maps to the Iceberg table name |
| `description` | no | `""` | Human-readable description |
| `version` | no | `""` | Flow version string |
| `owner` | no | `""` | Team or person responsible |
| `dependsOn` | no | `[]` | List of flow names that must execute before this flow |
| `maxRejectionRate` | no | — | Per-flow rejection rate threshold. Overrides the global `processing.maxRejectionRate`. |

### dependsOn

Declares explicit execution dependencies on other flows. Use this when a flow's transformation needs data from another flow (via `ctx.getFlow()`) but there is no FK relationship between them.

```yaml
name: order_report
dependsOn: [customers, products]
```

Dependencies declared via `dependsOn` are merged with FK-inferred dependencies. The framework validates that all referenced flow names exist and rejects self-references. Circular dependencies (including via `dependsOn`) are detected at startup.

## source

Defines where data is read from.

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `type` | no | `file` | Source type: `file` or `jdbc` |
| `path` | yes | — | For `file`: path to data (file, directory, or glob). For `jdbc`: table name (e.g. `public.customers`) |
| `format` | for `file` | — | File format: `csv`, `parquet`, `json`, `avro`, `orc`. Not used for `jdbc`. |
| `options` | no | `{}` | Options passed to the Spark reader |

### File source

```yaml
source:
  type: file
  path: "data/orders/"
  format: csv
  options:
    header: "true"
    delimiter: ";"
```

These are standard [Spark CSV reader options](https://spark.apache.org/docs/latest/sql-data-sources-csv.html). For Parquet and JSON, options are rarely needed.

### JDBC source

```yaml
source:
  type: jdbc
  path: "public.customers"
  options:
    url: "jdbc:postgresql://${DB_HOST}:5432/${DB_NAME}"
    user: "${DB_USER}"
    password: "${DB_PASSWORD}"
    driver: "org.postgresql.Driver"
    fetchSize: "10000"
```

To read a custom query instead of a full table:

```yaml
source:
  type: jdbc
  path: "public.customers"
  options:
    url: "jdbc:postgresql://host:5432/mydb"
    user: "${DB_USER}"
    password: "${DB_PASSWORD}"
    query: "SELECT * FROM customers WHERE updated_at > '${LAST_BATCH_DATE}'"
```

When `query` is provided, `path` is ignored. The query is wrapped as a subquery for Spark.

For parallel reads on large tables, add Spark JDBC partitioning options:

```yaml
options:
  partitionColumn: "id"
  lowerBound: "1"
  upperBound: "1000000"
  numPartitions: "10"
```

The JDBC driver must be on the classpath. The framework does not bundle any database drivers.

For details on file formats, see [Data Sources](../guides/data-sources.md).

## schema

Defines the expected schema and controls enforcement during read.

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `enforceSchema` | yes | — | If true, validate column presence and apply Spark schema during read |
| `allowExtraColumns` | yes | — | If false, reject data with columns not defined in `columns` |
| `columns` | yes | — | List of column definitions |

### Column definition

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | yes | — | Column name |
| `type` | yes | — | Data type (see table below) |
| `nullable` | yes | — | Whether the column allows NULL values. `false` triggers not-null validation. |
| `description` | no | `""` | Human-readable description |
| `sourceColumn` | no | — | Original column name in the source data. When set, the framework renames this column to `name` before validation. |

### Column types

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

!!!tip "Decimal precision"
    Use `decimal(p, s)` for financial amounts: `decimal(10, 2)` gives 10 total digits with 2 decimal places. Plain `decimal` defaults to `(38, 18)` which may be more precision than needed.

## loadMode

Controls how data is written to the Iceberg table.

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `type` | yes | — | Load mode: `full`, `delta`, `scd2` |
| `compareColumns` | SCD2 only | — | Columns used for SCD2 change detection. For delta mode, all non-PK columns are compared automatically. |
| `validFromColumn` | no | `valid_from` | SCD2 valid-from timestamp column |
| `validToColumn` | no | `valid_to` | SCD2 valid-to timestamp column |
| `isCurrentColumn` | no | `is_current` | SCD2 current-version flag column |
| `detectDeletes` | no | `false` | SCD2: soft-delete records absent from source |
| `isActiveColumn` | no | `is_active` | SCD2: active flag column (used with `detectDeletes`) |

- **full** — replaces all data atomically each batch
- **delta** — upsert via MERGE INTO with value-based change detection
- **scd2** — maintains full history with versioned rows

For SCD2 details, see [SCD2 Guide](../guides/scd2.md). For Iceberg write mechanics, see [Iceberg Integration](../guides/iceberg.md).

## validation

Defines data quality rules. For the complete validation reference, see [Validation Engine](../guides/validation.md).

```yaml
validation:
  primaryKey: [order_id]
  foreignKeys:
    - columns: [customer_id]
      references:
        flow: customers
        columns: [customer_id]
      onOrphan: warn
  rules:
    - type: regex
      column: email
      pattern: "^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"
      onFailure: reject
```

The entire `validation` section is optional. If omitted, no validation is performed. `primaryKey` must be non-empty for SCD2 and delta modes (the framework validates this at startup).

### Foreign key fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `columns` | list | yes | — | Column(s) in the current flow |
| `references.flow` | string | yes | — | Name of the parent flow |
| `references.columns` | list | yes | — | Column(s) in the parent flow (same order as `columns`) |
| `onOrphan` | string | — | `warn` | Post-batch orphan action: `warn`, `delete`, `ignore`. See [Orphan Detection](../guides/orphan-detection.md). |

The FK is identified by an auto-generated display name (e.g. `(customer_id) -> customers.(customer_id)`).

### Rule fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | yes | — | Rule type: `regex`, `range`, `domain`, `custom` |
| `column` | string | conditional | — | Column to validate. Required for `regex`, `range`, `domain`. Optional for `custom` (custom validators can do cross-column validation). |
| `pattern` | string | — | — | Regex pattern (for `regex` type) |
| `min` | string | — | — | Minimum value (for `range` type) |
| `max` | string | — | — | Maximum value (for `range` type) |
| `domainName` | string | — | — | Domain name from `domains.yaml` (for `domain` type) |
| `class` | string | — | — | Validator name (registered via builder) or fully qualified class name (for `custom` type) |
| `config` | map | — | — | Key-value config passed to custom validators |
| `description` | string | — | — | Human-readable description |
| `skipNull` | boolean | — | `true` | If true, NULL values pass without being checked |
| `onFailure` | string | — | `reject` | Action on failure: `reject`, `warn`, `skip` |

## output

Controls how validated data is written.

| Field | Default | Description |
|-------|---------|-------------|
| `rejectedPath` | — | Path for rejected records (overrides global `rejectedPath`) |
| `sortOrder` | `[]` | Iceberg write sort order columns |
| `icebergPartitions` | `[]` | Iceberg partition expressions |
| `tableProperties` | `{}` | Iceberg table properties |

### Partition transforms

Supported transforms for `icebergPartitions`:

| Transform | Example | Description |
|-----------|---------|-------------|
| identity | `country` | Partition by raw column value |
| `year(col)` | `year(order_date)` | Partition by year |
| `month(col)` | `month(order_date)` | Partition by year-month |
| `day(col)` | `day(created_at)` | Partition by date |
| `hour(col)` | `hour(event_time)` | Partition by hour |
| `bucket(n, col)` | `bucket(16, customer_id)` | Hash-partition into n buckets |
| `truncate(len, col)` | `truncate(3, zip_code)` | Partition by truncated value |

Transforms are case-insensitive.

!!!warning "Partitioning guidelines"
    - Partition on low-cardinality temporal columns (`month(order_date)`, `year(created_at)`)
    - Do **not** partition on high-cardinality columns (IDs, timestamps with seconds/milliseconds)
    - Do **not** partition on boolean columns (`is_current`) — only 2 values, creates severely imbalanced partitions

### Sort order

`sortOrder` controls data layout within files via `WRITE ORDERED BY`. This improves scan performance without affecting query semantics:

```yaml
output:
  sortOrder:
    - order_date
    - customer_id
```

### Table properties

Pass Iceberg table properties per-flow:

```yaml
output:
  tableProperties:
    write.merge.mode: "merge-on-read"
    write.format.default: "parquet"
    commit.retry.num-retries: "4"
```
