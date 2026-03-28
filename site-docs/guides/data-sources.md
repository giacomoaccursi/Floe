# Data Sources

Guide to file-based data readers. The framework reads data through `FileDataReader`, which supports CSV, Parquet, and JSON formats with optional schema enforcement.

## Supported formats

| Format | Description |
|--------|-------------|
| `csv` | Comma-separated values (or custom delimiter) |
| `parquet` | Columnar binary format |
| `json` | JSON lines (one JSON object per line) |

Unsupported formats throw an `UnsupportedOperationException` listing the valid options.

## CSV

CSV is the most common format for file-based ingestion. Configure it in the flow's `source` section:

```yaml
source:
  type: file
  path: "data/orders.csv"
  format: csv
  options:
    header: "true"
    delimiter: ","
```

### Common CSV options

All options are passed directly to Spark's CSV reader via `spark.read.options()`:

| Option | Default | Description |
|--------|---------|-------------|
| `header` | `false` | First line contains column names |
| `delimiter` | `,` | Field separator character |
| `quote` | `"` | Quote character for fields containing delimiters |
| `escape` | `\` | Escape character inside quoted fields |
| `nullValue` | (empty) | String representation of NULL |
| `encoding` | `UTF-8` | Character encoding |
| `multiLine` | `false` | Allow records spanning multiple lines |
| `dateFormat` | `yyyy-MM-dd` | Date parsing format |
| `timestampFormat` | `yyyy-MM-dd'T'HH:mm:ss` | Timestamp parsing format |
| `inferSchema` | `false` | Infer column types (not recommended — use `enforceSchema` instead) |

!!!tip
    Always set `header: "true"` for CSV files with column headers. Without it, Spark generates column names like `_c0`, `_c1`, etc.

### Example: semicolon-delimited with custom null

```yaml
source:
  type: file
  path: "data/european_orders.csv"
  format: csv
  options:
    header: "true"
    delimiter: ";"
    nullValue: "N/A"
    encoding: "ISO-8859-1"
```

## Parquet

Parquet files carry their own schema, so fewer options are needed:

```yaml
source:
  type: file
  path: "data/orders.parquet"
  format: parquet
```

Parquet options are rarely needed since the format is self-describing. If `enforceSchema` is `true`, the framework applies the configured schema during read, which can cast types or reorder columns.

## JSON

JSON lines format — one JSON object per line:

```yaml
source:
  type: file
  path: "data/events.json"
  format: json
```

### Common JSON options

| Option | Default | Description |
|--------|---------|-------------|
| `multiLine` | `false` | Parse multi-line JSON (single object spanning lines) |
| `dateFormat` | `yyyy-MM-dd` | Date parsing format |
| `timestampFormat` | `yyyy-MM-dd'T'HH:mm:ss` | Timestamp parsing format |

## File patterns

Use `filePattern` to read multiple files matching a glob pattern:

```yaml
source:
  type: file
  path: "data/orders"
  format: csv
  options:
    header: "true"
  filePattern: "orders_*.csv"
```

The pattern is appended to `path`, so the effective path becomes `data/orders/orders_*.csv`. This reads all files matching the glob in a single DataFrame.

If `filePattern` is not set, the `path` is used directly — it can point to a single file or a directory (Spark reads all files in a directory by default).

## Schema enforcement

When `schema.enforceSchema` is `true` in the flow config, the framework converts the `schema.columns` definition to a Spark `StructType` and applies it during read:

```yaml
schema:
  enforceSchema: true
  columns:
    - name: order_id
      type: integer
      nullable: false
      description: "PK"
    - name: total
      type: "decimal(10, 2)"
      nullable: false
      description: "Order total"
```

This has two effects:

1. **Type casting** — columns are read with the specified Spark types instead of being inferred. For CSV, this prevents everything from being read as strings.
2. **Column selection** — only the declared columns are read. Extra columns in the source file are ignored at the reader level (but may still be caught by `allowExtraColumns` validation).

### Type mapping

The reader maps string type names to Spark `DataType` instances:

| Type string | Spark type |
|-------------|-----------|
| `string`, `varchar`, `text` | `StringType` |
| `int`, `integer` | `IntegerType` |
| `long`, `bigint` | `LongType` |
| `float` | `FloatType` |
| `double` | `DoubleType` |
| `boolean`, `bool` | `BooleanType` |
| `date` | `DateType` |
| `timestamp`, `datetime` | `TimestampType` |
| `decimal` | `DecimalType(38, 18)` |
| `decimal(p, s)` | `DecimalType(p, s)` |
| `binary` | `BinaryType` |
| `byte`, `tinyint` | `ByteType` |
| `short`, `smallint` | `ShortType` |

Unrecognized type strings throw `UnsupportedOperationException` with the list of supported types.

!!!note "Decimal format"
    The decimal type accepts two forms: `decimal` (defaults to precision 38, scale 18) or `decimal(p, s)` with explicit precision and scale. Example: `decimal(10, 2)` for monetary amounts.

## DataReader factory

The framework uses `DataReaderFactory` to create the appropriate reader based on the source type. Currently only `file` is supported:

```scala
val reader = DataReaderFactory.create(sourceConfig, Some(schemaConfig))
val df = reader.read()
```

Unsupported source types throw `UnsupportedOperationException`.

## Related

- [Flow Configuration](../configuration/flows.md) — full flow YAML reference
- [Validation Engine](../guides/validation.md) — what happens after data is read
- [Architecture: Data Flow](../architecture/data-flow.md) — the complete pipeline
