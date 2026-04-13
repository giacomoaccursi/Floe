# Data Sources

The framework reads data through pluggable readers based on the `source.type` in the flow configuration. Two source types are supported: `file` and `jdbc`.

## File sources

Supports CSV, Parquet, JSON, Avro, and ORC formats with optional schema enforcement.

| Format | Description |
|--------|-------------|
| `csv` | Comma-separated values (or custom delimiter) |
| `parquet` | Columnar binary format |
| `json` | JSON lines (one JSON object per line) |
| `avro` | Apache Avro binary format (requires `spark-avro` on classpath) |
| `orc` | Optimized Row Columnar format (included in Spark) |

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

## Glob patterns

The `path` field accepts glob patterns to read multiple files in a single DataFrame:

```yaml
source:
  type: file
  path: "data/orders/orders_*.csv"
  format: csv
  options:
    header: "true"
```

Spark handles glob expansion natively. The `path` can be:

- A single file: `data/orders.csv`
- A directory (reads all files): `data/orders/`
- A glob pattern: `data/orders/orders_*.csv`

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

The framework uses `DataReaderFactory` to create the appropriate reader based on the source type:

| Source type | Reader | Description |
|-------------|--------|-------------|
| `file` | `FileDataReader` | CSV, Parquet, JSON from local or cloud storage |
| `jdbc` | `JDBCDataReader` | Any database with a JDBC driver |

```scala
// Internal usage — requires implicit SparkSession
val reader = DataReaderFactory.create(sourceConfig, Some(schemaConfig))(spark)
val df = reader.read()
```

Unsupported source types throw `UnsupportedOperationException`.

## JDBC sources

The JDBC reader connects to any database that provides a JDBC driver (PostgreSQL, MySQL, Oracle, SQL Server, Redshift, Snowflake, etc.).

```yaml
source:
  type: jdbc
  path: "public.customers"
  options:
    url: "jdbc:postgresql://host:5432/mydb"
    user: "${DB_USER}"
    password: "${DB_PASSWORD}"
    driver: "org.postgresql.Driver"
```

Key options:

| Option | Description |
|--------|-------------|
| `url` | JDBC connection URL (required) |
| `user` | Database username |
| `password` | Database password |
| `driver` | JDBC driver class name |
| `query` | Custom SQL query (overrides `path` as table name) |
| `fetchSize` | Number of rows fetched per round-trip |
| `partitionColumn` | Column for parallel reads (must be numeric/date) |
| `lowerBound` / `upperBound` | Range for partition column |
| `numPartitions` | Number of parallel JDBC connections |

All options are passed through to the Spark JDBC datasource. The framework does not bundle database drivers — add the appropriate driver JAR to your classpath.

Credentials should not be hardcoded in YAML. Use variable substitution (`${DB_PASSWORD}`) and resolve them at runtime via `withVariables()` in the pipeline builder, sourcing values from your secret manager.

## Custom readers

You can register custom data readers for source types not built into the framework. This lets you read from any source without modifying the framework code.

```scala
import com.etl.framework.io.readers.{DataReader, DataReaderFactory}
import com.etl.framework.config.{SourceConfig, SchemaConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

val s3SelectReader: DataReaderFactory.ReaderFactory = (config, schema, spark) => {
  new DataReader {
    override def read(): DataFrame = {
      spark.read
        .format("s3selectCSV")
        .options(config.options)
        .load(config.path)
    }
  }
}

IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withDataReader("s3select", s3SelectReader)
  .build()
  .execute()
```

Then in the flow YAML:

```yaml
source:
  type: s3select
  path: "s3://bucket/data/large-file.csv"
  options:
    compression: "GZIP"
```

The factory receives the full `SourceConfig`, optional `SchemaConfig`, and the `SparkSession`. Custom readers override built-in readers if the same type name is used.

## Related

- [Flow Configuration](../configuration/flows.md) — full flow YAML reference
- [Validation Engine](../guides/validation.md) — what happens after data is read
- [Architecture: Data Flow](../architecture/data-flow.md) — the complete pipeline
