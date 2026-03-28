# Configuration Reference

Single-page reference with all YAML fields. For detailed explanations, follow the links to the dedicated pages.

## global.yaml

### paths

| Field | Required | Description |
|-------|----------|-------------|
| `outputPath` | yes | Base directory for flow output data |
| `rejectedPath` | yes | Directory for rejected records |
| `metadataPath` | yes | Directory for batch and flow metadata JSON |

### processing

| Field | Default | Description |
|-------|---------|-------------|
| `batchIdFormat` | — | Java `DateTimeFormatter` pattern for batch ID (e.g. `yyyyMMdd_HHmmss`) |
| `failOnValidationError` | `false` | Stop batch when any flow has rejected records |
| `maxRejectionRate` | `0.1` | Rejection rate threshold (0.1 = 10%) |

### performance

| Field | Default | Description |
|-------|---------|-------------|
| `parallelFlows` | `false` | Execute independent flows in parallel |
| `parallelNodes` | `false` | Execute independent DAG nodes in parallel |

### iceberg

| Field | Default | Description |
|-------|---------|-------------|
| `catalogType` | `hadoop` | Catalog: `hadoop`, `glue` |
| `catalogName` | `spark_catalog` | Catalog name for SQL queries |
| `warehouse` | — (required) | Iceberg warehouse directory |
| `fileFormat` | `parquet` | Default data file format |
| `enableSnapshotTagging` | `true` | Tag snapshots with batch ID |
| `catalogProperties` | `{}` | Additional catalog key-value properties |

### iceberg.maintenance

| Field | Default | Description |
|-------|---------|-------------|
| `enableSnapshotExpiration` | `true` | Expire old snapshots |
| `snapshotRetentionDays` | `7` | Days to retain snapshots |
| `enableCompaction` | `true` | Compact small files |
| `targetFileSizeMb` | `128` | Target file size after compaction |
| `enableOrphanCleanup` | `true` | Remove orphaned files |
| `orphanRetentionMinutes` | `1440` | Grace period (min 1440) |
| `enableManifestRewrite` | `false` | Rewrite manifest files |

→ [Global Configuration](../configuration/global.md)

## domains.yaml

| Field | Default | Description |
|-------|---------|-------------|
| `name` | — | Domain identifier (must match map key) |
| `description` | — | Human-readable description |
| `values` | — | List of allowed values |
| `caseSensitive` | `true` | Case-insensitive comparison if false |

→ [Domains Configuration](../configuration/domains.md)

## Flow YAML

### Top-level

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Flow name (maps to Iceberg table) |
| `description` | no | Human-readable description |
| `version` | no | Flow version |
| `owner` | no | Responsible team/person |

### source

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `type` | yes | — | Source type: `file` |
| `path` | yes | — | Path to source data |
| `format` | yes | — | File format: `csv`, `parquet`, `json` |
| `options` | no | `{}` | Format-specific Spark reader options |
| `filePattern` | no | — | Glob pattern appended to path |

### schema

| Field | Default | Description |
|-------|---------|-------------|
| `enforceSchema` | — | Validate columns and apply Spark schema during read |
| `allowExtraColumns` | — | Reject data with undeclared columns |
| `columns` | — | List of column definitions |

### schema.columns[]

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | yes | — | Column name |
| `type` | yes | — | Data type |
| `nullable` | yes | — | Allow NULLs (`false` triggers not-null validation) |
| `description` | yes | — | Human-readable description |

### Column types

| Type | Spark type |
|------|-----------|
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

### loadMode

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `type` | yes | — | `full`, `delta`, `scd2` |
| `compareColumns` | SCD2 | — | Columns for change detection |
| `validFromColumn` | no | `valid_from` | SCD2 valid-from column |
| `validToColumn` | no | `valid_to` | SCD2 valid-to column |
| `isCurrentColumn` | no | `is_current` | SCD2 current flag |
| `detectDeletes` | no | `false` | SCD2 soft-delete |
| `isActiveColumn` | no | `is_active` | SCD2 active flag |

### validation

| Field | Required | Description |
|-------|----------|-------------|
| `primaryKey` | yes | List of PK columns (must not be empty) |
| `foreignKeys` | no | List of FK definitions |
| `rules` | no | List of validation rules |

### validation.foreignKeys[]

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | yes | — | FK constraint name |
| `column` | yes | — | Column in current flow |
| `references.flow` | yes | — | Parent flow name |
| `references.column` | yes | — | Parent column |
| `onOrphan` | no | `warn` | `warn`, `delete`, `ignore` |

### validation.rules[]

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `type` | yes | — | `regex`, `range`, `domain`, `custom` |
| `column` | yes* | — | Column to validate |
| `pattern` | regex | — | Regex pattern |
| `min` | range | — | Minimum value |
| `max` | range | — | Maximum value |
| `domainName` | domain | — | Domain from domains.yaml |
| `class` | custom | — | Fully qualified class name |
| `config` | no | — | Key-value config for custom validators |
| `description` | no | — | Human-readable description |
| `skipNull` | no | `true` | NULLs pass without checking |
| `onFailure` | no | `reject` | `reject`, `warn`, `skip` |

### output

| Field | Default | Description |
|-------|---------|-------------|
| `rejectedPath` | — | Override global rejectedPath |
| `sortOrder` | `[]` | Iceberg write sort order |
| `icebergPartitions` | `[]` | Iceberg partition expressions |
| `tableProperties` | `{}` | Iceberg table properties |

### Partition transforms

| Transform | Example | Description |
|-----------|---------|-------------|
| identity | `country` | Raw column value |
| `year(col)` | `year(order_date)` | By year |
| `month(col)` | `month(order_date)` | By year-month |
| `day(col)` | `day(created_at)` | By date |
| `hour(col)` | `hour(event_time)` | By hour |
| `bucket(n, col)` | `bucket(16, customer_id)` | Hash into n buckets |
| `truncate(len, col)` | `truncate(3, zip_code)` | By truncated value |

→ [Flow Configuration](../configuration/flows.md)

## DAG YAML

### Top-level

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | DAG name |
| `description` | no | Description |
| `version` | no | Version |
| `nodes` | yes | List of node definitions |

### nodes[]

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `id` | yes | — | Unique node ID |
| `description` | yes | — | Description |
| `sourceFlow` | yes | — | Source flow name |
| `sourcePath` | yes | — | Path to source data |
| `sourceTable` | no | — | Iceberg table (overrides sourcePath) |
| `dependencies` | yes | — | List of dependency node IDs |
| `join` | no | — | Join configuration |
| `select` | no | all | Columns to select |
| `filters` | no | none | SQL filter expressions |

### nodes[].join

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `type` | yes | — | `inner`, `left_outer`, `right_outer`, `full_outer` |
| `parent` | yes | — | Parent node ID |
| `conditions` | yes | — | Join conditions (left=parent, right=child) |
| `strategy` | yes | — | `nest`, `flatten`, `aggregate` |
| `nestAs` | no | `nested_records` | Nest column name |
| `aggregations` | no | — | Aggregation specs |

### nodes[].join.aggregations[]

| Field | Required | Description |
|-------|----------|-------------|
| `column` | yes | Column to aggregate |
| `function` | yes | `sum`, `count`, `avg`, `min`, `max`, `first`, `last`, `collect_list`, `collect_set` |
| `alias` | yes | Output column name |

→ [DAG Configuration](../configuration/dag.md)
