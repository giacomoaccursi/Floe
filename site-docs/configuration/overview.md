# Configuration Overview

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
pattern: "^$$[A-Z]{3}$$"         # resolves to: ^$[A-Z]{3}$
```

## Error handling

If a YAML file has syntax errors, the framework reports the file name and error location. If a required field is missing, the error message names the field and the section. If multiple flow files fail to load, all errors are reported together.

```
[CONFIG_FILE_ERROR] Configuration file error in 'config/flows/orders.yaml':
  Missing required field 'name' at 'root'
```

## Programmatic configuration

As an alternative to YAML files, you can provide configuration objects directly:

```scala
IngestionPipeline.builder()
  .withGlobalConfig(myGlobalConfig)
  .withFlowConfigs(myFlowConfigs)
  .withDomainsConfig(myDomainsConfig)
  .build()
```

You can also mix: provide some configs programmatically and load the rest from the directory. See [Pipeline Builder](../guides/pipeline-builder.md) for details.
