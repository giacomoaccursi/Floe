# Quickstart

A minimal pipeline that reads a CSV, validates it, and writes to Iceberg — in 5 minutes.

## 1. Create the config directory

```
config/
├── global.yaml
├── domains.yaml
└── flows/
    └── customers.yaml
```

## 2. global.yaml

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
  parallelFlows: false
  parallelNodes: false

iceberg:
  catalogType: "hadoop"
  catalogName: "spark_catalog"
  warehouse: "output/warehouse"
  fileFormat: "parquet"
  enableSnapshotTagging: true
  maintenance:
    enableSnapshotExpiration: true
    snapshotRetentionDays: 7
    enableCompaction: true
    targetFileSizeMb: 128
    enableOrphanCleanup: true
    orphanRetentionMinutes: 1440
    enableManifestRewrite: false
```

## 3. domains.yaml

```yaml
domains: {}
```

An empty domains file is valid — domains are only needed for domain validation rules.

## 4. flows/customers.yaml

```yaml
name: customers
description: "Customer master data"
version: "1.0"
owner: data-team

source:
  type: file
  path: "data/customers.csv"
  format: csv
  options:
    header: "true"

schema:
  enforceSchema: true
  allowExtraColumns: false
  columns:
    - name: customer_id
      type: integer
      nullable: false
      description: "Unique customer ID"
    - name: name
      type: string
      nullable: false
      description: "Customer name"
    - name: email
      type: string
      nullable: true
      description: "Email address"

loadMode:
  type: full

validation:
  primaryKey: [customer_id]
  foreignKeys: []
  rules:
    - type: regex
      column: email
      pattern: "^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"
      skipNull: true
      onFailure: reject

output: {}
```

## 5. Create sample data

`data/customers.csv`:

```csv
customer_id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
3,Charlie,invalid-email
4,Diana,diana@example.com
```

## 6. Run the pipeline

```scala
import com.etl.framework.pipeline.IngestionPipeline
import org.apache.spark.sql.SparkSession

object QuickstartApp extends App {
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Quickstart")
    .master("local[*]")
    .config("spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()

  val result = IngestionPipeline.builder()
    .withConfigDirectory("config")
    .build()
    .execute()

  println(s"Batch: ${result.batchId}, Success: ${result.success}")
  result.flowResults.foreach { fr =>
    println(f"  ${fr.flowName}: ${fr.validRecords} valid, ${fr.rejectedRecords} rejected")
  }

  spark.stop()
}
```

## What happens

1. The framework reads `customers.csv` and applies the schema
2. Validates: PK uniqueness on `customer_id`, regex on `email`
3. Charlie's row is rejected (invalid email) and written to `output/rejected/`
4. The 3 valid rows are written to the Iceberg table `spark_catalog.default.customers`
5. The snapshot is tagged with the batch ID for time travel

## Next steps

- [Project Setup](project-setup.md) — full project structure
- [Configuration Overview](../configuration/overview.md) — all config options
- [Validation Engine](../guides/validation.md) — all validation rule types
