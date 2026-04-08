# Pipeline Builder & TransformationContext

## Overview

`IngestionPipeline` is the public entry point of the framework. It provides a fluent builder API for configuring and executing ETL pipelines. The builder loads configuration (from YAML files or programmatic objects), registers transformations, configures catalog providers, and produces an executable pipeline.

`TransformationContext` is the immutable context passed to every transformation function. It provides access to the current flow's data, previously validated flows, the batch ID, and the SparkSession.

## IngestionPipeline.builder()

Create a pipeline using the builder pattern:

```scala
implicit val spark: SparkSession = SparkSession.builder()
  .appName("My ETL")
  .master("local[*]")
  .config("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .getOrCreate()

val pipeline = IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withPreValidationTransformation("orders", enrichOrders)
  .withPostValidationTransformation("orders", computeDerivedFields)
  .build()

val result = pipeline.execute()
```

### Builder methods

| Method | Description |
|--------|-------------|
| `withConfigDirectory(path)` | Loads `global.yaml`, `flows/*.yaml`, and optionally `domains.yaml` from the directory |
| `withGlobalConfig(config)` | Sets `GlobalConfig` programmatically (alternative to directory) |
| `withFlowConfigs(configs)` | Sets flow configs programmatically |
| `withDomainsConfig(config)` | Sets `DomainsConfig` programmatically |
| `withFlowTransformation(flow, pre, post)` | Registers pre and/or post-validation transformations for a flow |
| `withPreValidationTransformation(flow, fn)` | Shorthand for pre-validation only |
| `withPostValidationTransformation(flow, fn)` | Shorthand for post-validation only |
| `withCustomValidator(name, factory)` | Registers a custom validator by name. Use the same name in the flow YAML `class` field. |
| `withCatalogProvider(type, provider)` | Registers a custom Iceberg catalog provider |
| `withDerivedTable(name, fn)` | Registers a derived table computed after all flows are written to Iceberg |
| `withDataReader(type, factory)` | Registers a custom data reader for a source type. See [Data Sources — Custom readers](data-sources.md#custom-readers). |
| `withBatchListener(listener)` | Registers a listener notified on batch completion or failure. See [Batch Listeners](batch-listeners.md). |
| `withVariables(variables)` | Sets variables for YAML substitution (priority over env vars). See [Configuration Overview](../configuration/overview.md#variable-substitution) |
| `build()` | Builds the pipeline (returns `IngestionPipeline`) |

The builder does not have an `execute()` method — call `build()` first, then `execute()` on the returned pipeline.

### Configuration loading

The builder supports three configuration modes:

1. **Directory-based** — `withConfigDirectory("config")` loads everything from the directory:
   - `config/global.yaml` → `GlobalConfig`
   - `config/domains.yaml` → `DomainsConfig`
   - `config/flows/*.yaml` → `Seq[FlowConfig]`

2. **Fully programmatic** — provide `withGlobalConfig()` + `withFlowConfigs()` directly. No files are read.

3. **Mixed** — provide some configs programmatically and load the rest from the directory. For example, `withGlobalConfig(myConfig)` + `withConfigDirectory("config")` loads flows and domains from the directory but uses the provided global config.

If neither a config directory nor the required configs are provided, `build()` throws `MissingConfigFieldException`.

## Flow execution order

Flows are not executed in config file order. The framework builds an execution plan based on dependencies:

1. FK references and explicit `dependsOn` declarations are analyzed to build a dependency graph
2. Flows are topologically sorted so that dependencies execute before dependents
3. Independent flows (no dependency relationship) are grouped for potential parallel execution

If `performance.parallelFlows` is `true` in `global.yaml`, independent flows within the same group run in parallel using a bounded thread pool.

```yaml
performance:
  parallelFlows: true
```

This means if flow `orders` has a FK referencing `customers` (or declares `dependsOn: [customers]`), `customers` is guaranteed to execute first — regardless of the order in the YAML files or the `withFlowConfigs()` list.

## IngestionResult

`execute()` returns an `IngestionResult`:

```scala
case class IngestionResult(
  batchId: String,
  flowResults: Seq[FlowResult],
  success: Boolean,
  error: Option[String] = None,
  derivedTableResults: Seq[DerivedTableResult] = Seq.empty
)
```

| Field | Type | Description |
|-------|------|-------------|
| `batchId` | `String` | Batch identifier (formatted according to `processing.batchIdFormat`) |
| `flowResults` | `Seq[FlowResult]` | Results for each executed flow |
| `success` | `Boolean` | `true` if all flows completed without fatal errors |
| `error` | `Option[String]` | Error message if the batch failed |
| `derivedTableResults` | `Seq[DerivedTableResult]` | Results for each derived table (empty if none registered) |

### FlowResult

Each flow produces a `FlowResult`:

| Field | Type | Description |
|-------|------|-------------|
| `flowName` | `String` | Name of the flow |
| `batchId` | `String` | Batch identifier |
| `success` | `Boolean` | Whether the flow completed successfully |
| `inputRecords` | `Long` | Total records read from source (after pre-validation transform) |
| `validRecords` | `Long` | Records that passed validation |
| `rejectedRecords` | `Long` | Records that failed validation |
| `rejectionRate` | `Double` | `rejectedRecords / inputRecords` |
| `mergedRecords` | `Long` | Records after merge (equals inputRecords in current implementation) |
| `executionTimeMs` | `Long` | Flow execution time in milliseconds |
| `rejectionReasons` | `Map[String, Long]` | Count of rejections per validation step |
| `error` | `Option[String]` | Error message if the flow failed |
| `icebergMetadata` | `Option[IcebergFlowMetadata]` | Iceberg snapshot metadata (see [Iceberg Integration](iceberg.md)) |

## Transformations

Transformations are functions of type `TransformationContext => TransformationContext`. They run at two points in the pipeline:

```
Read → PreValidation Transform → Validate → PostValidation Transform → Write
```

### Pre-validation transformations

Run before validation. The `TransformationContext` has `validatedFlows = Map.empty` at this stage — no flows have been validated yet, so `ctx.getFlow()` always returns `None`.

Use cases:

- Data enrichment (add computed columns)
- Data cleansing (trim whitespace, normalize formats)
- Filtering out known-bad records before validation
- Joining with reference data from external sources

```scala
val enrichOrders: FlowTransformation = { ctx =>
  ctx.withData(
    ctx.currentData
      .withColumn("order_year", year(col("order_date")))
      .withColumn("email_lower", lower(col("email")))
  )
}
```

### Post-validation transformations

Run after validation, on the valid records only. The `TransformationContext` now has `validatedFlows` populated with all flows that have been validated so far in this batch.

Use cases:

- Computing derived fields from validated data
- Cross-flow lookups using `ctx.getFlow()`

```scala
val computeDerivedFields: FlowTransformation = { ctx =>
  val customers = ctx.getFlow("customers").get

  ctx.withData(
    ctx.currentData
      .join(broadcast(customers.select("customer_id", "segment")),
        Seq("customer_id"), "left")
      .withColumn("priority",
        when(col("segment") === "premium", lit("high"))
          .otherwise(lit("normal")))
  )
}
```

### Registering transformations

```scala
IngestionPipeline.builder()
  .withConfigDirectory("config")
  // Separate methods
  .withPreValidationTransformation("orders", enrichOrders)
  .withPostValidationTransformation("orders", computeDerived)
  // Or combined
  .withFlowTransformation("customers",
    preValidation = Some(cleanCustomers),
    postValidation = Some(tagCustomers))
  .build()
```

Multiple calls for the same flow merge transformations — a later `withPreValidationTransformation` does not overwrite a previously registered `withPostValidationTransformation`.

## TransformationContext

The context is immutable. Every method that modifies state returns a new instance.

### Available fields

| Field | Type | Description |
|-------|------|-------------|
| `currentFlow` | `String` | Name of the flow being processed |
| `currentData` | `DataFrame` | The flow's DataFrame at this point in the pipeline |
| `validatedFlows` | `Map[String, DataFrame]` | All flows validated so far in this batch |
| `batchId` | `String` | Current batch identifier |
| `spark` | `SparkSession` | The active SparkSession |

### withData(newData)

Returns a new context with a different DataFrame, preserving all other fields. This is the primary way to modify the flow's data in a transformation:

```scala
val transform: FlowTransformation = { ctx =>
  ctx.withData(ctx.currentData.filter(col("active") === true))
}
```

### getFlow(flowName)

Returns the DataFrame of a previously validated flow, or `None` if the flow has not been processed yet.

```scala
val postTransform: FlowTransformation = { ctx =>
  ctx.getFlow("customers") match {
    case Some(customers) =>
      ctx.withData(
        ctx.currentData.join(
          broadcast(customers.select("customer_id", "tier")),
          Seq("customer_id"), "left"))
    case None =>
      ctx
  }
}
```

Flow availability depends on execution order. The framework orders flows by FK dependencies and `dependsOn` declarations (see [Flow execution order](#flow-execution-order)), so if `orders` has a FK to `customers` (or declares `dependsOn: [customers]`), `customers` is always validated first and available via `getFlow("customers")`.

## Derived tables

Derived tables are Iceberg tables computed after all flows have been written. They read from the Iceberg catalog (full history, not just the current batch delta) and write the result as a full-load Iceberg table.

This is the recommended way to produce aggregations, splits, denormalizations, or any other output derived from your ingested data. Derived tables are first-class Iceberg tables — they have snapshots, time travel, schema evolution, and can be referenced by the DAG or queried directly.

### Registering derived tables

```scala
IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withDerivedTable("order_summary") { ctx =>
    ctx.table("orders")
      .groupBy("category")
      .agg(
        sum("total_amount").as("total_revenue"),
        count("*").as("order_count")
      )
  }
  .withDerivedTable("orders_domestic") { ctx =>
    ctx.table("orders").filter(col("country") === "IT")
  }
  .build()
```

Each derived table function receives a `DerivedTableContext`:

| Field | Type | Description |
|-------|------|-------------|
| `spark` | `SparkSession` | The active SparkSession |
| `batchId` | `String` | Current batch identifier |
| `catalogName` | `String` | Iceberg catalog name (from `global.yaml`) |

Each derived table must have a unique name. Registering two derived tables with the same name throws `IllegalArgumentException` at build time.

### ctx.table(name)

Reads a table from the Iceberg catalog. Returns the full table content (all historical data), not just the records from the current batch. This is the key difference from transformations, where `ctx.currentData` contains only the current batch's data.

```scala
ctx.table("orders")     // reads spark_catalog.default.orders
ctx.table("customers")  // reads spark_catalog.default.customers
```

You can also use `ctx.spark` for arbitrary Spark operations (reading external data, SQL queries, etc.).

!!!note "No validation on derived tables"
    Derived tables are not validated by the framework's validation engine. Their input data comes from Iceberg tables that have already been validated during ingestion, so the source data is clean. If your derived table function introduces logic that could produce unexpected results (e.g. NULLs from left joins), handle it in the function itself.

### Execution order

1. All flows execute (read → validate → transform → write to Iceberg)
2. Derived tables execute in registration order
3. Each derived table writes to `{catalogName}.default.{tableName}` as a full-load overwrite
4. Each successful write is tagged with the batch ID (if `enableSnapshotTagging` is `true` in `global.yaml`)
5. Iceberg maintenance runs on all successfully written derived tables (same settings as flow tables — see [Iceberg maintenance](../configuration/global.md#maintenance))

If a derived table fails, the error is logged and the remaining derived tables continue executing. The `IngestionResult.derivedTableResults` contains the outcome of each derived table.

!!!warning "Known limitations"
    - Derived tables always perform a full overwrite. There is no delta/merge mode — the entire table is recomputed each batch. For most use cases (aggregations, splits, denormalizations) this is correct because the result depends on the full dataset.
    - If a derived table reads from another derived table (via `ctx.table("other_derived")`), registration order matters. Register the dependency first. The framework does not validate or resolve inter-derived-table dependencies automatically.

### DerivedTableResult

| Field | Type | Description |
|-------|------|-------------|
| `tableName` | `String` | Name of the derived table |
| `success` | `Boolean` | Whether the table was written successfully |
| `recordsWritten` | `Long` | Number of records written (0 on failure) |
| `error` | `Option[String]` | Error message on failure |

### Using derived tables in the DAG

Once written, derived tables are regular Iceberg tables. Reference them in a DAG YAML like any flow:

```yaml
nodes:
  - id: summary_node
    sourceFlow: order_summary    # reads from the derived table

```

## Custom catalog providers

The framework ships with two built-in catalog providers:

| `catalogType` | Provider | Description |
|---------------|----------|-------------|
| `hadoop` | `HadoopCatalogProvider` | Local/HDFS filesystem catalog. Zero infrastructure. |
| `glue` | `GlueCatalogProvider` | AWS Glue Data Catalog. Requires S3 and Glue permissions. |

To use a built-in provider, set `catalogType` in `global.yaml`:

```yaml
# Hadoop (default)
iceberg:
  catalogType: "hadoop"
  catalogName: "spark_catalog"
  warehouse: "output/warehouse"

# Glue
iceberg:
  catalogType: "glue"
  catalogName: "spark_catalog"
  warehouse: "s3://my-bucket/warehouse"
  catalogProperties:
    glue.skip-name-validation: "true"
```

The `catalogProperties` map passes arbitrary key-value pairs to the catalog provider. For Glue, these are set as `spark.sql.catalog.{catalogName}.{key}` properties.

### Registering a custom provider

For catalogs not built into the framework (Hive, REST, Nessie), implement the `CatalogProvider` trait and register it on the builder:

```scala
import com.etl.framework.iceberg.catalog.CatalogProvider
import com.etl.framework.config.IcebergConfig
import org.apache.spark.sql.SparkSession

class NessieCatalogProvider extends CatalogProvider {
  override def catalogType: String = "nessie"

  override def configureCatalog(spark: SparkSession, config: IcebergConfig): Unit = {
    val prefix = s"spark.sql.catalog.${config.catalogName}"
    spark.conf.set(prefix, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(s"$prefix.catalog-impl",
      "org.apache.iceberg.nessie.NessieCatalog")
    spark.conf.set(s"$prefix.warehouse", config.warehouse)
    config.catalogProperties.foreach { case (k, v) =>
      spark.conf.set(s"$prefix.$k", v)
    }
  }

  override def validateConfig(config: IcebergConfig): Either[String, Unit] = Right(())
}

IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withCatalogProvider("nessie", () => new NessieCatalogProvider())
  .build()
```

The `CatalogProvider` trait has three methods:

| Method | Description |
|--------|-------------|
| `catalogType` | Identifier string (must match `iceberg.catalogType` in YAML) |
| `configureCatalog(spark, config)` | Configures the SparkSession with catalog-specific properties at runtime |
| `validateConfig(config)` | Validates the IcebergConfig; returns `Left(error)` to abort startup |

Custom providers override built-in ones if the same type key is used. The provider is registered as a factory function (`() => CatalogProvider`) to support lazy initialization.

## Complete example

```scala
import com.etl.framework.pipeline.IngestionPipeline
import com.etl.framework.core.TransformationContext
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object MyPipeline extends App {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Customer Orders Pipeline")
    .master("local[*]")
    .config("spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()

  // Pre-validation: normalize emails
  val normalizeCustomers: TransformationContext => TransformationContext = { ctx =>
    ctx.withData(
      ctx.currentData.withColumn("email", lower(trim(col("email"))))
    )
  }

  // Post-validation: compute derived fields using cross-flow lookup
  val enrichOrders: TransformationContext => TransformationContext = { ctx =>
    ctx.getFlow("customers") match {
      case Some(customers) =>
        ctx.withData(
          ctx.currentData.join(
            broadcast(customers.select("customer_id", "segment")),
            Seq("customer_id"), "left")
            .withColumn("priority",
              when(col("segment") === "premium", lit("high"))
                .otherwise(lit("normal"))))
      case None =>
        ctx
    }
  }

  val result = IngestionPipeline.builder()
    .withConfigDirectory("config")
    .withPreValidationTransformation("customers", normalizeCustomers)
    .withPostValidationTransformation("orders", enrichOrders)
    .build()
    .execute()

  println(s"Batch ${result.batchId} completed — success: ${result.success}")
  result.flowResults.foreach { fr =>
    println(f"  ${fr.flowName}: ${fr.validRecords} valid, " +
      f"${fr.rejectedRecords} rejected (${fr.rejectionRate * 100}%.1f%%), " +
      f"${fr.executionTimeMs}ms")
  }
}
```

## Related

- [Configuration Overview](../configuration/overview.md) — YAML config loading
- [Flow Configuration](../configuration/flows.md) — flow YAML reference
- [Validation Engine](validation.md) — validation pipeline
- [DAG Aggregation](dag-aggregation.md) — downstream aggregation
- [Cloud Deployment](cloud-deployment.md) — deploying on managed platforms
- [Architecture: Data Flow](../architecture/data-flow.md) — end-to-end pipeline
