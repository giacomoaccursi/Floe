# Pipeline Builder & TransformationContext

## Overview

`IngestionPipeline` is the public entry point of the framework. It provides a fluent builder API for configuring and executing ETL pipelines. The builder loads configuration (from YAML files or programmatic objects), registers transformations, configures catalog providers, and produces an executable pipeline.

`TransformationContext` is the immutable context passed to every transformation function. It provides access to the current flow's data, previously validated flows, the batch ID, the SparkSession, and a mechanism to register additional tables for downstream DAG aggregation.

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
| `withConfigDirectory(path)` | Loads `global.yaml`, `domains.yaml`, and `flows/*.yaml` from the directory |
| `withGlobalConfig(config)` | Sets `GlobalConfig` programmatically (alternative to directory) |
| `withFlowConfigs(configs)` | Sets flow configs programmatically |
| `withDomainsConfig(config)` | Sets `DomainsConfig` programmatically |
| `withFlowTransformation(flow, pre, post)` | Registers pre and/or post-validation transformations for a flow |
| `withPreValidationTransformation(flow, fn)` | Shorthand for pre-validation only |
| `withPostValidationTransformation(flow, fn)` | Shorthand for post-validation only |
| `withCatalogProvider(type, provider)` | Registers a custom Iceberg catalog provider |
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

Flows are not executed in config file order. The framework builds an execution plan based on foreign key dependencies:

1. FK references between flows are analyzed to build a dependency graph
2. Flows are topologically sorted so that parent flows execute before children
3. Independent flows (no FK relationship) are grouped for potential parallel execution

If `performance.parallelFlows` is `true` in `global.yaml`, independent flows within the same group run in parallel using a bounded thread pool.

```yaml
performance:
  parallelFlows: true
```

This means if flow `orders` has a FK referencing `customers`, `customers` is guaranteed to execute first — regardless of the order in the YAML files or the `withFlowConfigs()` list.

## IngestionResult

`execute()` returns an `IngestionResult`:

```scala
case class IngestionResult(
  batchId: String,
  flowResults: Seq[FlowResult],
  success: Boolean,
  error: Option[String] = None
)
```

| Field | Type | Description |
|-------|------|-------------|
| `batchId` | `String` | Batch identifier (formatted according to `processing.batchIdFormat`) |
| `flowResults` | `Seq[FlowResult]` | Results for each executed flow |
| `success` | `Boolean` | `true` if all flows completed without fatal errors |
| `error` | `Option[String]` | Error message if the batch failed |

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
- Creating additional tables for DAG aggregation
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
| `additionalTables` | `Map[String, AdditionalTableInfo]` | Tables registered via `addTable()` |

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

Flow availability depends on execution order. The framework orders flows by FK dependencies (see [Flow execution order](#flow-execution-order)), so if `orders` has a FK to `customers`, `customers` is always validated first and available via `getFlow("customers")`.

### addTable(tableName, data, outputPath, dagMetadata)

Registers an additional table created during transformation. The table is saved to the output path and made available for [DAG aggregation](dag-aggregation.md).

```scala
val createSummary: FlowTransformation = { ctx =>
  val summary = ctx.currentData
    .groupBy("customer_id")
    .agg(
      sum("total_amount").as("lifetime_value"),
      count("order_id").as("order_count")
    )

  ctx
    .addTable(
      tableName = "customer_summaries",
      data = summary,
      outputPath = Some("output/warehouse/customer_summaries"),
      dagMetadata = Some(AdditionalTableMetadata(
        primaryKey = Seq("customer_id"),
        joinKeys = Map("customers" -> Seq("customer_id")),
        description = Some("Customer lifetime value summaries"),
        partitionBy = Seq.empty
      ))
    )
}
```

`addTable` returns a new context with the table registered. The framework reads additional tables from the returned context, so they are automatically persisted and forwarded to the DAG. If you also need to modify the flow's DataFrame, chain `withData` before or after `addTable`:

```scala
ctx
  .withData(enrichedDf)
  .addTable("summary", summary, ...)
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `tableName` | `String` | yes | Unique name for the table |
| `data` | `DataFrame` | yes | The table's data |
| `outputPath` | `Option[String]` | no | Where to save the table. Defaults to `{globalConfig.paths.outputPath}/{flowName}_{tableName}` |
| `dagMetadata` | `Option[AdditionalTableMetadata]` | no | Metadata for DAG auto-discovery |

#### AdditionalTableMetadata

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `primaryKey` | `Seq[String]` | — | Primary key columns |
| `joinKeys` | `Map[String, Seq[String]]` | `Map.empty` | Map of parent flow name → join key columns |
| `description` | `Option[String]` | `None` | Human-readable description |
| `partitionBy` | `Seq[String]` | `Seq.empty` | Partition columns for output |

Tables registered with `dagMetadata` are automatically discovered by the DAG aggregation module when auto-discovery is enabled. See [DAG Aggregation](dag-aggregation.md#auto-discovery-of-additional-tables) for details.

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

  override def sparkSessionConfig(config: IcebergConfig): Map[String, String] =
    Map("spark.sql.extensions" ->
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

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

The `CatalogProvider` trait has four methods:

| Method | Description |
|--------|-------------|
| `catalogType` | Identifier string (must match `iceberg.catalogType` in YAML) |
| `sparkSessionConfig(config)` | Returns Spark properties that must be set before session creation (e.g. `spark.sql.extensions`) |
| `configureCatalog(spark, config)` | Configures the SparkSession with catalog-specific properties at runtime |
| `validateConfig(config)` | Validates the IcebergConfig; returns `Left(error)` to abort startup |

Custom providers override built-in ones if the same type key is used. The provider is registered as a factory function (`() => CatalogProvider`) to support lazy initialization.

## requiredSparkConfig helper

Some Spark properties (like `spark.sql.extensions`) must be set before the SparkSession is created. The `requiredSparkConfig` helper returns these properties for programmatic session creation:

```scala
val icebergConfig = IcebergConfig(
  catalogType = "hadoop",
  catalogName = "spark_catalog",
  warehouse = "output/warehouse"
)

val baseBuilder = SparkSession.builder()
  .appName("my-app")
  .master("local[*]")

val spark = IngestionPipeline
  .requiredSparkConfig(icebergConfig)
  .foldLeft(baseBuilder) { case (b, (k, v)) => b.config(k, v) }
  .getOrCreate()
```

If you registered custom catalog providers, pass them to `requiredSparkConfig` so their session-level properties are included:

```scala
val extraProviders = Map("nessie" -> (() => new NessieCatalogProvider()))

val spark = IngestionPipeline
  .requiredSparkConfig(icebergConfig, extraProviders)
  .foldLeft(baseBuilder) { case (b, (k, v)) => b.config(k, v) }
  .getOrCreate()
```

In cluster environments (Databricks, EMR, YARN), these properties are typically set via cluster config or `spark-submit --conf` and this helper is not needed.

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
