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
| `withConfigDirectory(path)` | Loads `global.yaml`, `flows/*.yaml`, and optionally `domains.yaml` from the directory. Supports local paths and remote filesystems (S3, HDFS, GCS, Azure). |
| `withGlobalConfig(config)` | Sets `GlobalConfig` programmatically (alternative to directory) |
| `withFlowConfigs(configs)` | Sets flow configs programmatically |
| `withDomainsConfig(config)` | Sets `DomainsConfig` programmatically |
| `withFlowTransformation(flow, pre, post)` | Registers pre and/or post-validation transformations for a flow |
| `withPreValidationTransformation(flow, fn)` | Shorthand for pre-validation only |
| `withPostValidationTransformation(flow, fn)` | Shorthand for post-validation only |
| `withCustomValidator(name, factory)` | Registers a custom validator by name. Use the same name in the flow YAML `class` field. |
| `withCatalogProvider(type, provider)` | Registers a custom Iceberg catalog provider |
| `withDerivedTable(name, dependencies, fn)` | Registers a derived table with explicit, attempt-pinned dependencies |
| `withDataReader(type, factory)` | Registers a custom data reader for a source type. See [Data Sources — Custom readers](data-sources.md#custom-readers). |
| `withBatchListener(listener)` | Registers a listener notified on batch completion or failure. See [Batch Listeners](batch-listeners.md). |
| `withPipelineId(id)` | Sets the stable environment-specific pipeline identity used by explicit execution requests. |
| `withCodeVersion(version)` | Sets the immutable application artifact identity, including custom transformations/readers/validators. |
| `withVariables(variables)` | Sets variables for YAML substitution (priority over env vars). See [Configuration Overview](../configuration/overview.md#variable-substitution) |
| `build()` | Builds the pipeline (returns `IngestionPipeline`) |
| `validate()` | Validates configuration without executing. Returns `Seq[String]` of issues (empty = valid). |

The builder does not have an `execute()` method: call `build()` first, then `execute()`/`executeOrThrow()` for an ad-hoc run or pass an explicit `ExecutionRequest` for platform-managed execution.

Registration names and factories/listeners must be non-blank and non-null. During `validate()` and `build()`, every
registered transformation must name a configured flow, every custom source type must have a matching data-reader
factory, and a custom validation rule must resolve either to a registered factory name or to a loadable public
`Validator` class with a public no-argument constructor. These checks happen before a source is read; names are exact
contract identifiers, so keep their case consistent between code and configuration.

The built pipeline also exposes `inspect(request)`. It performs read-only reconciliation of the original attempt against current Iceberg table identity, `main`, snapshot ancestry, exact operation-summary metadata, and report presence. Its `retrySafe` field is always false: evidence collection does not replace the recovery decision. See [Generic Runner — Inspect an attempt](runner.md#inspect-an-attempt).

### execute() vs executeOrThrow()

| Method | On success | On failure |
|--------|-----------|-----------|
| `execute()` | Returns `IngestionResult` with `success = true` | Returns `IngestionResult` with `success = false` |
| `executeOrThrow()` | Returns `IngestionResult` with `success = true` | Throws `BatchFailedException` |

Use `execute()` when you want to inspect the result programmatically. Use `executeOrThrow()` on managed platforms (AWS Glue, EMR, Databricks) where the runtime detects failure via uncaught exceptions:

```scala
// AWS Glue — job fails automatically if batch fails
IngestionPipeline.builder()
  .withConfigDirectory("config")
  .build()
  .executeOrThrow()
```

```scala
// Custom orchestration — inspect result
val result = IngestionPipeline.builder()
  .withConfigDirectory("config")
  .build()
  .execute()

if (!result.success) {
  // handle failure
}
```

### Configuration validation

`validate()` checks configuration without reading source data or executing Spark jobs. The builder still requires the application's implicit `SparkSession` because that is part of the public builder API.

```scala
val issues = IngestionPipeline.builder()
  .withConfigDirectory("config")
  .validate()

if (issues.nonEmpty) {
  issues.foreach(println)
  sys.exit(1)
}
```

It verifies:
- YAML files are parseable and contain required fields
- FK references point to existing flows
- `dependsOn` references point to existing flows
- No circular dependencies between flows

Use it in CI pipelines to catch config errors before deployment.

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
3. Independent flows (no dependency relationship) are ordered lexicographically by ID and grouped for potential parallel execution

If `performance.parallelFlows` is `true` in `global.yaml`, independent flows within the same group run in parallel using a bounded thread pool sized by `maxParallelFlows`.

```yaml
performance:
  parallelFlows: true
  maxParallelFlows: 4
```

This means if flow `orders` has a FK referencing `customers` (or declares `dependsOn: [customers]`), `customers` is guaranteed to execute first — regardless of the order in the YAML files or the `withFlowConfigs()` list. When parallel execution is disabled, independent flows run sequentially in lexical ID order.

## IngestionResult

`execute()` returns an `IngestionResult`:

```scala
case class IngestionResult(
  request: ExecutionRequest,
  flowResults: Seq[FlowResult],
  success: Boolean,
  error: Option[String] = None,
  derivedTableResults: Seq[DerivedTableResult] = Seq.empty,
  orphanReports: Seq[OrphanReport] = Seq.empty,
  status: ExecutionStatus = ExecutionStatus.Unknown,
  warnings: Seq[String] = Seq.empty,
  executionTimeMs: Long = 0L
)
```

| Field | Type | Description |
|-------|------|-------------|
| `request` | `ExecutionRequest` | Immutable logical/attempt identity and reproducibility metadata |
| `logicalRunId` / `attemptId` | `String` | Convenience accessors for business execution and this driver invocation |
| `flowResults` | `Seq[FlowResult]` | Results for each executed flow |
| `success` | `Boolean` | Functional result of synchronous flow, FK/orphan, and derived work |
| `error` | `Option[String]` | Error message if the batch failed |
| `derivedTableResults` | `Seq[DerivedTableResult]` | Results for each derived table (empty if none registered) |
| `orphanReports` | `Seq[OrphanReport]` | Completed FK/orphan checks |
| `status` | `ExecutionStatus` | `SUCCEEDED`, `SUCCEEDED_WITH_WARNINGS`, `FAILED`, `FAILED_PARTIAL`, or `UNKNOWN` |
| `warnings` | `Seq[String]` | Diagnostic failures that do not invalidate known data commits |

### Platform-managed execution

```scala
val pipeline = IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withPipelineId("orders-prod")
  .withCodeVersion(sys.env("APP_IMAGE_DIGEST"))
  .build()

val request = pipeline.executionDefinition.newRequest(
  logicalRunId = sys.env("LOGICAL_RUN_ID"),
  effectiveAt = Instant.parse(sys.env("EFFECTIVE_AT"))
)

val result = pipeline.executeOrThrow(request)
```

Persist the request in the hosting platform before submission. Floe has no `resume`/`replay` API and performs no whole-job retry. See [Failure Handling and Production Operations](recovery.md).

### FlowResult

Each flow produces a `FlowResult`:

| Field | Type | Description |
|-------|------|-------------|
| `flowName` | `String` | Name of the flow |
| `batchId` | `String` | Compatibility alias for the attempt identifier |
| `success` | `Boolean` | Whether the flow completed successfully |
| `inputRecords` | `Long` | Total records read from source (after pre-validation transform) |
| `validRecords` | `Long` | Records that passed validation |
| `rejectedRecords` | `Long` | Records that failed validation |
| `rejectionRate` | `Double` | `rejectedRecords / inputRecords` |
| `mergedRecords` | `Long` | Rows emitted by the post-validation transformation and submitted to the target write; not the rows physically changed by MERGE |
| `executionTimeMs` | `Long` | Flow execution time in milliseconds |
| `rejectionReasons` | `Map[String, Long]` | Count of rejections per validation step |
| `error` | `Option[String]` | Error message if the flow failed |
| `icebergMetadata` | `Option[IcebergFlowMetadata]` | Iceberg snapshot metadata (see [Iceberg Integration](iceberg.md)) |
| `resultingSnapshotId` | `Option[Long]` | Snapshot pinned for downstream dependencies, including a verified no-change state |
| `resultingSchemaJson` | `Option[String]` | Frozen Spark schema required when a valid empty table has no snapshot; optional for a snapshot-backed result, whose snapshot ID is authoritative |
| `warnings` | `Seq[String]` | Operational side-output failures that did not invalidate the committed target data |
| `dataOutcome` | `DataOutcome` | `NOT_ATTEMPTED`, `NOT_COMMITTED`, `NO_CHANGE`, `COMMITTED`, or `UNKNOWN` |

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
      .join(customers.select("customer_id", "segment"),
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
| `validatedFlows` | `Map[String, DataFrame]` | Completed dependencies pinned to their resulting snapshots |
| `logicalRunId` | `String` | Stable business execution identity |
| `attemptId` | `String` | Current driver invocation identity (`batchId` is a compatibility alias) |
| `effectiveAt` | `Instant` | Functional time supplied in the execution request, with at most microsecond precision |
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
          customers.select("customer_id", "tier"),
          Seq("customer_id"), "left"))
    case None =>
      ctx
  }
}
```

Flow availability depends on execution order. The framework orders flows by FK dependencies and `dependsOn` declarations (see [Flow execution order](#flow-execution-order)), so if `orders` has a FK to `customers` (or declares `dependsOn: [customers]`), `customers` is always validated first and available via `getFlow("customers")`.

## Derived tables

Derived tables are Iceberg tables computed after all flows and orphan checks complete. Every derived target declares its inputs. `ctx.table` resolves those names to the exact flow or derived state produced by this attempt; it never falls back to mutable catalog HEAD.

This is the recommended way to produce aggregations, splits, denormalizations, or any other output derived from your ingested data. Derived tables are first-class Iceberg tables with snapshots and time travel. In `ddlMode: validate` they must be provisioned with the exact derived schema; `automatic` permits Floe-managed creation and additive evolution.

### Registering derived tables

```scala
IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withDerivedTable("order_summary", Seq("orders"), ctx =>
    ctx.table("orders")
      .groupBy("category")
      .agg(
        sum("total_amount").as("total_revenue"),
        count("*").as("order_count")
      )
  )
  .withDerivedTable("orders_domestic", Seq("orders"), ctx =>
    ctx.table("orders").filter(col("country") === "IT")
  )
  .build()
```

Each derived table function receives a `DerivedTableContext`:

| Field | Type | Description |
|-------|------|-------------|
| `spark` | `SparkSession` | The active SparkSession |
| `logicalRunId` | `String` | Stable business execution identity |
| `attemptId` | `String` | Current driver invocation (`batchId` is an alias) |
| `effectiveAt` | `Instant` | Functional time supplied in the request, with at most microsecond precision |
| `availableTables` | `Set[String]` | Declared dependencies successfully resolved for this target |

Each derived table must have a non-blank, unqualified name without surrounding whitespace, path separators, or control characters. Names are unique case-insensitively because Spark commonly resolves identifiers case-insensitively; catalog and namespace come from the Iceberg configuration. Dependencies follow the same logical-name constraints. Invalid graphs and collisions with primary or quality-metrics targets fail during pipeline construction and are checked again by direct orchestrator execution before catalog, source, or target access.

### ctx.table(name)

Returns a declared dependency pinned to the state produced by this attempt. A primary flow is read at its `resultingSnapshotId`; a valid empty table without a snapshot is reconstructed from its frozen resulting schema. A derived dependency is handled the same way after its write completes. Unknown, undeclared, failed, or not-yet-produced inputs fail instead of reading HEAD.

```scala
ctx.table("orders")     // allowed only when "orders" is declared
ctx.table("customers")  // fails unless "customers" is declared
```

`ctx.spark` remains available for computations that do not bypass the declared inputs. Direct `spark.table`/SQL reads or mutable external reads are outside FLOe's pinned-input guarantee and must not be used by a qualified derived transformation.

!!!note "No validation on derived tables"
    Derived tables are not validated by the framework's validation engine. Their inputs may contain accepted warning rows, legacy data, or values produced by custom transformations. Enforce output invariants inside the function or in a downstream quality check.

### Execution order

1. Pipeline-wide structural preflight rejects unknown inputs, self-dependencies, duplicate names or dependencies, cycles, and managed-target collisions before any source or target access
2. All flows execute (read → validate → transform → write to Iceberg)
3. Orphan detection completes successfully
4. The derived executor revalidates its dependency graph against the attempt-pinned primary inputs as a last line of defence
5. Derived tables execute in stable topological order, so registration order does not encode dependencies
6. Each derived table writes to `{catalogName}.{namespace}.{tableName}` as a full-load overwrite
7. A successful snapshot is tagged when `enableSnapshotTagging` is enabled
8. The attempt report records typed outcomes, resulting schemas, and snapshot references

If a derived table fails, its transitive dependents are returned as failed and `NOT_ATTEMPTED`; independent derived tables still execute. The final attempt has `success = false`, `executeOrThrow()` throws, and listeners receive `onBatchFailed`. Already committed tables are not rolled back. If the write committed but Floe cannot load its resulting attempt-pinned state, the failed `DerivedTableResult` retains the committed snapshot, resulting snapshot, frozen schema, operation ID, and reconciliation flag; recovery therefore does not lose the evidence merely because downstream consumption failed.

!!!warning "Known limitations"
    - Derived tables always perform a full overwrite. There is no delta/merge mode — the entire table is recomputed each batch. For most use cases (aggregations, splits, denormalizations) this is correct because the result depends on the full dataset.
    - FLOe cannot sandbox arbitrary Scala code. A function that bypasses `ctx.table` can still read mutable state; such an application is outside the pinned-input contract.

### DerivedTableResult

| Field | Type | Description |
|-------|------|-------------|
| `tableName` | `String` | Name of the derived table |
| `success` | `Boolean` | Whether the table was written successfully |
| `recordsWritten` | `Long` | Number of records written (0 on failure) |
| `error` | `Option[String]` | Error message on failure |
| `snapshotId` | `Option[Long]` | Exact committed snapshot when a snapshot was created |
| `resultingSnapshotId` | `Option[Long]` | Resulting table reference, including a no-change operation |
| `resultingSchemaJson` | `Option[String]` | Frozen schema required for a snapshotless empty result; optional when `resultingSnapshotId` is present |
| `operationId` | `Option[String]` | Identity of this commit attempt, stored in snapshot metadata |
| `reconciled` | `Boolean` | Whether a client exception was resolved by finding this attempt's snapshot |
| `dataOutcome` | `DataOutcome` | Typed data effect for the target |

### Using derived tables in the DAG

Once written, derived tables are regular Iceberg tables. Reference them in a DAG YAML like any flow:

```yaml
nodes:
  - id: summary_node
    sourceFlow: order_summary    # resolves catalog.namespace.order_summary

```

## Custom catalog providers

The framework ships with two built-in catalog providers:

| `catalogType` | Provider | Description |
|---------------|----------|-------------|
| `hadoop` | `HadoopCatalogProvider` | Local/HDFS filesystem catalog. Zero infrastructure. |
| `glue` | `GlueCatalogProvider` | AWS Glue Data Catalog. Requires S3 and Glue permissions. |

Providers are invoked only when `catalogMode: configure` is explicit:

```yaml
# Hadoop (default)
iceberg:
  catalogMode: "configure"
  ddlMode: "automatic"
  catalogType: "hadoop"
  catalogName: "floe"
  warehouse: "output/warehouse"

# Glue
iceberg:
  catalogMode: "configure"
  ddlMode: "validate"
  catalogType: "glue"
  catalogName: "floe"
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

The corresponding `global.yaml` must set `catalogMode: configure`. In the default `existing` mode the platform configures the named catalog before creating the session, and no provider—built-in or custom—is invoked. This choice is independent of `ddlMode`: production integrations normally retain `ddlMode: validate` and provision tables separately.

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
import com.etl.framework.pipeline.TransformationContext
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
            customers.select("customer_id", "segment"),
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
