# Cloud Deployment

Guide to deploying Floe on managed cloud platforms: AWS Glue, Amazon EMR, and Databricks.

## Production baseline

Every production deployment needs infrastructure beyond the Spark job itself:

- a shared `JdbcRunStore` reachable by all retries/replicas;
- an immutable `pipelineVersion` such as the application Git SHA or image digest;
- an independently scheduled `MaintenanceWorker` using the same coordinator;
- durable, versioned source inputs (or a meaningful `source.options.replayToken` for JDBC/custom sources);
- one controlled writer per target table unless explicit external coordination is in place.

```scala
val runStore = new JdbcRunStore(() => coordinatorDataSource.getConnection)

val pipeline = IngestionPipeline.builder()
  .withConfigDirectory(configUri)
  .withRunStore(runStore)
  .withPipelineVersion(deploymentSha)
  .build()

pipeline.executeOrThrow()
```

The coordinator database is not the Iceberg catalog. It stores workflow state, leases, release manifests, and maintenance tasks. See [Recovery and Production Operations](recovery.md) before configuring scheduler retries.

## withVariables for parameter injection

Managed schedulers commonly expose explicit job arguments rather than portable environment variables. Normalize those arguments in the entry point and use `withVariables` to inject them into YAML configuration:

```scala
IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withVariables(Map(
    "DATA_DIR"       -> args("data_dir"),
    "OUTPUT_PATH"    -> args("output_path"),
    "WAREHOUSE_PATH" -> args("warehouse_path")
  ))
  .build()
  .executeOrThrow()  // throws on failure — Glue/EMR/Databricks detect it automatically
```

Variables are resolved in YAML files via `${VAR_NAME}` or `$VAR_NAME` syntax. Explicit variables take priority over environment variables with the same name.

For details on variable substitution, see [Configuration Overview](../configuration/overview.md#variable-substitution).

## Multi-environment configuration

The framework doesn't manage environments directly. It reads a config directory through Hadoop's filesystem APIs, so you control the environment by passing the correct local or remote URI. Two patterns work well:

### Separate directories per environment

Keep a full config set for each environment. One JAR, one parameter to select the environment:

```
config/
├── dev/
│   ├── global.yaml
│   └── flows/
│       ├── customers.yaml
│       └── orders.yaml
├── staging/
│   ├── global.yaml
│   └── flows/
│       ├── customers.yaml
│       └── orders.yaml
└── prod/
    ├── global.yaml
    └── flows/
        ├── customers.yaml
        └── orders.yaml
```

```scala
val env = args("--env")  // "dev", "staging", "prod"

IngestionPipeline.builder()
  .withConfigDirectory(s"config/$env")
  .build()
  .executeOrThrow()
```

Each environment can have different paths, rejection thresholds, validation rules, or even different flows. The downside is duplication — if a flow YAML is identical across environments, you maintain three copies.

### Shared config with variables

Keep one set of YAML files and use variables for the parts that change between environments:

```yaml
# global.yaml
paths:
  outputPath: "${OUTPUT_PATH}/data"
  rejectedPath: "${OUTPUT_PATH}/rejected"
  metadataPath: "${OUTPUT_PATH}/metadata"

performance:
  parallelFlows: false

processing:
  maxRejectionRate: ${MAX_REJECTION_RATE}

iceberg:
  catalogType: "${CATALOG_TYPE}"
  warehouse: "${WAREHOUSE_PATH}"
```

```yaml
# flows/customers.yaml
name: customers
source:
  type: file
  path: "${DATA_PATH}/customers/"
  format: parquet
```

Pass the variables at runtime:

```scala
val env = args("--env")

val variables = env match {
  case "dev" => Map(
    "OUTPUT_PATH" -> "output",
    "DATA_PATH" -> "data",
    "WAREHOUSE_PATH" -> "output/warehouse",
    "CATALOG_TYPE" -> "hadoop",
    "MAX_REJECTION_RATE" -> "0.5"
  )
  case "prod" => Map(
    "OUTPUT_PATH" -> "s3://prod-bucket/output",
    "DATA_PATH" -> "s3://prod-bucket/raw",
    "WAREHOUSE_PATH" -> "s3://prod-bucket/warehouse",
    "CATALOG_TYPE" -> "glue",
    "MAX_REJECTION_RATE" -> "0.01"
  )
}

IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withVariables(variables)
  .build()
  .executeOrThrow()
```

This avoids duplication — one set of YAML files, different values per environment. Variables set via `withVariables` take priority over environment variables with the same name.

### On managed platforms

On AWS Glue, EMR, or Databricks, the config files can be stored on any Hadoop-compatible filesystem. The framework reads them natively:

```scala
// Read config directly from S3
IngestionPipeline.builder()
  .withConfigDirectory("s3://my-bucket/config")
  .build()
  .executeOrThrow()
```

Alternatively, use the fully programmatic API (`withGlobalConfig` + `withFlowConfigs`) and load/decode configuration through application-owned code. A classpath resource inside a JAR is not automatically a Hadoop filesystem directory.

## AWS Glue

### SparkSession setup

AWS Glue manages the SparkSession. Use `GlueContext` and configure Iceberg extensions:

```scala
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import com.etl.framework.pipeline.IngestionPipeline

object GlueJob {
  def main(sysArgs: Array[String]): Unit = {
    val args = GlueArgParser.getResolvedOptions(sysArgs,
      Seq("data_dir", "output_path", "warehouse_path").toArray)

    val sc = new SparkContext()
    val glueContext = new GlueContext(sc)
    implicit val spark: SparkSession = glueContext.getSparkSession

    val result = IngestionPipeline.builder()
      .withConfigDirectory("config")
      .withVariables(Map(
        "DATA_DIR"       -> args("data_dir"),
        "OUTPUT_PATH"    -> args("output_path"),
        "WAREHOUSE_PATH" -> args("warehouse_path")
      ))
      .build()
      .executeOrThrow()
  }
}
```

### Glue job configuration

Set the Iceberg extension in the Glue job parameters, before Glue creates the session:

```
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

Configure the catalog through Floe's Glue provider:

```yaml
iceberg:
  catalogType: "glue"
  catalogName: "floe"
  warehouse: "s3://${BUCKET}/warehouse"
  catalogProperties:
    glue.skip-name-validation: "true"
```

Do not name this catalog `spark_catalog`: Spark reserves that name for its session catalog, while Floe installs Iceberg's standalone `SparkCatalog`.

### Packaging

Floe is compiled against Spark 3.5.8, Scala 2.12, and Iceberg 1.10.1. AWS Glue bundles an Iceberg version selected by the Glue runtime, so first compare the [official Glue/Iceberg compatibility table](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html) with Floe's versions.

For the pinned Floe build, construct one controlled dependency closure containing the matching `iceberg-spark-runtime-3.5_2.12` and `iceberg-aws-bundle` 1.10.1 artifacts. That can be one correctly assembled application JAR or a thin application/Floe JAR plus those dependencies—never both. Supply the resulting custom JAR set through `--extra-jars`, omit `iceberg` from `--datalake-formats`, and on Glue 5.0 or later set `--user-jars-first true`, as required by AWS. Do not also load Glue's bundled Iceberg runtime: duplicate versions on the driver/executor classpath can cause linkage errors or different commit semantics.

Treat each Glue runtime upgrade as a compatibility change. Run at least create, append, `MERGE INTO`, snapshot tagging, metadata-table reads, release recovery, and every enabled maintenance procedure against the exact runtime and IAM/Lake Formation policy before promotion.

## Amazon EMR

### SparkSession setup

On EMR, configure the SparkSession with Iceberg extensions before creating it:

```scala
import com.etl.framework.pipeline.IngestionPipeline
import org.apache.spark.sql.SparkSession

object EMRJob extends App {
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("ETL Pipeline")
    .config("spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()

  val result = IngestionPipeline.builder()
    .withConfigDirectory("config")
    .withVariables(Map(
      "DATA_DIR"       -> sys.env("DATA_DIR"),
      "OUTPUT_PATH"    -> sys.env("OUTPUT_PATH"),
      "WAREHOUSE_PATH" -> sys.env("WAREHOUSE_PATH")
    ))
    .build()
    .executeOrThrow()
}
```

### spark-submit

The recommended AWS configuration uses the Glue catalog in `global.yaml`, not HadoopCatalog directly on S3:

```yaml
iceberg:
  catalogType: "glue"
  catalogName: "floe"
  warehouse: "s3://my-bucket/warehouse"
```

```bash
spark-submit \
  --class com.mycompany.EMRJob \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --jars /path/to/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/path/to/iceberg-aws-bundle-1.10.1.jar \
  my-pipeline.jar
```

!!!tip
    Amazon EMR 6.5.0 and later can provide Iceberg, but the exact Spark, Scala and Iceberg versions are release-specific; consult the [official EMR Iceberg guide](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-iceberg-use-spark-cluster.html). If `my-pipeline.jar` already contains the compatible Iceberg runtime and AWS bundle, omit `--jars`. Otherwise use the release-provided libraries or the Floe-pinned libraries, never both. HadoopCatalog on S3 is not a shortcut: Floe rejects it unless a lock manager and lock table are configured.

### Environment variables

On EMR, pass variables via `spark-submit --conf spark.executorEnv.VAR=value` or via the EMR step configuration. Alternatively, use `withVariables` with values from `sys.env` or command-line arguments.

## Databricks

### Support boundary

Databricks is not currently a zero-configuration Floe target. The built-in providers are Hadoop and AWS Glue; they do not register tables as Unity Catalog managed Iceberg tables. Current Databricks support for managed and foreign Iceberg tables is runtime- and catalog-specific—check the [official Iceberg requirements](https://docs.databricks.com/aws/en/iceberg/) for the exact workspace and runtime.

Do not use `/dbfs/warehouse` as the production design and do not replace `catalogName` with a Unity Catalog name. A production deployment needs a supported object-store catalog, a provider that implements that catalog's authentication and configuration, and an integration test of every SQL operation Floe issues.

### SparkSession setup

Databricks manages the SparkSession. Access it via `spark` in notebooks or configure it in the cluster settings:

```scala
import com.etl.framework.pipeline.IngestionPipeline

// spark is already available in Databricks notebooks
implicit val sparkSession = spark

val result = IngestionPipeline.builder()
  .withConfigDirectory("/dbfs/config")
  .withVariables(Map(
    "DATA_DIR"       -> dbutils.widgets.get("data_dir"),
    "OUTPUT_PATH"    -> dbutils.widgets.get("output_path"),
    "WAREHOUSE_PATH" -> dbutils.widgets.get("warehouse_path")
  ))
  .build()
  .executeOrThrow()
```

### Cluster configuration for an external catalog experiment

If you deliberately test Floe with a separate external Iceberg catalog, add only the extension in the cluster's Spark configuration before the session starts:

```
spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

Using Unity Catalog is not automatic. Its Iceberg REST endpoint has an operation matrix, and Databricks documents that external clients cannot run table-maintenance operations on managed Iceberg tables. A Unity Catalog integration therefore needs a dedicated Floe `CatalogProvider` plus an explicit maintenance ownership decision; merely supplying REST properties is insufficient. Validate create, overwrite, append, `MERGE INTO`, tags, metadata tables, release-manifest snapshot reads, and maintenance on the exact Databricks Runtime. See [Access Databricks tables from Apache Iceberg clients](https://docs.databricks.com/aws/en/external-access/iceberg).

### Packaging

Upload the framework JAR as a cluster or workspace library. Store configuration in a location readable through Hadoop APIs, or load it in application code and use `withGlobalConfig` plus `withFlowConfigs`. A Unity Catalog volume path is not automatically a Hadoop URI accepted by every deployment mode.

## spark-submit configuration reference

Common `--conf` properties for all platforms:

| Property | Value | Required |
|----------|-------|----------|
| `spark.sql.extensions` | `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` | Yes |
| `spark.sql.catalog.{name}` | `org.apache.iceberg.spark.SparkCatalog` | Yes |
| `spark.sql.catalog.{name}.type` | `hadoop` / `hive` / `rest` | When the selected catalog uses Iceberg's named `type` shortcut |
| `spark.sql.catalog.{name}.warehouse` | Warehouse path | Yes |
| `spark.sql.catalog.{name}.catalog-impl` | Catalog implementation class | For Glue/Nessie (instead of `.type`) |

!!!note
    The spark-submit `--conf` properties configure the Spark catalog directly. When using the framework's `global.yaml` with `catalogType: hadoop` or `catalogType: glue`, Floe sets the catalog properties at startup—you only need the `spark.sql.extensions` line before session creation. Do not configure the same catalog in both places with conflicting values.

!!!note
    The `spark.sql.extensions` property **must** be set before the SparkSession is created. On managed platforms, set it in the cluster/job configuration rather than in code.

## Related

- [Configuration Overview — withVariables](../configuration/overview.md#variable-substitution) — variable substitution
- [Pipeline Builder](pipeline-builder.md) — builder API and custom catalog providers
- [Installation](../getting-started/installation.md) — local setup and dependencies
- [Recovery and Production Operations](recovery.md) — coordinator, release manifests, and incident runbooks
