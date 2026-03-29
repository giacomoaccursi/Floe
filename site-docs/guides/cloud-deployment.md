# Cloud Deployment

Guide to deploying the Spark ETL Framework on managed cloud platforms: AWS Glue, Amazon EMR, and Databricks.

## withVariables for parameter injection

On managed platforms, job parameters don't arrive as environment variables. Use `withVariables` to inject them into YAML configuration:

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

Variables are resolved in YAML files via `${VAR_NAME}` or `$VAR_NAME` syntax. Explicit variables take priority over environment variables with the same name.

For details on variable substitution, see [Configuration Overview](../configuration/overview.md#variable-substitution).

## Multi-environment configuration

The framework doesn't manage environments directly — it reads config from a local directory. You control which config is loaded by passing the right path. Two patterns work well:

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
  .execute()
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
  .execute()
```

This avoids duplication — one set of YAML files, different values per environment. Variables set via `withVariables` take priority over environment variables with the same name.

### On managed platforms

On AWS Glue, EMR, or Databricks, the config files must be accessible from the worker's local filesystem. Common approaches:

- Package the config directory inside the JAR (as resources)
- Copy config files from S3/DBFS to a local temp directory at job startup
- Use the fully programmatic API (`withGlobalConfig` + `withFlowConfigs`) and load configs from any source

The framework is agnostic about where configs come from — `withConfigDirectory` reads from local filesystem, but `withGlobalConfig`/`withFlowConfigs` accept objects from any source.

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
      .execute()
  }
}
```

### Glue job configuration

Set Iceberg extensions and catalog properties in the Glue job parameters:

```
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.spark_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
--conf spark.sql.catalog.spark_catalog.warehouse=s3://my-bucket/warehouse
```

Or use the Glue catalog provider in `global.yaml`:

```yaml
iceberg:
  catalogType: "glue"
  catalogName: "spark_catalog"
  warehouse: "s3://${BUCKET}/warehouse"
  catalogProperties:
    glue.skip-name-validation: "true"
```

### Packaging

Package the framework JAR and your application code as a fat JAR. Upload to S3 and reference it as `--extra-jars` in the Glue job configuration.

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
    .execute()
}
```

### spark-submit

```bash
spark-submit \
  --class com.mycompany.EMRJob \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hadoop \
  --conf spark.sql.catalog.spark_catalog.warehouse=s3://my-bucket/warehouse \
  --jars /path/to/iceberg-spark-runtime.jar \
  my-pipeline.jar
```

!!!tip
    On EMR, Iceberg runtime JARs can be pre-installed via bootstrap actions or included in the EMR release configuration. Check your EMR version for built-in Iceberg support.

### Environment variables

On EMR, pass variables via `spark-submit --conf spark.executorEnv.VAR=value` or via the EMR step configuration. Alternatively, use `withVariables` with values from `sys.env` or command-line arguments.

## Databricks

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
  .execute()
```

### Cluster configuration

Add Iceberg extensions in the cluster's Spark configuration:

```
spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.spark_catalog org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.spark_catalog.type hadoop
spark.sql.catalog.spark_catalog.warehouse /dbfs/warehouse
```

Or use the Databricks Unity Catalog if available.

### Packaging

Upload the framework JAR as a cluster library or workspace library. Reference config files from DBFS (`/dbfs/config/`) or Unity Catalog volumes.

## spark-submit configuration reference

Common `--conf` properties for all platforms:

| Property | Value | Required |
|----------|-------|----------|
| `spark.sql.extensions` | `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` | Yes |
| `spark.sql.catalog.{name}` | `org.apache.iceberg.spark.SparkCatalog` | Yes |
| `spark.sql.catalog.{name}.type` | `hadoop` / `hive` / `rest` | Yes |
| `spark.sql.catalog.{name}.warehouse` | Warehouse path | Yes |
| `spark.sql.catalog.{name}.catalog-impl` | Catalog implementation class | For Glue/Nessie |

!!!note
    The `spark.sql.extensions` property **must** be set before the SparkSession is created. On managed platforms, set it in the cluster/job configuration rather than in code.

## Related

- [Configuration Overview — withVariables](../configuration/overview.md#variable-substitution) — variable substitution
- [Pipeline Builder](pipeline-builder.md) — builder API and custom catalog providers
- [Installation](../getting-started/installation.md) — local setup and dependencies
