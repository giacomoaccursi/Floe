# Installation

## Requirements

- Java 17 or later
- Scala 2.12.18
- Apache Spark 3.5.x
- SBT 1.9+

## SBT dependency

Add to your `build.sbt`:

```scala
libraryDependencies += "io.github.giacomoaccursi" %% "spark-etl-framework" % "<version>"
```

Spark is a `provided` dependency — the framework expects it on the classpath at runtime (cluster, spark-submit, or local dev with Spark installed).

## Java 17+ compatibility

Spark on Java 17+ requires JVM module access flags (`--add-opens`) to work correctly. Without them, Spark fails at startup with `InaccessibleObjectException`.

This is a Spark requirement, not specific to this framework. The exact flags depend on your Spark and Java version. Refer to the [Apache Spark documentation](https://spark.apache.org/docs/latest/) for the flags required by your version.

On Java 18+, you also need `-Djava.security.manager=allow` for Hadoop's `UserGroupInformation`.

## SparkSession setup

The framework requires Iceberg SQL extensions to be registered on the SparkSession **before** it is created. Spark does not allow adding extensions after session creation.

There are two ways to set this up:

### Option 1: Manual configuration

Set the extensions directly on the SparkSession builder:

```scala
val spark = SparkSession.builder()
  .appName("My Pipeline")
  .master("local[*]")
  .config("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .getOrCreate()
```

All other Iceberg settings (catalog name, warehouse path, catalog type) are configured automatically by the framework from `global.yaml` — you only need the extensions line.

### Option 2: requiredSparkConfig helper

If you want the framework to tell you exactly which Spark properties are needed, use `IngestionPipeline.requiredSparkConfig()`. It returns a `Map[String, String]` with all properties that must be set before session creation:

```scala
import com.etl.framework.pipeline.IngestionPipeline
import com.etl.framework.config.IcebergConfig

// Define your Iceberg config (or load it from YAML)
val icebergConfig = IcebergConfig(warehouse = "output/warehouse")

// Get the required Spark properties
val requiredProps: Map[String, String] = IngestionPipeline.requiredSparkConfig(icebergConfig)
// Returns: Map("spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

// Apply them to the session builder
val spark = requiredProps
  .foldLeft(SparkSession.builder().appName("my-app").master("local[*]")) {
    case (builder, (key, value)) => builder.config(key, value)
  }
  .getOrCreate()
```

This is useful when:

- You use a custom catalog provider (Glue, Nessie) that may require additional session-level properties
- You want to keep the session setup in sync with the framework's requirements automatically

If you registered custom catalog providers, pass them so their properties are included:

```scala
val extraProviders = Map("nessie" -> (() => new NessieCatalogProvider()))
val requiredProps = IngestionPipeline.requiredSparkConfig(icebergConfig, extraProviders)
```

### On managed platforms

On Databricks, EMR, Dataproc, and Glue, the SparkSession is pre-created by the platform. The Iceberg extensions are typically configured at the cluster level or via job parameters — you don't need to set them in code. Check your platform's documentation for how to add `spark.sql.extensions`.
