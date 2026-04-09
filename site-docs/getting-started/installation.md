# Installation

## Requirements

- Java 17 or later
- Scala 2.12.18
- Apache Spark 3.5.x
- SBT 1.9+

## SBT dependency

Add to your `build.sbt`:

```scala
libraryDependencies += "io.github.giacomoaccursi" %% "floe" % "<version>"
```

Spark is a `provided` dependency — the framework expects it on the classpath at runtime (cluster, spark-submit, or local dev with Spark installed).

## Java 17+ compatibility

Spark on Java 17+ requires JVM module access flags (`--add-opens`) to work correctly. Without them, Spark fails at startup with `InaccessibleObjectException`.

This is a Spark requirement, not specific to this framework. The exact flags depend on your Spark and Java version. Refer to the [Apache Spark documentation](https://spark.apache.org/docs/latest/) for the flags required by your version.

On Java 18+, you also need `-Djava.security.manager=allow` for Hadoop's `UserGroupInformation`.

## SparkSession setup

The framework requires Iceberg SQL extensions to be registered on the SparkSession **before** it is created. Spark does not allow adding extensions after session creation.

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

### On managed platforms

On Databricks, EMR, Dataproc, and Glue, the SparkSession is pre-created by the platform. The Iceberg extensions are typically configured at the cluster level or via job parameters — you don't need to set them in code. Check your platform's documentation for how to add `spark.sql.extensions`.
