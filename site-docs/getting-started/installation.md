# Installation

## Requirements

- Java 17 (the runtime used by this project's CI)
- Scala 2.12.18
- Apache Spark 3.5.8 (the pinned compile/test target)
- SBT 1.9+

## SBT dependency

Add to your `build.sbt`:

```scala
libraryDependencies += "io.github.giacomoaccursi" %% "floe" % "<version>"
```

Spark and Iceberg are `provided` dependencies—the framework expects exactly one compatible engine runtime on the application or cluster classpath. Floe is compiled against `iceberg-spark-runtime-3.5_2.12` 1.10.1 but does not add that JAR, or `iceberg-aws-bundle`, transitively. Test any vendor-patched or different Spark/Iceberg combination as a separate compatibility target.

For a self-managed deployment, supply the matching Iceberg runtime explicitly. Add the AWS bundle only for an AWS catalog/FileIO that requires it:

```scala
val icebergVersion = "1.10.1"

libraryDependencies ++= Seq(
  "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % icebergVersion % "provided",
  // Only for GlueCatalog/S3FileIO; omit for non-AWS deployments.
  "org.apache.iceberg" % "iceberg-aws-bundle" % icebergVersion % "provided"
)
```

Use normal compile/runtime scope instead of `provided` only for local `sbt run`, where no cluster supplies the JARs. Never package a second copy when the target platform already supplies Iceberg.

## Logging

Floe depends on `slf4j-api` but does not impose a logging backend. Spark distributions already provide one; standalone applications must select exactly one SLF4J 2.x provider themselves. Shipping Logback alongside Spark's Log4j bridge creates multiple providers and makes the selected implementation classpath-order dependent.

## Java compatibility

Spark 3.5 supports Java 8, 11, and 17; Floe is built and tested on Java 17. Do not assume a later JDK is supported merely because it can launch the application. Check the exact Spark release's support matrix before changing the runtime.

Local SBT execution on Java 17 may need the module-access options already listed under `Test / javaOptions` in Floe's `build.sbt`. Cluster launchers and vendor runtimes often provide their own options. If you see `InaccessibleObjectException`, apply the flags to the forked application/driver JVM, not only to SBT itself.

Refer to the [Spark 3.5.8 runtime requirements](https://spark.apache.org/docs/3.5.8/) rather than the moving `latest` documentation.

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

The extensions must always be present before session creation. With the default `catalogMode: existing`, the platform must also configure the named `spark.sql.catalog.*` settings; Floe validates them without overwriting the session. `catalogMode: configure` is an explicit opt-in for provider-based bootstrap. Table ownership is separate: `ddlMode: validate` requires pre-provisioned targets, while `automatic` permits Floe-managed DDL for local/bootstrap workflows.

### On managed platforms

On Databricks, EMR, Dataproc, and Glue, the SparkSession may be pre-created by the platform. Configure the Iceberg extensions at cluster/job startup and verify that the platform's Spark, Scala, and Iceberg artifacts match Floe's compatibility matrix. Do not add a second Iceberg runtime JAR when the platform already supplies one; an incompatible platform version is an unsupported deployment, not something classpath ordering can repair.
