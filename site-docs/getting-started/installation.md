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

Spark, Iceberg, JSON4s/Jackson, and SLF4J are `provided` dependencies, and the assembly excludes Scala—the framework expects the platform-owned Spark stack exactly once on the application and executor classpaths. Floe is compiled against Spark 3.5.8, Scala 2.12.18, JSON4s 3.7.0-M11, SLF4J 2.0.7, and `iceberg-spark-runtime-3.5_2.12` 1.10.1, matching the upstream Spark distribution, but does not package those runtime classes or `iceberg-aws-bundle`. Test any vendor-patched or different combination as a separate compatibility target.

For a self-managed deployment, start from the matching Spark distribution; it supplies Scala, JSON4s/Jackson, and SLF4J. Supply the matching Iceberg runtime explicitly. Add the AWS bundle only for an AWS catalog/FileIO that requires it:

```scala
val icebergVersion = "1.10.1"

libraryDependencies ++= Seq(
  "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % icebergVersion % "provided",
  // Only for GlueCatalog/S3FileIO; omit for non-AWS deployments.
  "org.apache.iceberg" % "iceberg-aws-bundle" % icebergVersion % "provided"
)
```

Use normal compile/runtime scope instead of `provided` only for local `sbt run`, where no cluster supplies the JARs. Never package a second copy when the target platform already supplies Iceberg.

## Deployment assembly

Running `sbt assembly` in this repository creates the generic YAML runner JAR. The build discards Java module descriptors that cannot coexist in an uber JAR, keeps Floe and its non-engine dependencies, and excludes every platform-owned class family listed above. CI inspects the resulting archive and fails if those classes leak into it.

A custom application that depends on Floe owns its own assembly settings and must preserve the same boundary. Validate the finished application JAR with `spark-submit` on the exact Spark/Iceberg runtime selected for deployment; successful compilation alone does not qualify a managed platform.

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
