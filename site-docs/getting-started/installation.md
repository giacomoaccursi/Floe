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

Spark on Java 17+ requires JVM module access flags. Add to your `build.sbt`:

```scala
run / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Djava.security.manager=allow"
)
```

On Java 18+, the `-Djava.security.manager=allow` flag is required for Hadoop's `UserGroupInformation`.

## SparkSession extensions

Iceberg SQL extensions must be configured before the SparkSession is created:

```scala
val spark = SparkSession.builder()
  .appName("My Pipeline")
  .master("local[*]")
  .config("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .getOrCreate()
```

The framework configures all other Iceberg catalog settings automatically from `global.yaml`. Only the extensions must be set at session creation time.

For programmatic session creation, use the `requiredSparkConfig` helper:

```scala
val icebergConfig = IcebergConfig(warehouse = "output/warehouse")
val spark = IngestionPipeline
  .requiredSparkConfig(icebergConfig)
  .foldLeft(SparkSession.builder().appName("my-app").master("local[*]")) {
    case (b, (k, v)) => b.config(k, v)
  }
  .getOrCreate()
```
