# Project Setup

## Recommended project structure

```
my-etl-project/
├── build.sbt
├── config/
│   ├── global.yaml
│   ├── domains.yaml          # optional
│   └── flows/
│       ├── customers.yaml
│       ├── orders.yaml
│       └── order_items.yaml
└── src/main/scala/
    └── com/mycompany/
        └── MyPipeline.scala  # Entry point
```

Source data can be local files, S3 paths, or JDBC databases — configured per-flow in the YAML. Diagnostic output paths are created when written. An Iceberg warehouse and target tables are platform-owned by default; the local quickstart explicitly enables catalog bootstrap and automatic DDL.

## build.sbt

```scala
scalaVersion := "2.12.18"

val sparkVersion = "3.5.8"
val icebergVersion = "1.10.1"

libraryDependencies ++= Seq(
  "io.github.giacomoaccursi" %% "floe" % "<version>",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % icebergVersion % "provided"
)

run / fork := true    // Apply application JVM options to a separate process
run / javaOptions += "-Xmx2G"
// If local Java 17 execution needs module access, copy the tested --add-opens
// set from Floe's build.sbt. Spark 3.5 supports Java 8, 11, and 17.
```

This is the cluster packaging profile: Spark and Iceberg are supplied once by the deployment. For local `sbt run`, remove `% "provided"` from those dependencies so they are present on the runtime classpath. Add `iceberg-aws-bundle` at the same version only when the chosen AWS catalog/FileIO needs it; do not add it for the local Hadoop catalog.

## Entry point

```scala
import com.etl.framework.pipeline.IngestionPipeline
import org.apache.spark.sql.SparkSession

object MyPipeline extends App {
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("My ETL Pipeline")
    .master("local[*]")
    .config("spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()

  val result = IngestionPipeline.builder()
    .withConfigDirectory("config")
    .build()
    .execute()

  if (result.success) {
    result.flowResults.foreach { fr =>
      println(f"  ${fr.flowName}: ${fr.validRecords} valid, " +
        f"${fr.rejectedRecords} rejected")
    }
  } else {
    System.err.println(s"Batch failed: ${result.error.getOrElse("unknown")}")
    sys.exit(1)
  }

  spark.stop()
}
```

On managed platforms (Glue, EMR, Databricks), use `executeOrThrow()` instead — the platform detects failure automatically via the uncaught exception:

```scala
IngestionPipeline.builder()
  .withConfigDirectory("config")
  .build()
  .executeOrThrow()
```

For production, give the pipeline and application artifact stable identities, then submit an explicit request:

```scala
val pipeline = IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withPipelineId("orders-prod")
  .withCodeVersion(sys.env("APP_IMAGE_DIGEST"))
  .build()

val request = pipeline.executionDefinition.newRequest(
  logicalRunId = sys.env("LOGICAL_RUN_ID"),
  effectiveAt = java.time.Instant.parse(sys.env("EFFECTIVE_AT"))
)
pipeline.executeOrThrow(request)
```

Persist the request before submission and disable automatic scheduler retries until the full application is qualified as repeatable. See [Failure Handling and Production Operations](../guides/recovery.md).

## Running locally

```bash
sbt run
```

## Adding transformations

```scala
import com.etl.framework.pipeline.TransformationContext
import org.apache.spark.sql.functions._

val normalizeEmails: TransformationContext => TransformationContext = { ctx =>
  ctx.withData(ctx.currentData.withColumn("email", lower(trim(col("email")))))
}

val result = IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withPreValidationTransformation("customers", normalizeEmails)
  .build()
  .execute()
```

## Next steps

- [Configuration Overview](../configuration/overview.md) — understand the YAML files
- [Pipeline Builder](../guides/pipeline-builder.md) — full builder API
- [Cloud Deployment](../guides/cloud-deployment.md) — deploy on Glue, EMR, Databricks
- [Recovery and Production Operations](../guides/recovery.md) — durable coordinator and runbooks
