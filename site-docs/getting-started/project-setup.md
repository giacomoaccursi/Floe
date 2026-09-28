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

Source data can be local files, S3 paths, or JDBC databases — configured per-flow in the YAML. The `output/` directory (data, rejected, metadata, warehouse) is created automatically at runtime based on the paths in `global.yaml`.

## build.sbt

```scala
scalaVersion := "2.12.18"

val sparkVersion = "3.5.8"

libraryDependencies ++= Seq(
  "io.github.giacomoaccursi" %% "floe" % "<version>",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

run / fork := true    // Apply application JVM options to a separate process
run / javaOptions += "-Xmx2G"
// If local Java 17 execution needs module access, copy the tested --add-opens
// set from Floe's build.sbt. Spark 3.5 supports Java 8, 11, and 17.
```

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

For production, build the pipeline once with a shared coordinator and an immutable deployment revision:

```scala
val pipeline = IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withRunStore(new JdbcRunStore(() => dataSource.getConnection))
  .withPipelineVersion(sys.env("APP_RELEASE_SHA"))
  .build()

pipeline.executeOrThrow()
```

The application supplies `dataSource` and its JDBC driver. See [Recovery and Production Operations](../guides/recovery.md) before enabling scheduled production runs.

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
