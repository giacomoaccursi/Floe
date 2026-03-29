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
├── data/                     # Source data files
│   ├── customers.csv
│   └── orders.csv
└── src/main/scala/
    └── com/mycompany/
        └── MyPipeline.scala  # Entry point
```

The `output/` directory (data, rejected, metadata, warehouse) is created automatically at runtime based on the paths in `global.yaml`.

## build.sbt

```scala
scalaVersion := "2.12.18"

val sparkVersion = "3.5.8"

libraryDependencies ++= Seq(
  "io.github.giacomoaccursi" %% "spark-etl-framework" % "<version>",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

run / fork := true
run / javaOptions += "-Xmx2G"
// Add --add-opens flags required by Spark on Java 17+
// See: https://spark.apache.org/docs/latest/
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
    println(s"Batch ${result.batchId} completed successfully")
    result.flowResults.foreach { fr =>
      println(f"  ${fr.flowName}: ${fr.validRecords} valid, " +
        f"${fr.rejectedRecords} rejected (${fr.rejectionRate * 100}%.1f%%)")
    }
  } else {
    System.err.println(s"Batch failed: ${result.error.getOrElse("unknown")}")
    sys.exit(1)
  }

  spark.stop()
}
```

## Running locally

```bash
sbt run
```

## Adding transformations

```scala
import com.etl.framework.core.TransformationContext
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
