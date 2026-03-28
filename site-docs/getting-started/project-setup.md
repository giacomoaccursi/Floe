# Project Setup

## Recommended project structure

```
my-etl-project/
├── build.sbt
├── config/
│   ├── global.yaml
│   ├── domains.yaml
│   └── flows/
│       ├── customers.yaml
│       ├── orders.yaml
│       └── order_items.yaml
├── data/                          # Source data files
│   ├── customers.csv
│   └── orders.csv
├── output/                        # Generated at runtime
│   ├── data/
│   ├── rejected/
│   ├── metadata/
│   └── warehouse/                 # Iceberg tables
└── src/main/scala/
    └── com/mycompany/
        └── MyPipeline.scala       # Entry point
```

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
run / javaOptions ++= Seq(
  "-Xmx2G",
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
