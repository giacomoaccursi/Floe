# Development Setup

## Prerequisites

- **Java 17** or later (Java 17 recommended for development)
- **Scala 2.12.18**
- **SBT 1.9+**
- **Apache Spark 3.5.x** (provided dependency — included automatically for tests)

## Clone and build

```bash
git clone <repository-url>
cd floe
sbt compile
```

## Run tests

```bash
sbt test
```

Tests are configured with:

- **Forked JVM** with 2 GB heap (`-Xmx2g`)
- **Sequential execution** (`parallelExecution := false`) to avoid conflicts on the shared SparkSession
- **Local SparkSession** with `master("local[*]")` and `spark.sql.shuffle.partitions = 1`

### Java 17+ JVM options

The `build.sbt` includes the required `--add-opens` flags for Java 17+ compatibility. If running tests from an IDE, add these JVM options to your run configuration:

```
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
-Djava.security.manager=allow
```

## IDE setup

### IntelliJ IDEA

1. Open the project as an SBT project
2. Wait for SBT import to complete
3. Mark `src/main/scala` as Sources Root
4. Mark `src/test/scala` as Test Sources Root
5. Set the project SDK to Java 17
6. Add the JVM options above to your run/debug configurations

### VS Code with Metals

1. Open the project folder
2. Metals will detect the SBT build and import automatically
3. Use the Metals test explorer to run tests

## Project structure

```
src/main/scala/com/etl/framework/
├── config/          # YAML config loading & validation
├── validation/      # Validation engine
├── iceberg/         # Iceberg table writes
├── io/              # Data readers
├── aggregation/     # DAG execution
├── orchestration/   # Flow and batch orchestration
├── pipeline/        # Entry point and builder API
└── exceptions/      # Exception hierarchy
```

## Related

- [Workflow](workflow.md) — commit conventions and CI/CD
- [Code Quality](code-quality.md) — Scala and Spark best practices
