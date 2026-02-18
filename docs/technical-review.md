# Technical Review

Comprehensive analysis of the framework's technical debt, anti-patterns, performance issues, and extensibility gaps.

---

## 1. SQL Injection in the Iceberg Layer

Every SQL statement in `IcebergTableWriter`, `IcebergTableManager`, and `OrphanDetector` is built via string interpolation. Table names, column names, property values, and snapshot IDs are injected directly into SQL strings without quoting or escaping.

Examples:

```scala
// IcebergTableWriter - MERGE INTO
s"MERGE INTO $tableName AS target USING _staged AS source ON $mergeOn ..."

// IcebergTableManager - maintenance
s"CALL ${catalogName}.system.expire_snapshots(table => '$tableName', ...)"

// OrphanDetector - time travel
s"SELECT $refCol FROM $tableName VERSION AS OF $prevSnapshotId"

// OrphanDetector - cascade delete
s"DELETE FROM $childTableName WHERE $fkCol IN (SELECT $fkCol FROM $removedPksView)"
```

Today these values come from YAML config files written by the team, so exploitation is unlikely. But the framework is designed to be generic: if it ever accepts external input (API-driven config, multi-tenant environments, user-provided flow names), this becomes a real attack surface. Iceberg and Spark SQL don't support parameterized queries, so the fix is to validate identifiers against `^[a-zA-Z_][a-zA-Z0-9_]*$` at config load time and reject anything that doesn't match.

**Impact**: high in a multi-tenant or externally-configured deployment, low in the current single-team setup.

---

## 2. DataFrame Materialization for Logging

The most pervasive performance issue: `count()` and `isEmpty` calls that exist only to produce log messages. Each one triggers a full Spark job.

| File | Lines | Call | Purpose |
|------|-------|------|---------|
| FlowExecutor | 89, 111, 112, 145, 150, 151 | `count()` | Log input/valid/rejected counts |
| FlowDataWriter | 60, 90, 117 | `count()` | Log written/rejected counts |
| FlowTransformer | 28, 37, 64, 75 | `count()` | Log pre/post transformation counts |
| FlowMetadataWriter | 80 | `count()` | Record count in metadata JSON |
| BatchModelWriter | 20 | `count()` | Log dataset size |
| FinalModelWriter | 47, 112 | `count()` | Log dataset size (x2, code is duplicated) |
| IcebergTableWriter | 28, 48, 137 | `count()` | Log source records |
| DAGNodeProcessor | 35 | `count()` | Log node result size |
| ValidationEngine | 100 | `count()` | Track rejection counts per step |
| OrphanDetector | 79, 224 | `count()` | Condition check + logging |
| BaseValidator | 37 | `isEmpty` | Skip when no invalid records |
| PrimaryKeyValidator | 31 | `isEmpty` | Skip when no duplicates |
| SchemaValidator | 30, 35, 39 | `isEmpty` | Skip when schema matches |
| ForeignKeyValidator | 39 | `isEmpty` | Skip when no orphans |
| CustomRulesValidator | 78 | `isEmpty` | Check rejected set |

A single flow with 5 validation rules can trigger 15+ unnecessary Spark jobs before even writing data. On a million-row dataset these add minutes of pure overhead.

**Fix approaches**:
- Remove counts from logging (rely on Spark write statistics or accumulator-based metrics)
- Replace `isEmpty` with `head(1).isEmpty` (evaluates one partition, not all)
- Defer rejection counting to the final aggregation step in ValidationEngine, not per-step
- Use Spark accumulators for lightweight counter tracking

---

## 3. SCD2Merger: Five Joins on the Same Data

`SCD2Merger.historicizeChanges()` performs a full_outer join to classify records, then joins the result back to `existing` and `newData` five separate times:

```
1. existing.join(changedRecords, ...)       -> closedRecords
2. newData.join(changedRecords, ...)        -> newVersions
3. existing.join(unchangedKeys, ...)        -> unchangedRecords
4. existing.join(missingFromSourceKeys, ...) -> missingRecords
5. unionByName of all parts + historicalRecords
```

None of the intermediate DataFrames (`changedRecords`, `unchangedKeys`, `missingFromSourceKeys`, `existingCurrent`, `newDataWithCompareKey`) are cached. Spark may re-evaluate the initial full_outer join multiple times.

The Iceberg path avoids this entirely by using a single atomic MERGE INTO. But the Parquet fallback still uses this multi-join approach, meaning non-Iceberg deployments pay the full cost.

**Fix**: cache `existingCurrent` and the full_outer join result after classification, then unpersist after the final union. Alternatively, rewrite using window functions on a single union.

---

## 4. Mutable State

### 4a. Builder pattern with `var`

`IngestionPipelineBuilder` uses `var` fields for optional config:

```scala
private var _globalConfig: Option[GlobalConfig] = None
private var _flowConfigs: Option[Seq[FlowConfig]] = None
private var _domainsConfig: Option[DomainsConfig] = None
```

This is acceptable for a builder, but the same pattern appears in I/O classes where it's not justified:

`FileDataReader`, `BatchModelWriter`, and `FinalModelWriter` all use `var writer/reader` reassigned across if-else branches. This can be replaced with method chaining or fold on Options.

### 4b. Mutable collections in orchestration

`FlowOrchestrator.execute()` uses `mutable.ArrayBuffer` and `mutable.Map` to accumulate results across groups. The mutable map `validatedFlows` is passed to `FlowGroupExecutor`, which can execute flows in parallel. If two parallel flows both complete and call back into the same mutable map, there's a race condition.

`FlowGroupExecutor.executeParallel()` uses `ExecutionContext.Implicits.global` with `Duration.Inf` timeout. No thread pool bounds, no timeout, and shared mutable state.

### 4c. MetricsCollector

Four levels of nesting mutable state:

```scala
private val histograms = mutable.Map[String, mutable.ArrayBuffer[Double]]()
```

Protected by `lock.synchronized`, but lock contention becomes a bottleneck if metrics are recorded frequently. `concurrent.TrieMap` with `AtomicLong` counters would be lock-free.

### 4d. OrphanDetector

Uses `mutable.Map[String, DataFrame]` for cascade propagation and `mutable.ArrayBuffer[OrphanReport]` for report accumulation. Could be replaced with a fold over the topological flow sequence, threading an immutable map through each step.

### 4e. ConfigurableValidator trait

```scala
trait ConfigurableValidator {
  def configure(config: Map[String, String]): Unit
}
```

Return type `Unit` forces implementors to use `var` for internal state. Should return a new configured instance instead: `def configure(config: Map[String, String]): ConfigurableValidator`.

---

## 5. Non-Exhaustive Pattern Matching

The CLAUDE.md explicitly forbids wildcard matches on sealed traits. Several places violate this:

### JoinStrategyExecutor (lines 133-145)

`AggregationFunction` has 9 cases (Sum, Count, Avg, Min, Max, First, Last, CollectList, CollectSet). Only 5 are handled; the rest fall into a wildcard that throws `UnsupportedOperationException` at runtime. The compiler can't warn about missing cases because of the catch-all.

### CustomRulesValidator (line 44)

```scala
customRule.onFailure match {
  case OnFailureAction.Reject => ...
  case _ => processWarnRule(...)
}
```

If `OnFailureAction.Skip` is used here, it silently behaves like Warn. Should be exhaustive.

### FlowExecutor, FlowDataWriter

Load mode and format references use `.name` string comparisons instead of pattern matching on the sealed trait. This bypasses compile-time exhaustiveness.

---

## 6. Error Handling

### 6a. Swallowed exceptions in IcebergTableManager

Eight locations catch `Exception` with underscore:

```scala
catch { case _: Exception => ... }
```

This suppresses the actual error. A `PermissionDeniedException`, `OutOfMemoryError`, or `SparkException` all get the same treatment. The methods `tableExists()`, `runMaintenance()`, `rollbackToSnapshot()` all hide the root cause.

### 6b. Lost stack traces

`FlowExecutor` and `FlowOrchestrator` capture `error.getMessage` in result objects, discarding the stack trace. When a flow fails, the operator sees "Processing error" instead of a full trace pointing to the source.

### 6c. OrphanDetector time travel

```scala
catch {
  case e: Exception =>
    logger.warn(s"Failed to perform time travel on ${parentCfg.name}: ${e.getMessage}")
    None
}
```

If time travel fails due to a real bug (wrong SQL syntax, missing column), it's silently skipped. The batch completes successfully with no orphan detection, and nobody knows it didn't run.

### 6d. ConfigLoader silent env var substitution

When an environment variable is missing during YAML interpolation, the framework keeps the raw `${VAR_NAME}` string in the config value. No warning, no error. If this is a JDBC password or a warehouse path, the pipeline fails much later with a confusing error.

### 6e. AdditionalTableDiscovery

Returns `Seq.empty` on any exception. The caller can't distinguish "no tables" from "file system error". DAG execution proceeds with missing nodes.

---

## 7. Resource Leaks

### Temp views in IcebergTableWriter

`createOrReplaceTempView()` is called before SQL execution, with `dropTempView()` after. If the SQL throws an exception, the temp view persists for the duration of the SparkSession. In a long-running session processing many batches, these accumulate.

Fix: use try/finally to guarantee cleanup, or use subquery CTEs instead of temp views.

### No cache/unpersist discipline

No DataFrame in the framework is ever explicitly cached or unpersisted. When a DataFrame is used in multiple operations (e.g., `validationResult.valid` is counted, transformed, and written), Spark may re-evaluate the entire lineage each time. Conversely, accidental persistence through Spark's internal caching can cause memory pressure without any explicit unpersist call.

---

## 8. Thread Safety in Parallel Execution

`FlowGroupExecutor.executeParallel()`:

```scala
val futures = group.flows.map { flowConfig =>
  Future { executeFlow(flowConfig, batchId, validatedFlows) }
}
Await.result(Future.sequence(futures), Duration.Inf)
```

Problems:
- `ExecutionContext.Implicits.global` ties to the ForkJoinPool. No control over thread count.
- `Duration.Inf` means a hung flow blocks the entire batch forever.
- `validatedFlows` is a shared `mutable.Map` passed to all futures without synchronization.
- SparkSession is shared across threads. Spark's catalog operations are not fully thread-safe. Concurrent `createOrReplaceTempView` calls from different flows can clobber each other.

---

## 9. Temp View Name Collisions

IcebergTableWriter uses fixed temp view names:

```scala
df.createOrReplaceTempView("_iceberg_scd2_source")
spark.sql(...).createOrReplaceTempView("_iceberg_scd2_staged")
```

OrphanDetector namespaces views with flow and FK names, which is better:

```scala
val removedPksView = s"_orphan_removed_pks_${childFlow.name}_${fk.name}"
```

But if two SCD2 flows execute in parallel (via FlowGroupExecutor), both use `_iceberg_scd2_source` and `_iceberg_scd2_staged`, causing data corruption. The views should include the flow name.

---

## 10. Compare Key via String Concatenation

`SCD2Merger` detects changes by concatenating compare columns with `||` separator:

```scala
concat_ws("||", compareColumns.map(col): _*)
```

If any column value contains `||`, two different records can produce the same compare key, causing false negatives (changes not detected). The correct approach is a struct comparison or hash-based detection:

```scala
val compareStruct = struct(compareColumns.map(col): _*)
```

Struct comparison handles nulls correctly and doesn't depend on separator choice.

---

## 11. Duplicated Code

### FinalModelWriter

Two `write()` overloads share 100+ lines of identical partitioning, compression, and option-setting logic. Bug fixes must be applied in both.

### ConfigEnums

All 10 enums repeat the same fromString/reader/writer boilerplate. The string literal for each case object appears twice: once in `val name = "..."` and once in the `fromString` match arm. Adding a new case requires editing two places per enum, and forgetting one causes a silent runtime failure.

### TimingUtil

`timed[T]` and `timedWithDuration[T]` have nearly identical try/catch blocks. The second could delegate to the first.

### ConfigLoader / BatchModelMapper

Both define identical `ProductHint` implicits:

```scala
implicit def productHint[T]: ProductHint[T] =
  ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
```

Should be extracted to a shared `ConfigHints` object.

---

## 12. Validation at Execution Time Instead of Load Time

Several invariants are only checked when code runs, not when config is loaded:

| Invariant | Where it fails | When it should be checked |
|-----------|---------------|--------------------------|
| SCD2 requires validFrom/validTo/isCurrent columns | `DeltaMergerFactory.create()` at merge time | `ConfigLoader` at startup |
| AggregationFunction supports only 5 of 9 values | `JoinStrategyExecutor` at DAG execution | `ConfigLoader` at startup |
| Environment variables are resolvable | Runtime (unresolved `${VAR}` string) | `ConfigLoader` at startup |
| FK referenced flow exists | `ForeignKeyValidator` at validation time | `ExecutionPlanBuilder` at planning time |
| `maxRejectionRate` is between 0 and 1 | Never (silently misbehaves) | `ConfigLoader` at startup |
| `formatVersion` is 1 or 2 | Iceberg runtime error | `IcebergConfig` validation |
| Partition transform arguments are valid | `IcebergTableManager` at table creation | `ConfigLoader` at startup |

The CLAUDE.md says "Fail fast: validare config al startup, non a runtime." These violations mean the pipeline can run for minutes before hitting a config error.

---

## 13. Missing Abstractions

### No retry strategy

Every I/O operation (file read, JDBC read, Iceberg write, maintenance) fails immediately on error. No retry with backoff, no circuit breaker. For transient failures (network glitch, HDFS hiccup), the entire batch fails.

### No write listener/hook

There's no extension point for post-write actions. If a team wants to send a Slack notification after a flow completes, invalidate a cache, or trigger a downstream job, they must modify framework code.

### Two transformation phases only

FlowExecutor supports `preValidationTransformation` and `postValidationTransformation`. There's no hook for pre-merge or post-merge transformations. In the Iceberg path, merge happens during write, so "post-merge" would require a different integration point.

### No custom merge strategy

`DeltaMergerFactory` is a closed factory: `Full`, `Delta`, `SCD2`. Adding a new merge strategy (e.g., SCD1, SCD3, deduplication-only) requires modifying the factory and the enum. A plugin-based approach would be more extensible.

### Hard-coded file names

`global.yaml`, `domains.yaml`, and the `flows/` directory name are hard-coded in `IngestionPipeline`. Different environments might need different naming conventions.

### JDBCDataReader has no connection management

No connection pooling, no fetch size configuration, no partition strategy for large tables. Compared to `FileDataReader` which has schema enforcement, pattern matching, and type mapping, the JDBC reader is minimal.

---

## 14. Scala-Specific Anti-Patterns

### Stringly-typed SQL in NotNullValidator

```scala
val nullCondition = notNullColumns
  .map(col => s"`$col` IS NULL")
  .mkString(" OR ")
df.filter(nullCondition)
```

Every other validator uses the typed Spark API (`col().isNull`). This one builds raw SQL strings, losing type safety and IDE support. If a column name contains a backtick, this breaks.

### Untyped column types in config

`ColumnConfig.type` is a plain `String` ("integer", "string", etc.). Typos like "integr" are only caught at runtime when Spark tries to parse the type. Should be a sealed trait validated at load time.

### `@unchecked` type erasure in FinalModelMapper

```scala
instance match {
  case mapper: FinalModelMapper[B, F] @unchecked => mapper
}
```

Type parameters are erased at runtime. This match succeeds for any `FinalModelMapper` regardless of its actual type parameters. A `FinalModelMapper[String, Int]` would pass a check for `FinalModelMapper[Customer, Order]`.

### ExecutionPlan rebuilt unnecessarily

`FlowOrchestrator` builds the execution plan at line 59, then builds it again at line 168 inside `runPostBatchOrphanDetection`. The plan is deterministic for the same input; it should be computed once and passed through.

---

## 15. Data Integrity Gaps

### No schema validation before unionByName

`UpsertMerger` unions existing and new data without checking schema compatibility:

```scala
val combined = existing.unionByName(newData)
```

If column types differ (e.g., String vs Int for the same column name), `unionByName` may silently coerce or throw a confusing error.

### FK null handling is undefined

`ForeignKeyValidator` joins child FK column with parent PK column. In SQL semantics, `NULL != NULL`, so null FK values automatically become orphans. This might be the intended behavior, but it's not documented and there's no config option to allow nullable FKs.

### Orphan cascade captures PKs only, not FK context

When `OrphanDetector` deletes child records and propagates cascade, it saves the child's PK columns. But if the child has multiple FKs to different parents, only the first FK's cascade is captured. Subsequent FKs on the same child flow may not propagate correctly.

### TOCTOU in table creation

`IcebergTableManager.createOrUpdateTable()` calls `tableExists()` then `CREATE TABLE IF NOT EXISTS`. Between the check and the creation, another process could create the table with a different schema. The `IF NOT EXISTS` clause prevents a crash but silently uses the other process's schema.

---

## 16. Monitoring Gaps

### ValidationEngine has zero logging

The entire validation pipeline runs without a single log statement. When a validation step takes 10 minutes or rejects 90% of records, there's no visibility until the flow completes and the summary is written.

### MetricsCollector.parseTags crashes on malformed input

```scala
val parts = pair.split(":")
parts(0) -> parts(1)  // ArrayIndexOutOfBoundsException if no ':'
```

No bounds checking. A tag string like `"flow,batch"` (missing colon) crashes the metrics system.

### No execution time tracking per validation step

The framework tracks total flow execution time but not individual validation step duration. When debugging slow flows, there's no way to identify which rule is expensive.

---

## Summary by Priority

### Must fix (correctness or data integrity)

1. Non-exhaustive match in JoinStrategyExecutor — First, Last, CollectList, CollectSet crash at runtime
2. Silent env var substitution failure — secrets/paths silently wrong
3. Thread safety in parallel execution — mutable map without synchronization
4. Temp view name collision in parallel SCD2 — data corruption risk
5. Compare key separator collision — false negatives in change detection

### Should fix (performance)

6. 30+ unnecessary `count()`/`isEmpty` calls — minutes of wasted Spark jobs per batch
7. SCD2Merger five uncached joins — repeated full-outer join evaluation
8. No cache/unpersist discipline anywhere — uncontrolled memory pressure
9. ExecutionPlan rebuilt twice — unnecessary computation

### Should fix (maintainability)

10. SQL injection surface — validate identifiers at config load time
11. FinalModelWriter code duplication — 100+ duplicated lines
12. ConfigEnums boilerplate — 10 enums with identical fromString pattern
13. Config validation at load time, not runtime — fail fast
14. Swallowed exceptions in IcebergTableManager — 8 catch-all blocks hiding real errors

### Nice to have (extensibility)

15. Plugin-based merge strategy
16. Write listener/hook system
17. Retry strategy for I/O operations
18. Additional transformation phases (pre-merge, post-merge)
19. Lock-free MetricsCollector
