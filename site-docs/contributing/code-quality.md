# Code Quality

Scala style rules and Spark best practices for contributing to the framework.

## Scala style

### Immutability and safety

- Prefer immutability: `val` over `var`, immutable collections by default
- Use `Option` over `null` — pattern match on `Option` instead of null checks
- Prefer pattern matching over chains of `if/else`
- Write pure functions: same input → same output, no hidden side effects

### Naming and structure

- Use explicit, descriptive names — clarity over brevity
- Keep functions small and single-responsibility
- No unnecessary complexity: three similar lines beat a premature abstraction
- Do not add features, refactoring, or "improvements" beyond what is explicitly requested

### Documentation

- Do not add docstrings, comments, or type annotations to code that was not modified
- Comments should explain *why*, not *what* — the code should be self-explanatory
- Keep comments up to date when modifying code

## Spark best practices

### Avoid expensive operations

- **Never `.collect()` on large DataFrames** without explicit justification; prefer distributed aggregations
- **Avoid redundant `count()`**: if a count is already available from a previous operation, reuse it arithmetically
- **Minimize Spark actions**: every `count()`, `collect()`, `save()` is a separate Spark job. Group operations to reduce job count

### Caching

- **Cache with `try-finally`**: every `df.cache()` must have a corresponding `df.unpersist()` guaranteed by a `try-finally` block

```scala
val cached = df.cache()
try {
  // use cached
} finally {
  cached.unpersist()
}
```

### Joins

- **Join awareness**: avoid repeated joins on the same datasets; derive projections from existing join results instead of re-joining
- **Let AQE decide**: Spark's Adaptive Query Execution automatically chooses the optimal join strategy (broadcast vs shuffle) at runtime based on actual data sizes. Do not force `broadcast()` explicitly — AQE handles it better in most cases.

```scala
// Good: let AQE decide the join strategy
val enriched = orders.join(customers, Seq("customer_id"), "left")

// Bad: re-joining the same tables multiple times
val step1 = orders.join(customers, Seq("customer_id"))
val step2 = orders.join(customers, Seq("customer_id"))  // redundant join
```

### Partitioning

- Do not partition on high-cardinality columns (IDs, fine-grained timestamps)
- Prefer low-cardinality columns (dates, categories)
- Do not partition on boolean columns — only 2 values, creates severely imbalanced partitions

### Parallelism

- **Bounded parallelism**: do not use `ExecutionContext.Implicits.global` for parallel Spark operations; use an explicitly sized thread pool
- Size thread pools based on the workload, not the number of available processors

### Action awareness

Every change touching DataFrames, joins, aggregations, or writes must be evaluated against the number of Spark actions it generates. Fewer actions = fewer jobs = less latency.

!!!tip "Counting actions"
    Before submitting a PR that modifies DataFrame operations, count the number of Spark actions (count, collect, save, show) in the affected code path. If you can reduce the count without sacrificing correctness, do it.

## Code review checklist

When reviewing Spark code, check for:

- [ ] No `.collect()` on unbounded DataFrames
- [ ] No redundant `count()` calls
- [ ] Every `cache()` has a matching `unpersist()` in a `try-finally`
- [ ] No repeated joins on the same datasets
- [ ] No forced `broadcast()` — let AQE decide join strategy
- [ ] Partitioning uses low-cardinality columns
- [ ] Thread pools are explicitly sized (no global execution context)
- [ ] Minimal number of Spark actions

## Related

- [Development Setup](development.md) — build and test
- [Workflow](workflow.md) — commit conventions and PR process
