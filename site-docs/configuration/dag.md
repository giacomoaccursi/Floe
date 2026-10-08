# DAG Configuration

Brief reference for DAG YAML files. For the complete guide including join strategies, execution model, and examples, see [DAG Aggregation Guide](../guides/dag-aggregation.md).

## Structure

```yaml
name: customer_orders_aggregation
description: "Customers with nested orders"
parallelNodes: true
maxParallelNodes: 4

nodes:
  - id: orders_node
    sourceFlow: orders
    filters:
      - "status != 'cancelled'"

  - id: customers_node
    sourceFlow: customers
    select: [customer_id, name, email, country]
    joins:
      - type: left_outer
        with: orders_node
        conditions:
          - left: customer_id
            right: customer_id
        strategy: nest
        nestAs: orders
```

## Top-level fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | yes | — | DAG name |
| `description` | no | `""` | Human-readable description |
| `version` | no | `""` | Version string |
| `parallelNodes` | no | `false` | Execute independent nodes in parallel |
| `maxParallelNodes` | no | `4` | Positive upper bound on concurrently submitted independent nodes |
| `nodes` | yes | — | List of node definitions |

## Node fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | yes | — | Unique node identifier |
| `description` | string | no | `""` | Human-readable description |
| `sourceFlow` | string | conditional | — | Source flow name. Configure exactly one of `sourceFlow` and `sourceTable`. |
| `sourceTable` | string | conditional | — | Full Iceberg table name for an external table. Configure exactly one of `sourceFlow` and `sourceTable`. |
| `joins` | list | no | `[]` | List of join configurations. Dependencies are inferred automatically. |
| `select` | list | no | all columns | Columns to select from source data |
| `filters` | list | no | none | SQL filter expressions |

## Join fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | yes | — | `inner`, `left_outer`, `right_outer`, `full_outer` |
| `with` | string | yes | — | ID of the node to join with |
| `conditions` | list | yes | — | Non-empty join conditions (`left` = current node column, `right` = `with` node column). Each key pair must have the same Spark data type. |
| `strategy` | string | yes | — | `nest`, `flatten`, `aggregate` |
| `nestAs` | string | no | `nested_records` | Field name for the nested array. Valid only for `nest`. |
| `aggregations` | list | conditional | — | Non-empty aggregation specs required by `aggregate`; invalid for the other strategies. |

## Aggregation fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `column` | string | yes | Column to aggregate |
| `function` | string | yes | `sum`, `count`, `avg`, `min`, `max`, `first`, `last`, `collect_list`, `collect_set` |
| `alias` | string | yes | Output column name |
| `orderBy` | list | for `first`/`last` | Child columns that define ascending semantic order. `first` uses the minimum ordering tuple and `last` the maximum; include a unique tie-breaker for deterministic selection. |

`first` and `last` require a non-empty `orderBy`; the other functions reject `orderBy` because it would otherwise be ignored. `nestAs` and aggregation aliases must be unique and must not collide with parent columns. Nested arrays are sorted by their complete child struct. Both `collect_list` and `collect_set` are sorted by value for deterministic output, and `collect_set` removes duplicates; use Spark-orderable input types.

## Fail-fast validation

The same structural validator runs for YAML loading and the programmatic `DAGOrchestrator` API. It rejects blank or case-insensitively duplicated node IDs, ambiguous sources, missing/self join targets, empty or duplicated join conditions, blank filters or column references, strategy-specific fields that would be ignored, and duplicated output aliases. Cycles and multiple terminal roots are rejected while building the execution plan.

## Learn more

- [DAG Aggregation Guide](../guides/dag-aggregation.md) — join strategies, execution model, complete examples
- [Architecture: Execution Model](../architecture/execution-model.md) — how DAG nodes are scheduled
