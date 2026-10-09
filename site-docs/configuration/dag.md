# DAG Configuration

Brief reference for DAG YAML files. For the complete guide including join strategies, execution model, and examples, see [DAG Aggregation Guide](../guides/dag-aggregation.md).

## Structure

```yaml
name: customer_orders_aggregation
description: "Customers with nested orders"

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
        orderBy: [order_id]
```

## Top-level fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | yes | — | DAG name |
| `description` | no | `""` | Human-readable description |
| `version` | no | `""` | Version string |
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
| `orderBy` | list | for `nest` | — | Child columns defining the nested array's ascending lexicographic order. Required and valid only for `nest`; use a unique final tie-breaker. |
| `aggregations` | list | conditional | — | Non-empty aggregation specs required by `aggregate`; invalid for the other strategies. |

## Aggregation fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `column` | string | yes | Column to aggregate |
| `function` | string | yes | `sum`, `count`, `avg`, `min`, `max`, `first`, `last`, `collect_list`, `collect_set` |
| `alias` | string | yes | Output column name |
| `orderBy` | list | for `first`/`last` | Child columns that define ascending semantic order. `first` uses the minimum ordering tuple and `last` the maximum; include a unique tie-breaker for deterministic selection. |

The join-level `orderBy` and `aggregations[].orderBy` are separate fields. `first` and `last` require a non-empty aggregation-level `orderBy`; the other functions reject it because it would otherwise be ignored. `nestAs` and aggregation aliases must be unique and must not collide with parent columns. Nest and aggregation ordering keys, plus inputs to `min`, `max`, `collect_list`, and `collect_set`, must be Spark-orderable; the nested payload and the value selected by `first`/`last` may contain non-orderable data such as maps. `sum` and `avg` accept numeric or ANSI interval columns only; Floe does not allow Spark to coerce strings implicitly. Collection outputs are sorted by value for deterministic output, and `collect_set` removes duplicates. For an outer-join parent with no matching children, `count` returns `0`, collection functions return `[]`, and nullable scalar aggregates remain `NULL`.

## Fail-fast validation

The same structural validator runs for YAML loading and the programmatic `DAGOrchestrator` API. It rejects blank or case-insensitively duplicated node IDs, ambiguous sources, missing/self join targets, empty or duplicated join conditions, blank filters or column references, strategy-specific fields that would be ignored, and duplicated output aliases. Cycles and multiple terminal roots are rejected while building the execution plan.

## Learn more

- [DAG Aggregation Guide](../guides/dag-aggregation.md) — join strategies, execution model, complete examples
- [Architecture: Execution Model](../architecture/execution-model.md) — how DAG nodes are scheduled
