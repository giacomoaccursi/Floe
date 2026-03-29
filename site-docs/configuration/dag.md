# DAG Configuration

Brief reference for DAG YAML files. For the complete guide including join strategies, execution model, and examples, see [DAG Aggregation Guide](../guides/dag-aggregation.md).

## Structure

```yaml
name: customer_orders_aggregation
description: "Aggregates customers with their orders"

nodes:
  - id: customers_node
    sourceFlow: customers
    select: [customer_id, name, email, country]

  - id: orders_node
    sourceFlow: orders
    filters:
      - "status != 'cancelled'"
    joins:
      - type: left_outer
        with: customers_node
        conditions:
          - left: customer_id
            right: customer_id
        strategy: nest
        nestAs: orders
```

## Node fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | yes | — | Unique node identifier |
| `description` | string | no | `""` | Human-readable description |
| `sourceFlow` | string | conditional | — | Source flow name. Required if `sourceTable` is not set. |
| `sourceTable` | string | conditional | — | Full Iceberg table name. Use instead of `sourceFlow` for external tables. |
| `joins` | list | no | `[]` | List of join configurations. Dependencies are inferred automatically. |
| `select` | list | no | all columns | Columns to select from source data |
| `filters` | list | no | none | SQL filter expressions |

## Join fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | yes | — | `inner`, `left_outer`, `right_outer`, `full_outer` |
| `with` | string | yes | — | ID of the node to join with |
| `conditions` | list | yes | — | Join conditions (`left` = current node column, `right` = `with` node column) |
| `strategy` | string | yes | — | `nest`, `flatten`, `aggregate` |
| `nestAs` | string | no | `nested_records` | Field name for nested array (Nest only) |
| `aggregations` | list | no | — | Aggregation specs (Aggregate only) |

## Aggregation fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `column` | string | yes | Column to aggregate |
| `function` | string | yes | `sum`, `count`, `avg`, `min`, `max`, `first`, `last`, `collect_list`, `collect_set` |
| `alias` | string | yes | Output column name |

## Learn more

- [DAG Aggregation Guide](../guides/dag-aggregation.md) — join strategies, execution model, complete examples
- [Architecture: Execution Model](../architecture/execution-model.md) — how DAG nodes are scheduled
