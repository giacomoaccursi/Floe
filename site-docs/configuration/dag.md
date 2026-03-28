# DAG Configuration

Brief reference for DAG YAML files. For the complete guide including join strategies, execution model, and examples, see [DAG Aggregation Guide](../guides/dag-aggregation.md).

## Structure

```yaml
name: customer_orders_aggregation
description: "Aggregates customers with their orders and order items"
version: "1.0"
nodes:
  - id: customers_node
    description: "Customer base table"
    sourceFlow: customers
    dependencies: []
    select: [customer_id, name, email, country]

  - id: orders_node
    description: "Orders joined to customers"
    sourceFlow: orders
    dependencies: [customers_node]
    join:
      type: left_outer
      parent: customers_node
      conditions:
        - left: customer_id
          right: customer_id
      strategy: nest
      nestAs: orders
    select: [order_id, customer_id, status, total_amount, order_date]
    filters:
      - "status != 'cancelled'"
```

## Node fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | yes | — | Unique node identifier |
| `description` | string | no | `""` | Human-readable description |
| `sourceFlow` | string | yes | — | Name of the source flow |
| `sourceTable` | string | — | — | Iceberg table name (overrides the default `{catalogName}.default.{sourceFlow}`) |
| `dependencies` | list | yes | — | List of node IDs this node depends on |
| `join` | object | — | — | Join configuration |
| `select` | list | — | all columns | Columns to select from source data |
| `filters` | list | — | none | SQL filter expressions |

## Join fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | yes | — | `inner`, `left_outer`, `right_outer`, `full_outer` |
| `parent` | string | yes | — | ID of the parent node |
| `conditions` | list | yes | — | Join conditions (`left` = parent column, `right` = child column) |
| `strategy` | string | yes | — | `nest`, `flatten`, `aggregate` |
| `nestAs` | string | — | `nested_records` | Field name for nested array (Nest only) |
| `aggregations` | list | — | — | Aggregation specs (Aggregate only) |

## Aggregation fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `column` | string | yes | Column to aggregate |
| `function` | string | yes | `sum`, `count`, `avg`, `min`, `max`, `first`, `last`, `collect_list`, `collect_set` |
| `alias` | string | yes | Output column name |

## Learn more

- [DAG Aggregation Guide](../guides/dag-aggregation.md) — join strategies, execution model, auto-discovery, complete examples
- [Architecture: Execution Model](../architecture/execution-model.md) — how DAG nodes are scheduled
