# DAG Aggregation

## Overview

The DAG aggregation module builds a directed acyclic graph of data transformations that join, filter, and aggregate data from multiple flows. Each node in the DAG represents a data source (an Iceberg table derived from a flow) and optionally joins its data with a parent node using one of three strategies: Nest, Flatten, or Aggregate.

The DAG is defined in a YAML configuration file. At execution time, the framework resolves dependencies, detects cycles, groups independent nodes for parallel execution, and produces a single output DataFrame from the root node.

## Architecture

The module validates the DAG configuration, resolves dependencies from join declarations, detects cycles, groups independent nodes for parallel execution, and produces a single output DataFrame from the root node. Three join strategies are supported: Nest, Flatten, and Aggregate.

DAG nodes can reference any Iceberg table in the catalog, including [derived tables](pipeline-builder.md#derived-tables) produced by the pipeline.

## DAG configuration

A DAG is defined in a YAML file with this structure:

```yaml
name: customer_orders_aggregation
description: "Customers with nested orders and item counts"

nodes:
  - id: order_items_node
    sourceFlow: order_items

  - id: orders_node
    sourceFlow: orders
    filters:
      - "status != 'cancelled'"
    joins:
      - type: left_outer
        with: order_items_node
        conditions:
          - left: order_id
            right: order_id
        strategy: aggregate
        aggregations:
          - column: quantity
            function: sum
            alias: total_quantity
          - column: item_id
            function: count
            alias: item_count

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

For the field reference, see [DAG Configuration](../configuration/dag.md).

### Node reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | yes | — | Unique node identifier |
| `description` | string | no | `""` | Human-readable description |
| `sourceFlow` | string | — | — | Name of the source flow. Reads from `{catalogName}.{namespace}.{sourceFlow}`. Required if `sourceTable` is not set. |
| `sourceTable` | string | — | — | Full Iceberg table name. Use instead of `sourceFlow` for external tables. |
| `joins` | list | — | `[]` | List of join configurations. Dependencies are inferred automatically. |
| `select` | list | — | all columns | Columns to select from source data |
| `filters` | list | — | none | SQL filter expressions applied to source data |

### Join configuration reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | yes | — | Join type: `inner`, `left_outer` (or `left`), `right_outer` (or `right`), `full_outer` (or `full`) |
| `with` | string | yes | — | ID of the node to join with |
| `conditions` | list | yes | — | Join conditions: `left` = column on the current node, `right` = column on the `with` node |
| `strategy` | string | yes | — | Join strategy: `nest`, `flatten`, `aggregate` |
| `nestAs` | string | — | `nested_records` | Field name for nested array (Nest strategy only) |
| `aggregations` | list | — | — | Aggregation specs (Aggregate strategy only) |

### Aggregation specification

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `column` | string | yes | Column to aggregate |
| `function` | string | yes | Aggregation function |
| `alias` | string | yes | Output column name |

Supported aggregation functions: `sum`, `count`, `avg` (alias: `average`), `min`, `max`, `first`, `last`, `collect_list`, `collect_set`.

## sourceTable — reading from external Iceberg tables

By default, a node reads from `{catalogName}.{namespace}.{sourceFlow}`. If you need to read from a table outside the framework's naming convention (e.g. a table created by another system, or in a different catalog/namespace), use `sourceTable` instead of `sourceFlow`:

```yaml
- id: external_customers_node
  description: "Customers from external catalog"
  sourceTable: "other_catalog.analytics.customers"
```

When `sourceTable` is set, `sourceFlow` is not needed. Use one or the other:

- `sourceFlow: customers` → reads from `{catalogName}.default.customers` (framework-managed table)
- `sourceTable: "other_catalog.analytics.customers"` → reads from the exact table specified

## Join strategies

### Nest

Combines a parent table with a child table by grouping all matching child records into a nested array column on each parent row. Use this when you want a denormalized structure — for example, a customer row with all their orders embedded as an array.

```yaml
- id: customers_node
  sourceFlow: customers
  joins:
    - type: left_outer
      with: orders_node
      conditions:
        - left: customer_id
          right: customer_id
      strategy: nest
      nestAs: orders
```

In `conditions`, `left` refers to the current node's column and `right` refers to the `with` node's column.

Result (`customers` with nested `orders`):

| customer_id | name | orders |
|-------------|------|--------|
| 1 | Alice | [{order_id: 101, total: 50.0}, {order_id: 102, total: 30.0}] |
| 2 | Bob | [{order_id: 103, total: 75.0}] |
| 3 | Charlie | [] |

With a `left_outer` join, parents with no matching children get an empty array (`[]`), not NULL. With an `inner` join, those parents are excluded from the result.

The child records are grouped by the join key columns using `collect_list(struct(*))`. The `nestAs` field controls the output column name (defaults to `nested_records`).

### Flatten

Combines a parent table with a child table by adding the child's columns directly alongside the parent's columns — a standard SQL join. Use this when the relationship is 1:1 or when you want one row per match.

```yaml
- id: orders_node
  sourceFlow: orders
  joins:
    - type: left_outer
      with: shipping_node
      conditions:
        - left: order_id
          right: order_id
      strategy: flatten
```

Result (`orders` with `shipping` columns flattened in):

| order_id | status | shipping_date | carrier |
|----------|--------|---------------|---------|
| 101 | shipped | 2024-01-15 | FedEx |
| 102 | pending | NULL | NULL |

If a child column has the same name as a parent column (excluding join keys), it is automatically prefixed with the node ID (e.g. `orders_node_status`) to avoid ambiguity. Join key columns from the child side are dropped.

!!!warning "Fan-out risk"
    Flatten is a standard join — if a parent row matches multiple child rows (1:N relationship), the parent row is duplicated for each match. Use Nest or Aggregate if you need to preserve the parent's cardinality.

### Aggregate

Combines a parent table with a child table by computing summary statistics from the child records and joining them back to the parent. Use this when you want one row per parent with aggregated values from the children.

```yaml
- id: orders_node
  sourceFlow: orders
  joins:
    - type: left_outer
      with: order_items_node
      conditions:
        - left: order_id
          right: order_id
      strategy: aggregate
      aggregations:
        - column: quantity
          function: sum
          alias: total_quantity
        - column: item_id
          function: count
          alias: item_count
        - column: unit_price
          function: avg
          alias: avg_price
```

At least one aggregation is required — an empty `aggregations` list throws `ValidationConfigException`.

The child is grouped by the join key columns, aggregated, then joined to the parent. Only the parent columns and the aggregation aliases appear in the output.

!!!note "Data skew on aggregate and nest joins"
    Aggregate and Nest strategies use `groupBy` on the join key columns. If a key value is heavily skewed (e.g. one customer has 90% of all orders), the executor handling that key becomes a bottleneck. Spark's Adaptive Query Execution (AQE) handles skew automatically for regular joins (Flatten strategy) but not for `groupBy` aggregations.

## Filters and select

Filters and select are applied to the node's source data **before** any join with the parent.

Filters are SQL expressions passed to `DataFrame.filter()`. Multiple filters in the list are applied sequentially (implicit AND). You can use `AND`, `OR`, `NOT`, and any Spark SQL predicate within a single filter:

```yaml
filters:
  - "status != 'cancelled'"
  - "total_amount > 0 OR is_refund = true"
```

Select restricts which columns are kept:

```yaml
select: [order_id, customer_id, status, total_amount]
```

If `select` is empty, all columns are included.

The execution order is: **load source → apply filters → apply select → join with parent**. This means filters can reference columns that are not in the `select` list — they are applied before column selection.

## Dependency resolution and execution order

The DAG is executed in topological order. The framework:

1. Validates all nodes: checks for duplicate IDs, missing dependencies, self-referential joins, empty join conditions.
2. Builds a dependency graph. Join parents are implicit dependencies — a node cannot execute before its join parent even if not listed in `dependencies`.
3. Performs topological sort. If a cycle is detected, a `CircularDependencyException` is thrown with the full cycle path.
4. Groups nodes by execution level: nodes whose dependencies are all satisfied can run in the same group.

### Parallel execution

Nodes within the same execution group can run in parallel if `parallelNodes` is `true` in the DAG YAML:

```yaml
parallelNodes: true
```

Parallel execution uses a bounded thread pool sized at `availableProcessors * 2` to avoid saturating the driver.

If `parallelNodes` is `false`, all groups execute sequentially regardless of independence.

### Root node

The root node is the node that no other node depends on — it produces the final output DataFrame. If multiple root nodes exist, the first one is used (with a warning). If no root node exists (all nodes are dependencies of others), an error is thrown.

## Complete DAG example

```yaml
name: customer_360
description: "Customer 360 view with orders, items, and shipping"
version: "1.0"
nodes:
  - id: order_items_node
    sourceFlow: order_items

  - id: shipping_node
    sourceFlow: shipping

  - id: orders_node
    description: "Orders enriched with item aggregates and shipping"
    sourceFlow: orders
    filters:
      - "order_date >= '2024-01-01'"
    select: [order_id, customer_id, status, total_amount, order_date]
    joins:
      - type: left_outer
        with: order_items_node
        conditions:
          - left: order_id
            right: order_id
        strategy: aggregate
        aggregations:
          - column: quantity
            function: sum
            alias: total_items
          - column: line_total
            function: sum
            alias: items_total
          - column: product_id
            function: collect_set
            alias: unique_products
      - type: left_outer
        with: shipping_node
        conditions:
          - left: order_id
            right: order_id
        strategy: flatten

  - id: customers_node
    description: "Customer 360 with nested orders"
    sourceFlow: customers
    select: [customer_id, name, email, country, segment]
    joins:
      - type: left_outer
        with: orders_node
        conditions:
          - left: customer_id
            right: customer_id
        strategy: nest
        nestAs: orders
```

In this DAG:

- `order_items_node` and `shipping_node` are leaf nodes — they load data from Iceberg
- `orders_node` joins with both `order_items_node` (aggregate) and `shipping_node` (flatten) — multiple joins on a single node
- `customers_node` is the root — it nests the enriched orders into each customer
- `order_items_node` and `shipping_node` can execute in parallel (no dependency between them)

## Error handling

| Error | When | Exception |
|-------|------|-----------|
| Duplicate node IDs | Graph build | `IllegalStateException` |
| Missing dependency | Graph build | `IllegalStateException` |
| Self-referential join | Graph build | `IllegalStateException` |
| Empty join conditions | Graph build | `IllegalStateException` |
| Circular dependency | Topological sort | `CircularDependencyException` (includes cycle path) |
| Missing parent result | Node execution | `IllegalStateException` |
| Empty aggregations on Aggregate strategy | Join execution | `ValidationConfigException` |
| No root node found | Graph build | `IllegalStateException` |

## Related

- [DAG Configuration](../configuration/dag.md) — YAML field reference
- [Pipeline Builder — Derived Tables](pipeline-builder.md#derived-tables) — producing Iceberg tables for DAG consumption
- [Architecture: Execution Model](../architecture/execution-model.md) — how DAG nodes are scheduled
- [Reference: Exceptions](../reference/exceptions.md) — exception hierarchy
