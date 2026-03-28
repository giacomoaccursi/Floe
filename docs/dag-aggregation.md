# DAG Aggregation

## Overview

The DAG aggregation module builds a directed acyclic graph of data transformations that join, filter, and aggregate data from multiple flows. Each node in the DAG represents a data source (a flow's output or an Iceberg table) and optionally joins its data with a parent node using one of three strategies: Nest, Flatten, or Aggregate.

The DAG is defined in a YAML configuration file. At execution time, the framework resolves dependencies, detects cycles, groups independent nodes for parallel execution, and produces a single output DataFrame from the root node.

## Architecture

The module is composed of five components:

| Component | Responsibility |
|-----------|---------------|
| `DAGOrchestrator` | Entry point. Loads config, runs discovery, delegates to graph builder and executor. |
| `DAGGraphBuilder` | Validates nodes, builds dependency graph, topological sort, groups nodes for parallel execution. |
| `DAGExecutor` | Executes the plan group by group, sequential or parallel. |
| `DAGNodeProcessor` | Executes a single node: loads source data, applies filters/select, joins with parent. |
| `JoinStrategyExecutor` | Implements the three join strategies (Nest, Flatten, Aggregate). |

Additionally, `AdditionalTableDiscovery` can auto-discover tables registered during transformations (via `TransformationContext.addTable()`) and inject them as DAG nodes.

## DAG configuration

A DAG is defined in a YAML file with this structure:

```yaml
name: customer_orders_aggregation
description: "Aggregates customers with their orders and order items"
version: "1.0"
nodes:
  - id: customers_node
    description: "Customer base table"
    sourceFlow: customers
    sourcePath: "output/warehouse/customers"
    dependencies: []
    select: [customer_id, name, email, country]

  - id: orders_node
    description: "Orders joined to customers"
    sourceFlow: orders
    sourcePath: "output/warehouse/orders"
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

  - id: order_items_node
    description: "Order items aggregated per order"
    sourceFlow: order_items
    sourcePath: "output/warehouse/order_items"
    dependencies: [orders_node]
    join:
      type: left_outer
      parent: orders_node
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
```

### Node reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | yes | — | Unique node identifier |
| `description` | string | yes | — | Human-readable description |
| `sourceFlow` | string | yes | — | Name of the source flow |
| `sourcePath` | string | yes | — | Path to source data (Parquet) |
| `sourceTable` | string | — | — | Iceberg table name (overrides `sourcePath` if set) |
| `dependencies` | list | yes | — | List of node IDs this node depends on |
| `join` | object | — | — | Join configuration (absent for root/leaf nodes with no parent) |
| `select` | list | — | all columns | Columns to select from source data |
| `filters` | list | — | none | SQL filter expressions applied to source data |

### Join configuration reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | yes | — | Join type: `inner`, `left_outer` (or `left`), `right_outer` (or `right`), `full_outer` (or `full`) |
| `parent` | string | yes | — | ID of the parent node to join with |
| `conditions` | list | yes | — | Join conditions: `left` = column on the **parent** node, `right` = column on the **current** (child) node |
| `strategy` | string | yes | — | Join strategy: `nest`, `flatten`, `aggregate` |
| `nestAs` | string | — | `nested_records` | Field name for nested array (Nest strategy only) |
| `aggregations` | list | — | — | Aggregation specs (Aggregate strategy only) |

Note on `conditions`: the naming can be counterintuitive. `left` refers to the parent DataFrame (the node referenced by `parent`), and `right` refers to the current node's source data. This matches the join order: `parent.join(child, parent(left) === child(right))`.

### Aggregation specification

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `column` | string | yes | Column to aggregate |
| `function` | string | yes | Aggregation function |
| `alias` | string | yes | Output column name |

Supported aggregation functions: `sum`, `count`, `avg` (alias: `average`), `min`, `max`, `first`, `last`, `collect_list`, `collect_set`.

## sourceTable — reading from Iceberg

By default, a node reads its source data from Parquet files at `sourcePath`. Setting `sourceTable` overrides this behavior and reads directly from an Iceberg table using `spark.table()`:

```yaml
- id: customers_node
  description: "Customers from Iceberg"
  sourceFlow: customers
  sourcePath: "output/warehouse/customers"  # fallback, not used when sourceTable is set
  sourceTable: "spark_catalog.default.customers"
  dependencies: []
```

This is useful when the DAG needs to read the latest committed state of a table (including data from previous batches) rather than the raw output files of the current batch.

## Join strategies

### Nest

Groups child records into a nested array column on the parent. The result is a denormalized structure where each parent row contains an array of its related child records.

```yaml
strategy: nest
nestAs: orders
```

Given a parent `customers` and child `orders`:

| customer_id | name | orders |
|-------------|------|--------|
| 1 | Alice | [{order_id: 101, total: 50.0}, {order_id: 102, total: 30.0}] |
| 2 | Bob | [{order_id: 103, total: 75.0}] |
| 3 | Charlie | [] |

If a parent has no matching children (left outer join), the nested array is empty (`[]`), not NULL.

The child records are grouped by the join key columns using `collect_list(struct(*))`. The `nestAs` field controls the output column name (defaults to `nested_records`).

### Flatten

Flattens child columns directly into the parent row. This is a standard join where child columns are added alongside parent columns.

```yaml
strategy: flatten
```

Given a parent `orders` and child `shipping`:

| order_id | status | shipping_date | carrier |
|----------|--------|---------------|---------|
| 101 | shipped | 2024-01-15 | FedEx |
| 102 | pending | NULL | NULL |

If a child column has the same name as a parent column (excluding join keys), it is automatically prefixed with `child_` to avoid ambiguity. Join key columns from the child side are dropped.

**Important:** Flatten is a standard join — if a parent row matches multiple child rows (1:N relationship), the parent row is duplicated for each match. This is a fan-out. Use Nest or Aggregate if you need to preserve the parent's cardinality.

### Aggregate

Aggregates child records using specified functions and joins the results to the parent. This is the most flexible strategy for producing summary statistics.

```yaml
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

## Filters and select

Filters and select are applied to the node's source data before any join with the parent.

Filters are SQL expressions:

```yaml
filters:
  - "status != 'cancelled'"
  - "order_date >= '2024-01-01'"
```

Filters are applied sequentially using `DataFrame.filter()`. Each filter expression must be a valid Spark SQL predicate.

Select restricts which columns are kept:

```yaml
select: [order_id, customer_id, status, total_amount]
```

If `select` is empty, all columns are included.

The execution order is: load source → apply filters → apply select → join with parent. This means filters can reference columns that are not in the `select` list — they are applied before column selection.

## Dependency resolution and execution order

The DAG is executed in topological order. The framework:

1. Validates all nodes: checks for duplicate IDs, missing dependencies, self-referential joins, empty join conditions.
2. Builds a dependency graph. Join parents are implicit dependencies — a node cannot execute before its join parent even if not listed in `dependencies`.
3. Performs topological sort. If a cycle is detected, a `CircularDependencyException` is thrown with the full cycle path.
4. Groups nodes by execution level: nodes whose dependencies are all satisfied can run in the same group.

### Parallel execution

Nodes within the same execution group can run in parallel if `performance.parallelNodes` is `true` in `global.yaml`:

```yaml
performance:
  parallelFlows: true
  parallelNodes: true
```

Parallel execution uses a bounded thread pool sized at `availableProcessors * 2` to avoid saturating the driver. The timeout for parallel groups is 2 hours.

If `parallelNodes` is `false`, all groups execute sequentially regardless of independence.

### Root node

The root node is the node that no other node depends on — it produces the final output DataFrame. If multiple root nodes exist, the first one is used (with a warning). If no root node exists (all nodes are dependencies of others), an error is thrown.

## Auto-discovery of additional tables

When `autoDiscoverAdditionalTables` is enabled on the `DAGOrchestrator`, the framework scans the metadata directory for tables registered during transformations via `TransformationContext.addTable()`.

Each discovered table is converted into a DAG node with:
- ID: `{tableName}_node`
- Dependencies inferred from `joinKeys` metadata
- Join configuration inferred from the first entry in `joinKeys` (left outer, Nest strategy)

The discovered nodes are appended to the configured nodes before the execution plan is built. This allows transformation-generated tables to participate in the DAG without explicit YAML configuration.

Discovery reads JSON metadata files from `{metadataPath}/latest/additional_tables/`. Each file must contain:

```json
{
  "table_name": "order_summaries",
  "path": "output/warehouse/order_summaries",
  "dag_metadata": {
    "primary_key": ["order_id"],
    "join_keys": {
      "customers": ["customer_id"]
    },
    "description": "Order summaries by customer",
    "partition_by": ["order_date"]
  }
}
```

If the metadata directory does not exist or contains no valid files, discovery returns an empty list silently.

## Complete DAG example

```yaml
name: customer_360
description: "Customer 360 view with orders, items, and shipping"
version: "1.0"
nodes:
  - id: customers_node
    description: "Customer dimension"
    sourceFlow: customers
    sourcePath: "output/warehouse/customers"
    sourceTable: "spark_catalog.default.customers"
    dependencies: []
    select: [customer_id, name, email, country, segment]

  - id: orders_node
    description: "Orders nested into customers"
    sourceFlow: orders
    sourcePath: "output/warehouse/orders"
    dependencies: [customers_node]
    filters:
      - "order_date >= '2024-01-01'"
    select: [order_id, customer_id, status, total_amount, order_date]
    join:
      type: left_outer
      parent: customers_node
      conditions:
        - left: customer_id
          right: customer_id
      strategy: nest
      nestAs: orders

  - id: items_node
    description: "Item counts and totals per order"
    sourceFlow: order_items
    sourcePath: "output/warehouse/order_items"
    dependencies: [orders_node]
    join:
      type: left_outer
      parent: orders_node
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

  - id: shipping_node
    description: "Shipping details flattened into orders"
    sourceFlow: shipping
    sourcePath: "output/warehouse/shipping"
    dependencies: [orders_node]
    join:
      type: left_outer
      parent: orders_node
      conditions:
        - left: order_id
          right: order_id
      strategy: flatten
```

In this DAG:
- `customers_node` is a leaf node (no join, no dependencies) — it loads customer data from Iceberg
- `orders_node` depends on `customers_node` and nests orders as an array column
- `items_node` and `shipping_node` both depend on `orders_node` and can execute in parallel
- `items_node` aggregates order items per order
- `shipping_node` flattens shipping columns into the order record
- The root node is determined automatically (the node no other node depends on)

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
