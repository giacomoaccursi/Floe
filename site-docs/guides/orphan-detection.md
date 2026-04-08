# Orphan Detection — Post-Batch FK Integrity

## The problem

An ETL framework managing multiple flows connected by foreign key relationships has a structural problem: when a parent flow removes records, child flows can end up with references to keys that no longer exist.

This happens in two concrete scenarios:

1. **Full load**: the parent receives a new complete snapshot that replaces all previous data. If the new snapshot is missing records that existed before, every child pointing to those keys becomes orphaned.

2. **SCD2 with detectDeletes**: the parent receives a snapshot and records absent from the source get closed (soft-delete: `is_current = false`, `is_active = false`). From a logical standpoint those keys are no longer active, but children keep referencing them.

In both cases, intra-batch FK validation does not help: FK validation runs *during* each flow's execution, comparing incoming data against what's already written. But the orphan problem arises *after* all flows have executed, because it's the parent that changes its own data and invalidates the children's references.

### Why FK validation is not enough

The framework's FK validation operates at ingestion time: when the child flow is processed, it checks that every FK value in its input has a match in the parent table. If the parent has already run in the same batch, its data is up to date and the check works for the child's *new* records.

But child records already present in the table (written in previous batches) are not re-validated. They're already in Iceberg, they don't go through the validation pipeline. If the parent has since lost some keys, those child records remain with dangling FKs.

## The solution: post-batch orphan detection

Orphan detection runs **after** all batch flows have executed successfully and **before** Iceberg table maintenance. This ordering is critical: maintenance could expire previous snapshots, making the time travel comparison impossible.

### How it works

#### 1. Determine which parents can generate orphans

Not all load modes can remove records:

| Load Mode | Can remove records? | Reason |
|-----------|-------------------|--------|
| **Full**  | Yes | Replaces all data with the new snapshot |
| **SCD2 with detectDeletes** | Yes | Closes records absent from source (soft-delete) |
| **SCD2 without detectDeletes** | No | Records absent from source are preserved as-is |
| **Delta** | No | Insert and update only, never delete |

#### 2. Find removed keys via time travel

Iceberg maintains snapshot history. Each flow, after writing, records the current `snapshotId` and the `parentSnapshotId` (the snapshot that existed before this execution).

To find keys removed by a parent:

1. Read PKs from the **previous snapshot** (`VERSION AS OF <parentSnapshotId>`)
2. Read PKs from the **current state** of the table
3. Compute the difference: `previous_PKs - current_PKs = removed_PKs`

For SCD2, the "current state" only considers records where `is_current = true`, because closed records are logically deleted even though they're still physically present.

If no previous snapshot exists (first-ever execution of the parent), there's nothing to compare against and the check is skipped.

#### 3. Find orphaned records in children

With the removed parent keys, the detector looks for child records that reference them. The comparison is on the child's FK columns against the parent's referenced columns. Any matches are orphans.

#### 4. Apply the configured action

Each FK relationship in the child declares an `onOrphan` action that determines what to do when orphans are found:

| Action | Behavior | Propagates cascade? |
|--------|----------|-------------------|
| **Warn** | Logs the number of orphans found. Does not modify data | No |
| **Delete** | Removes orphaned records from the child table | Yes |
| **Ignore** | Skips the check entirely for this FK | — |

#### 5. Cascade

When a child flow undergoes a `Delete`, the deleted records have their own PKs and FK values. If a third flow (grandchild) has a FK towards the child, those records also become orphans.

The cascade mechanism works as follows:

1. After deleting orphaned records from the child, the PK and FK columns of deleted records are saved in a `removedKeysByFlow` map. Both column sets are preserved so that downstream flows can look up whichever columns they reference.
2. When processing the grandchild, before performing time travel, the detector checks if its parent (the child) has entries in the cascade map.
3. If the cascade map contains all the columns referenced by the grandchild's FK, those keys are used as "removed keys" instead of time travel. If the referenced columns are not present in the map (column mismatch), the cascade is skipped for that FK and the grandchild is not affected.
4. When a child has multiple FKs and more than one triggers a `Delete`, the cascade map accumulates keys from each delete via union, so downstream flows see the combined set of removed keys.

Cascade propagates only with `Delete`. With `Warn` the detector signals the issue but does not modify data, so the chain stops: there's no point propagating an alarm without having actually removed anything.

#### 6. Topological order

Flows are processed in topological order (the same order used for batch execution). This guarantees that:

- A parent is always analyzed before its children
- Cascade flows in the correct direction (parent → child → grandchild)
- No race conditions occur between flows at the same level

## When does orphan detection run?

Orphan detection is **not** executed on every batch. Two guard conditions must both be satisfied:

### Guard 1 — At least one FK must be actionable

If every FK across all flows has `onOrphan: ignore`, there is nothing to do. The orchestrator checks this before instantiating the detector and returns immediately if no actionable FK exists.

In practice: as long as at least one FK is configured with `onOrphan: warn` or `onOrphan: delete`, this guard passes.

### Guard 2 — The parent flow must be capable of removing records

At the level of each individual parent flow, the detector only processes parents whose load mode can actually remove records:

| Load Mode | Analyzed as parent? |
|-----------|-------------------|
| **Full** | Yes — replaces data, can remove keys |
| **SCD2 with `detectDeletes: true`** | Yes — soft-deletes absent records |
| **SCD2 with `detectDeletes: false`** | No — absent records are preserved |
| **Delta** | No — insert/update only, never deletes |

If the parent was processed in the current batch but its load mode cannot remove records, the detector skips it without performing time travel.

### Summary

```
at least one FK with onOrphan != ignore
    AND parent load mode in {Full, SCD2 with detectDeletes}
        → run orphan detection
```

Only when both conditions are met does the detector actually read Iceberg snapshots and look for orphaned records.

## Configuration

In the child flow's YAML configuration, each FK declares its own `onOrphan`:

```yaml
validation:
  primaryKey: [order_id]
  foreignKeys:
    - columns: [customer_id]
      references:
        flow: customers
        columns: [customer_id]
      onOrphan: warn      # warn | delete | ignore
```

The default is `warn`: report the problem without touching the data.

## Output

Each detection produces an `OrphanReport` included in the batch metadata:

- **flowName**: the child flow containing the orphans
- **fkName**: the FK involved
- **parentFlowName**: the parent flow that removed the keys
- **orphanCount**: number of orphaned records found
- **removedParentKeyCount**: number of keys removed by the parent
- **actionTaken**: action taken (`warn` or `delete`)
- **deletedChildKeyCount**: records actually deleted (only for `delete`)
- **cascadeSource**: if the orphans were caused by a cascade delete (rather than time travel), this field contains the name of the flow whose delete triggered the cascade. `null` when the orphans were detected via time travel directly.

## Example

Consider a data warehouse with three tables:

```
customers (Full load)
    |
    v
orders (Delta) -- FK: orders.customer_id -> customers.customer_id
    |
    v
order_items (Delta) -- FK: order_items.order_id -> orders.order_id
```

### Initial state

**customers**

| customer_id | name     |
|-------------|----------|
| C1          | Alice    |
| C2          | Bob      |
| C3          | Charlie  |

**orders**

| order_id | customer_id | total |
|----------|-------------|-------|
| O1       | C1          | 100   |
| O2       | C2          | 200   |
| O3       | C2          | 150   |

**order_items**

| item_id | order_id | product | qty |
|---------|----------|---------|-----|
| I1      | O1       | Widget  | 2   |
| I2      | O2       | Gadget  | 1   |
| I3      | O3       | Widget  | 5   |

### Batch: a new customers snapshot arrives without C2

The source system sends a Full load of `customers` containing only C1 and C3. Bob (C2) has been removed.

After flow execution:

**customers** (updated): C1 (Alice), C3 (Charlie)

**orders** (unchanged — Delta, no new data): O1→C1, O2→**C2**, O3→**C2**

Orders O2 and O3 are now orphans: they reference C2 which no longer exists.

### Orphan detection in action

The detector processes flows in topological order: `customers → orders → order_items`.

**Step 1**: `customers` is a parent with no outbound FKs. No action.

**Step 2**: `orders` has FK `fk_customer` towards `customers`.

- Time travel on `customers`: previous snapshot had `{C1, C2, C3}`, current state has `{C1, C3}`.
- Removed keys: `{C2}`.
- Orphaned records in `orders`: O2, O3 (both have `customer_id = C2`).

If `onOrphan = warn`: logs "orders.fk_customer has 2 orphaned records (1 parent key removed from customers)". Done, no data modification.

If `onOrphan = delete`:

1. Deletes O2 and O3 from `orders`.
2. Saves the deleted PKs `{O2, O3}` in the cascade map for `orders`.

**Step 3** (only if step 2 was `delete`): `order_items` has FK `fk_order` towards `orders`.

- The detector finds `orders` in the cascade map with keys `{O2, O3}`.
- Searches `order_items` for records with `order_id IN (O2, O3)`: finds I2 and I3.

If `onOrphan = delete`: deletes I2 and I3 from `order_items`.
If `onOrphan = warn`: logs the warning, does not delete.

### Final result (full delete cascade scenario)

**customers**: C1, C3 · **orders**: O1 · **order_items**: I1

All orphaned data has been removed through cascade, maintaining referential integrity.

### Final result (warn scenario)

**customers**: C1, C3 · **orders**: O1, O2, O3 (O2 and O3 reported as orphans) · **order_items**: I1, I2, I3 (not checked because warn does not propagate)

Orphaned data remains in the tables but the team receives notification in the batch metadata and can decide how to intervene.

## Limitations and considerations

- **Time travel and retention**: if Iceberg maintenance has already expired the previous snapshot, time travel fails and the check is silently skipped (with a warning in the log). This is why orphan detection runs before maintenance.

- **First execution**: on the very first batch there is no previous snapshot. The check is skipped because there's no baseline to compare against.

- **Performance**: time travel and left_anti join on PKs are lightweight operations as long as parent tables have a reasonable number of distinct keys. The detector projects only FK and PK columns from the child table (not `SELECT *`), keeping the scan narrow even on wide tables.

- **Atomicity**: each DELETE on an Iceberg table is atomic. If the process fails mid-cascade, tables already cleaned stay clean and tables not yet processed remain untouched. On the next batch the detector retries.

- **Warn as default**: the default is `warn` intentionally. Automatic deletion is a destructive operation that requires a conscious choice. In a production environment it's preferable to signal and let the team decide, rather than silently deleting data.

## Related

- [Flow Configuration — foreignKeys](../configuration/flows.md#foreign-key-fields) — FK and onOrphan configuration
- [Validation Engine — Foreign key integrity](validation.md#foreign-key-integrity) — intra-batch FK validation
- [Iceberg Integration — Post-batch lifecycle](iceberg.md#post-batch-lifecycle) — where orphan detection fits
- [SCD2 Guide](scd2.md) — SCD2 with detectDeletes and orphan implications
- [Architecture: Execution Model](../architecture/execution-model.md) — topological ordering
