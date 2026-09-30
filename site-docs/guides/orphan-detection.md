# Orphan Detection — Post-Batch FK Integrity

## The problem

When a parent table loses a key, records already stored in a child table can become orphaned. Ordinary FK validation is not sufficient: it validates the child records ingested in the current attempt, not every historical child record already present in Iceberg.

This can happen when the parent uses:

- **Full load**, which replaces the table state and can remove keys;
- **SCD2 with `detectDeletes: true`**, which closes keys absent from the incoming complete snapshot.

Delta loads and SCD2 loads without delete detection do not remove logical parent keys and therefore cannot trigger this check.

## Supported behavior

FLOe performs post-flow detection and supports two actions:

| Action | Behavior |
|--------|----------|
| `warn` | Detects and reports current child records that reference parent keys removed by this attempt. It never changes child data. |
| `ignore` | Does not run post-batch detection for that FK. |

`warn` is the default.

`onOrphan: delete` is deliberately unsupported and is rejected during configuration loading or programmatic pipeline validation. Automatic deletion would be a second functional write, potentially followed by more deletes across descendants. Iceberg makes each table commit atomic, not an arbitrary multi-table cascade. Without a durable worklist and recovery protocol, a terminated driver could leave the cascade partially applied and a later run could not prove which deletes remain. FLOe therefore reports the integrity violation and leaves remediation to an explicit, reviewed workflow.

## Detection algorithm

### 1. Establish whether the parent can remove keys

| Parent load mode | Checked? | Reason |
|------------------|----------|--------|
| Full | Yes | The new snapshot can omit old keys. |
| SCD2 with `detectDeletes: true` | Yes | Missing current keys are closed. |
| SCD2 with `detectDeletes: false` | No | Missing keys remain current. |
| Delta | No | The supported keyed merge inserts or updates; it does not delete. |

### 2. Compute the removed parent-key set

For a qualifying parent, FLOe uses the parent snapshot recorded for the write:

1. project the referenced key columns from the previous snapshot;
2. project the same columns from the resulting parent state;
3. calculate `previous keys LEFT ANTI current keys`.

For SCD2, both sides include only current versions (`is_current = true`, or the configured equivalent). Closed history is not repeatedly classified as a new removal.

The first successful write has no previous snapshot, so there is no removal baseline and the check is skipped.

### 3. Match current child records

FLOe joins the removed parent keys to the child FK columns and counts matching current child records. For an SCD2 child, only its current versions are considered. The result is logged and included in attempt metadata; child rows are never mutated by orphan detection.

### 4. Fail safely when evidence is unavailable

If a required retained snapshot cannot be read, or the child state cannot be inspected, orphan detection fails the attempt. FLOe does not turn missing evidence into an empty orphan set. Derived tables are not run after this failure, and already committed flow tables are not rolled back.

## Execution conditions

Detection runs after all ordinary flows have succeeded and before derived tables when both conditions hold:

1. at least one FK has `onOrphan: warn`;
2. its parent uses Full load or SCD2 with `detectDeletes: true`.

If every FK uses `ignore`, the entire phase is skipped.

Flows are inspected in dependency order. The detector reports direct FK violations only; because it never deletes child rows, there is no delete cascade to propagate.

## Configuration

```yaml
validation:
  primaryKey: [order_id]
  foreignKeys:
    - columns: [customer_id]
      references:
        flow: customers
        columns: [customer_id]
      onOrphan: warn      # warn | ignore
```

Use `ignore` only when dangling references are explicitly acceptable or monitored elsewhere:

```yaml
validation:
  foreignKeys:
    - columns: [legacy_customer_id]
      references:
        flow: customers
        columns: [customer_id]
      onOrphan: ignore
```

## Output

Every detected relationship produces an `OrphanReport` in the attempt report:

| Field | Meaning |
|-------|---------|
| `flowName` | Child flow containing the orphaned rows. |
| `fkName` | Human-readable FK identity. |
| `parentFlowName` | Parent flow that lost the referenced keys. |
| `orphanCount` | Current child records referencing removed keys. |
| `removedParentKeyCount` | Distinct removed parent keys considered. |
| `actionTaken` | `warn`. |

The warning is a functional data-quality finding, not proof that downstream data is safe. Route the attempt report to the owning team and define an external remediation process with approval, predicates, validation, and audit evidence appropriate to the domain.

## Example

Suppose the previous `customers` snapshot contains `{C1, C2, C3}` and the new Full load contains `{C1, C3}`. Existing `orders` still contains two rows that reference `C2`.

FLOe:

1. reads the previous and resulting customer key sets;
2. identifies `{C2}` as removed;
3. finds the two current orders referencing `C2`;
4. emits an `OrphanReport` with `orphanCount = 2` and `actionTaken = warn`;
5. leaves `orders` unchanged.

The data owner can then choose a domain-correct repair—for example restoring the parent, closing child records, quarantining them, or running an independently reviewed delete job. That decision cannot be inferred safely from the FK alone.

## Operational requirements

- **Snapshot retention:** retain the previous parent snapshot and its files until the attempt and incident window have closed. An expired snapshot cannot be reconstructed from its numeric ID.
- **Writer coordination:** the hosting platform must prevent incompatible writers from changing inspected targets during the qualified execution window. Iceberg optimistic concurrency is not a business-ordering mechanism.
- **Scale testing:** the detector projects key columns only, but distinct operations, anti-joins, and child matching can still scan and shuffle substantial data. Benchmark representative cardinalities.
- **First execution:** no previous snapshot means no comparison baseline; the check is skipped and this fact must not be interpreted as a proof that no legacy orphans exist.
- **Failed attempt:** flow commits already observed remain committed. Follow the normal partial-result recovery procedure before another run.

## Related

- [Flow Configuration — foreignKeys](../configuration/flows.md#foreign-key-fields)
- [Validation Engine — Foreign key integrity](validation.md#foreign-key-integrity)
- [Iceberg Integration — Post-attempt lifecycle](iceberg.md#post-attempt-lifecycle)
- [SCD2 Guide](scd2.md)
- [Failure Handling and Production Operations](recovery.md)
