# Quality Metrics — Iceberg Table

## Overview

The framework can write per-flow quality metrics to a dedicated Iceberg table during batch finalization. This gives you a queryable history of data quality over time — rejection rates, orphan counts, execution times — without parsing JSON metadata files.

The feature is opt-in. Set `qualityMetricsTable` in the global config to enable it:

```yaml
processing:
  qualityMetricsTable: "quality_metrics"
```

The target follows the global DDL policy. With the enterprise default `ddlMode: validate`, provision it with the exact schema below before running the pipeline; its absence fails target preflight before any flow executes. With the explicit local/bootstrap mode `ddlMode: automatic`, Floe creates it on first use. Metric appends remain diagnostic and best-effort after the table contract has passed preflight.

## Table schema

| Column | Type | Description |
|--------|------|-------------|
| `batch_id` | STRING | Unique batch identifier |
| `batch_timestamp` | TIMESTAMP | When the batch was executed |
| `batch_success` | BOOLEAN | Whether the overall batch succeeded |
| `flow_name` | STRING | Name of the flow |
| `load_mode` | STRING | Load mode: `full`, `delta`, `scd2` |
| `input_records` | LONG | Total records read from source |
| `valid_records` | LONG | Records that passed validation |
| `rejected_records` | LONG | Records rejected by validation |
| `rejection_rate` | DOUBLE | Ratio of rejected to input records |
| `records_written` | LONG | Records written to Iceberg (from write result metadata, falls back to `valid_records`) |
| `orphan_count` | LONG | Orphaned records found for this flow (sum across all FKs) |
| `execution_time_ms` | LONG | Flow execution time in milliseconds |
| `success` | BOOLEAN | Whether this individual flow succeeded |

Each batch appends one row per flow. Empty batches write a single summary row with `flow_name = "batch_summary"`.

For the default Parquet/format-v2 contract, a platform migration can provision the table as follows (replace the identifier with the configured catalog, namespace, and `qualityMetricsTable` value):

```sql
CREATE TABLE floe.default.quality_metrics (
  batch_id STRING,
  batch_timestamp TIMESTAMP,
  batch_success BOOLEAN,
  flow_name STRING,
  load_mode STRING,
  input_records BIGINT,
  valid_records BIGINT,
  rejected_records BIGINT,
  rejection_rate DOUBLE,
  records_written BIGINT,
  orphan_count BIGINT,
  execution_time_ms BIGINT,
  success BOOLEAN
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '2',
  'write.format.default' = 'parquet'
);
```

If the global Iceberg format settings differ, the migration must use those exact values or validate-only execution will reject the table.

## Example queries

```sql
-- Rejection rate trend for a specific flow
SELECT batch_id, batch_timestamp, rejection_rate
FROM quality_metrics
WHERE flow_name = 'customers'
ORDER BY batch_timestamp

-- Flows with highest rejection rate in the last 7 days
SELECT flow_name, AVG(rejection_rate) AS avg_rejection
FROM quality_metrics
WHERE batch_timestamp > current_timestamp - INTERVAL 7 DAYS
  AND flow_name != 'batch_summary'
GROUP BY flow_name
ORDER BY avg_rejection DESC

-- Orphan detection history
SELECT flow_name, batch_id, orphan_count
FROM quality_metrics
WHERE orphan_count > 0
ORDER BY batch_timestamp DESC

-- Failed flows
SELECT batch_id, flow_name, batch_timestamp
FROM quality_metrics
WHERE success = false

-- Average execution time per flow
SELECT flow_name, AVG(execution_time_ms) AS avg_ms
FROM quality_metrics
WHERE flow_name != 'batch_summary'
GROUP BY flow_name
```

## Relationship with JSON metadata

The quality metrics table does not replace the JSON metadata written to `metadataPath`. They serve different purposes:

| | JSON metadata | Quality metrics table |
|---|---|---|
| Format | Per-flow JSON files plus one batch summary | Iceberg table (append) |
| Access | File system, any editor | SQL via Spark |
| Content | Full batch detail (Iceberg snapshots, manifest locations, rejection reasons) | Aggregated per-flow metrics |
| Use case | Debugging, audit trail | Trend analysis, dashboards, alerting |
| Requires Spark | No | Yes |

Neither output is a workflow coordinator. JSON and quality metrics are best-effort diagnostics and may fail independently; the hosting platform owns job state, while Iceberg snapshot history supplies commit evidence. Maintenance is a separate scheduled job and does not update earlier metric rows.

## Related

- [Global Configuration — processing](../configuration/global.md#processing) — `qualityMetricsTable` setting
- [Validation Engine](validation.md) — how records are validated and rejected
- [Orphan Detection](orphan-detection.md) — how orphan counts are computed
- [Recovery and Production Operations](recovery.md) — authoritative state and maintenance monitoring
