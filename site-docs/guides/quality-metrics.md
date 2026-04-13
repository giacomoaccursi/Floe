# Quality Metrics — Iceberg Table

## Overview

The framework can write per-flow quality metrics to a dedicated Iceberg table after each batch. This gives you a queryable history of data quality over time — rejection rates, orphan counts, execution times — without parsing JSON metadata files.

The feature is opt-in. Set `qualityMetricsTable` in the global config to enable it:

```yaml
processing:
  qualityMetricsTable: "quality_metrics"
```

The table is created automatically on first use in the configured Iceberg catalog and namespace.

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
| Format | JSON file per batch | Iceberg table (append) |
| Access | File system, any editor | SQL via Spark |
| Content | Full batch detail (Iceberg snapshots, manifest locations, rejection reasons) | Aggregated per-flow metrics |
| Use case | Debugging, audit trail | Trend analysis, dashboards, alerting |
| Requires Spark | No | Yes |

## Related

- [Global Configuration — processing](../configuration/global.md#processing) — `qualityMetricsTable` setting
- [Validation Engine](validation.md) — how records are validated and rejected
- [Orphan Detection](orphan-detection.md) — how orphan counts are computed
