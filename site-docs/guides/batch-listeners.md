# Batch Listeners — Notifications and Hooks

## Overview

Batch listeners let you react to pipeline completion or failure. The framework calls your listener with the full `IngestionResult` — you decide what to do with it: send a Slack message, publish to SNS, write a metric, log to an external system.

The framework does not bundle any notification provider. You implement the `BatchListener` trait with your own dependencies and register it in the pipeline builder.

## The trait

```scala
trait BatchListener {
  def onBatchCompleted(result: IngestionResult): Unit
  def onBatchFailed(result: IngestionResult): Unit
}
```

- `onBatchCompleted` is called when synchronous publication succeeds (rejected records are allowed unless a threshold is exceeded)
- `onBatchFailed` is called when synchronous publication fails—for example a flow error, rejection-threshold breach, orphan-handling failure, or derived-table failure

Both receive the attempt's `IngestionResult` with request identity, typed status, and flow/derived results. Listeners are best-effort notifications, not transactional publication or workflow state.

If a listener throws an exception, it is caught and logged as a warning. The batch result is not affected — other listeners still run.

## Registration

```scala
IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withBatchListener(mySlackNotifier)
  .withBatchListener(myMetricsPublisher)
  .build()
  .execute()
```

Multiple listeners can be registered. They are called in registration order.

## Examples

### Slack webhook

```scala
class SlackNotifier(webhookUrl: String) extends BatchListener {
  override def onBatchCompleted(result: IngestionResult): Unit = {
    val rejected = result.flowResults.map(_.rejectedRecords).sum
    if (rejected > 0)
      post(s"Batch ${result.batchId}: ${result.flowResults.size} flows, $rejected rejected records")
  }

  override def onBatchFailed(result: IngestionResult): Unit =
    post(s"ALERT: Batch ${result.batchId} failed — ${result.error.getOrElse("unknown error")}")

  private def post(message: String): Unit = {
    // Use your preferred HTTP client (java.net, sttp, okhttp, etc.)
  }
}
```

### AWS SNS

```scala
class SNSNotifier(topicArn: String) extends BatchListener {
  private val client = software.amazon.awssdk.services.sns.SnsClient.builder().build()

  override def onBatchCompleted(result: IngestionResult): Unit = { /* optional */ }

  override def onBatchFailed(result: IngestionResult): Unit = {
    client.publish(
      software.amazon.awssdk.services.sns.model.PublishRequest.builder()
        .topicArn(topicArn)
        .subject(s"ETL batch failed: ${result.batchId}")
        .message(result.error.getOrElse("Unknown error"))
        .build()
    )
  }
}
```

### Simple logging

```scala
class LoggingListener extends BatchListener {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  override def onBatchCompleted(result: IngestionResult): Unit =
    logger.info(s"Batch ${result.batchId} completed: ${result.flowResults.size} flows")

  override def onBatchFailed(result: IngestionResult): Unit =
    logger.error(s"Batch ${result.batchId} failed: ${result.error.getOrElse("unknown")}")
}
```

## What's in IngestionResult

| Field | Type | Description |
|-------|------|-------------|
| `batchId` | `String` | Unique batch identifier |
| `flowResults` | `Seq[FlowResult]` | Per-flow results (records, rejections, timing, Iceberg metadata) |
| `success` | `Boolean` | Whether the batch completed successfully |
| `error` | `Option[String]` | Error message if the batch failed |
| `derivedTableResults` | `Seq[DerivedTableResult]` | Results of derived table execution |
| `orphanReports` | `Seq[OrphanReport]` | Completed post-write FK/orphan checks |
| `status` | `ExecutionStatus` | Observed attempt state (`SUCCEEDED`, `FAILED_PARTIAL`, `UNKNOWN`, and others) |
| `warnings` | `Seq[String]` | Best-effort diagnostic failures that did not erase known data outcomes |

Each `FlowResult` also exposes optional `icebergMetadata`, `resultingSnapshotId`, operational `warnings`, and a typed `dataOutcome`. No single per-flow field authorizes a whole-job retry; earlier targets may already have committed.

## Related

- [Pipeline Builder](pipeline-builder.md) — full builder API reference
- [Architecture: Execution Model](../architecture/execution-model.md) — batch lifecycle
- [Failure Handling and Production Operations](recovery.md) — partial commits, unknown outcomes, and incident handling
