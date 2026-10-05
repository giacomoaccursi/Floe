# Step Functions / EMR Serverless reference integration

Status: **ASL structure tested, not deployed or qualified in an AWS account**. It is a draft integration, not a supported production backend.

Deploy `emr-serverless.asl.json` as a **Standard Workflow**. The task uses the optimized `emr-serverless:startJobRun.sync` integration, so Step Functions waits for the remote EMR Serverless job instead of treating submission as success. There is deliberately no `Retry` block: an error or timeout enters a terminal failure that requires reconciliation.

The state-machine input must contain only non-secret control references:

```json
{
  "application_id": "00example",
  "execution_role_arn": "arn:aws:iam::123456789012:role/floe-emr-serverless",
  "application_jar_uri": "s3://immutable-artifacts/floe/app-sha256.jar",
  "config_uri": "s3://immutable-config/orders/v42",
  "request_uri": "s3://control/orders/requests/attempt-id.json",
  "attempt_id": "d59882b1-b84a-4ae2-92fe-2bfe5ec78e41",
  "log_uri": "s3://restricted-logs/floe/"
}
```

The request must already exist and its `attempt_id` must match the input. The state machine does not manufacture or mutate Floe identity. Store the execution ARN and returned EMR Serverless job-run identity next to the request.

## Required controls

- The Step Functions role needs `StartJobRun`, `GetJobRun`, `CancelJobRun`, `iam:PassRole`, and the EventBridge permissions required by `.sync`. Test polling as well as event completion; missing polling permission can leave a workflow stuck.
- The EMR execution role needs least-privilege access to the immutable JAR/config/request, source data, catalog, warehouse, diagnostic paths, and log destination.
- Serialize every writer for the target set before starting the execution. Step Functions does not provide a cross-execution or cross-platform table lock by itself. Keep that lock after timeout/cancellation until the EMR job is terminal and Iceberg evidence is reconciled.
- A stopped `.sync` task triggers only a best-effort cancellation attempt. Never infer that the Spark driver or a pending catalog commit stopped from the workflow state alone.
- `TimeoutSeconds` is an operational bound, not a safe retry signal. Adjust it to the qualified job duration without weakening the post-timeout freeze procedure.

## Mandatory live qualification

Validate the definition with AWS, then test success, Floe non-zero exit, IAM denial, EMR job failure, Step Functions timeout, stopped execution, lost EventBridge notification with polling, and two overlapping submissions under the real serialization control. Record the exact EMR release, Spark/Iceberg artifacts, region, catalog, IAM policies, and state-machine revision. Until those tests pass, keep this integration out of the supported-backend matrix.

Official references: [EMR Serverless optimized integration](https://docs.aws.amazon.com/step-functions/latest/dg/connect-emr-serverless.html) and [permissions/cancellation for `.sync`](https://docs.aws.amazon.com/step-functions/latest/dg/service-integration-iam-templates.html).

