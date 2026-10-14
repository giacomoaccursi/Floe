"""Reference Airflow DAG for one immutable Floe request on Spark Kubernetes.

This file deliberately does not create the ExecutionRequest. The deployment control
plane must persist it before triggering the DAG and pass only its URI.
"""

from datetime import datetime, timedelta, timezone

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, Param


# Deployment-owned identity. Replace CODE_VERSION during image/DAG promotion;
# never copy either value from the submitted execution request.
PIPELINE_ID = "orders-prod"
CODE_VERSION = "sha256:replace-with-immutable-artifact-digest"


with DAG(
    dag_id="floe_orders_kubernetes",
    description="Run one persisted Floe request and wait for its Spark driver",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    params={
        "config_uri": Param(type="string", pattern=r"^s3://[^\s]+$"),
        "request_uri": Param(type="string", pattern=r"^s3://[^\s]+\.json$"),
    },
    tags=["floe", "orders", "manual-recovery"],
) as dag:
    run_floe = SparkSubmitOperator(
        task_id="run_floe",
        application="local:///opt/floe/floe-application.jar",
        java_class="com.etl.framework.runner.FloeRunner",
        conn_id="spark_k8s",
        deploy_mode="cluster",
        name="floe-orders-{{ run_id | replace(':', '_') | replace('+', '_') }}",
        application_args=[
            "run",
            "--config",
            "{{ params.config_uri }}",
            "--request",
            "{{ params.request_uri }}",
            "--pipeline-id",
            PIPELINE_ID,
            "--code-version",
            CODE_VERSION,
        ],
        conf={
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        },
        track_driver_via_k8s_api=True,
        durable=True,
        retries=0,
        pool="floe_orders_writers",
        pool_slots=1,
        execution_timeout=timedelta(hours=4),
        status_poll_interval=10,
    )

