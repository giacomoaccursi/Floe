# Airflow reference integration

Status: **dependency installation, DAG import/validation, and contract structure tested on 5 October 2026; not qualified on a live Kubernetes/Airflow deployment**. The isolated check used Python 3.12.13 with the exact pinned Airflow/providers and completed `pip check`, DAG import, and `dag.validate()`. CI repeats the constrained dependency installation and DAG validation so dependency/API drift fails before merge. Do not list this combination as a supported production backend until the live checks below pass in the target environment.

The DAG submits one already-persisted Floe request through `SparkSubmitOperator` in Kubernetes cluster mode and waits for the remote driver. It is pinned to Airflow 3.3.2, Spark provider 6.3.2, and the published Kubernetes provider 9.0.1. Airflow 3.3+ is required for the operator's durable task-state store; `track_driver_via_k8s_api=True` is required for Kubernetes reconnection.

## Deployment prerequisites

- Build an immutable driver image containing `/opt/floe/floe-application.jar`, one compatible Spark/Iceberg runtime, and all catalog dependencies. The `local:///` URI means the JAR must exist in the driver image.
- Configure the `spark_k8s` Spark Submit connection for Kubernetes cluster mode. The Airflow worker needs permission to create the driver and executor resources and to read/delete driver pods.
- Create the Airflow pool `floe_orders_writers` with one slot. Every Airflow DAG writing any overlapping target must use the same pool. This pool does not fence jobs submitted outside that Airflow deployment; those require a shared platform lock or a single submission authority.
- Persist the strict Floe `ExecutionRequest` before triggering the DAG. Pass immutable `config_uri` and `request_uri` parameters. Do not pass credentials or large payloads through DAG params or XCom.
- Keep Spark/application retry and supervision disabled unless that exact backend recipe has been qualified. The DAG sets task retries to zero. `durable=True` records the remote driver identity, but this example does not assume that every worker-loss timing can be recovered without operator investigation.

Install Airflow with its official constraints for the selected Python version, then install the pinned providers while keeping Airflow pinned:

```bash
AIRFLOW_VERSION=3.3.2
PYTHON_VERSION=3.12
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

python -m pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
python -m pip install -r integrations/airflow/requirements.txt
python -m pip check
```

## Mandatory live qualification

1. Parse and serialize the DAG with the exact Airflow image and connection configuration.
2. Run a successful request and a Floe result that returns each non-zero process exit category.
3. Kill the Airflow worker after the driver ID is stored; prove that recovery observes the same driver and does not submit another application.
4. Kill the worker at the submission/identity-persistence boundary; establish the incident procedure for the ambiguous case before allowing any resubmission.
5. Timeout and clear a running task; prove whether the driver stopped. Keep the writer pool blocked until Kubernetes shows a terminal driver and Iceberg effects are inspected.
6. Submit two overlapping DAG runs and a non-Airflow writer; prove the chosen external serialization mechanism, not only `max_active_runs`.
7. Retain request URI, Airflow run/task IDs, driver pod name, application logs, Floe JSON result, and Iceberg inspection evidence.

Official references: [SparkSubmitOperator durable execution](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html#durable-execution-crash-recovery), [Spark Submit connection](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/connections/spark-submit.html), and [Airflow constrained installation](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html).
