package com.etl.framework.integration

import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class OrchestratorExamplesTest extends AnyFlatSpec with Matchers {

  implicit private val formats: Formats = DefaultFormats

  "The Airflow reference DAG" should "preserve the manual-recovery and remote-driver contract" in {
    val dag = read("integrations/airflow/floe_orders_dag.py")
    val requirements = read("integrations/airflow/requirements.txt")

    dag should include("max_active_runs=1")
    dag should include("default_args={\"retries\": 0}")
    dag should include("deploy_mode=\"cluster\"")
    dag should include("track_driver_via_k8s_api=True")
    dag should include("durable=True")
    dag should include("retries=0")
    dag should include("pool=\"floe_orders_writers\"")
    dag should include("\"--request\"")
    dag should include("\"{{ params.request_uri }}\"")
    dag should not include "xcom_push"

    requirements.split("\\r?\\n").iterator.filter(_.nonEmpty).toSet shouldBe Set(
      "apache-airflow==3.3.2",
      "apache-airflow-providers-apache-spark==6.3.2",
      "apache-airflow-providers-cncf-kubernetes==9.0.1"
    )
  }

  "The Step Functions reference state machine" should "wait for EMR Serverless without blind retries" in {
    val raw = read("integrations/aws-step-functions/emr-serverless.asl.json")
    val definition = parse(raw)
    val task = definition \ "States" \ "RunFloe"

    (definition \ "QueryLanguage").extract[String] shouldBe "JSONPath"
    (task \ "Resource").extract[String] shouldBe "arn:aws:states:::emr-serverless:startJobRun.sync"
    (task \ "TimeoutSeconds").extract[Int] shouldBe 14400
    (task \ "Parameters" \ "JobDriver" \ "SparkSubmit" \ "EntryPointArguments.$")
      .extract[String] should include("$.request_uri")
    (task \ "Parameters" \ "JobDriver" \ "SparkSubmit" \ "SparkSubmitParameters")
      .extract[String] should include("com.etl.framework.runner.FloeRunner")
    (task \ "Catch") should not be JNothing
    raw should not include "\"Retry\""
  }

  private def read(path: String): String =
    new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
}
