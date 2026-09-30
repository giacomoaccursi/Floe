package com.etl.framework.orchestration

import com.etl.framework.TestFixtures
import com.etl.framework.config._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class ExecutionContractTest extends AnyFlatSpec with Matchers {

  private val effectiveAt = Instant.parse("2026-09-29T08:00:00Z")

  private def definition(
      flows: Seq[FlowConfig] = Seq(TestFixtures.flowConfig("orders")),
      codeVersion: String = "sha256:application-v1",
      sourceOptions: Map[String, String] = Map.empty,
      sparkConfig: Map[String, String] = Map("spark.sql.session.timeZone" -> "UTC"),
      warehouse: String = "s3://warehouse-a"
  ): PipelineDefinition = {
    val configuredFlows = flows.map(flow => flow.copy(source = flow.source.copy(options = sourceOptions)))
    PipelineDefinitionBuilder.build(
      pipelineId = "orders-prod",
      codeVersion = codeVersion,
      globalConfig = TestFixtures.globalConfig(
        iceberg = IcebergConfig(
          warehouse = warehouse,
          catalogProperties = Map("region" -> "eu-west-1")
        )
      ),
      flowConfigs = configuredFlows,
      domainsConfig = None,
      derivedTableNames = Seq("daily_orders"),
      sparkSemanticConfig = sparkConfig
    )
  }

  "ExecutionRequest" should "separate a stable logical run from unique attempt IDs" in {
    val pipeline = definition()
    val first = pipeline.newRequest("scheduled-2026-09-29", effectiveAt)
    val second = pipeline.newRequest("scheduled-2026-09-29", effectiveAt)

    first.logicalRunId shouldBe second.logicalRunId
    first.effectiveAt shouldBe second.effectiveAt
    first.configDigest shouldBe second.configDigest
    first.attemptId should not be second.attemptId
    first.retryProfile shouldBe RetryProfile.ManualRecovery
  }

  it should "reject unsafe identifiers, invalid intervals and duplicate input identities" in {
    val pipeline = definition()

    an[IllegalArgumentException] should be thrownBy pipeline.newRequest("../escape", effectiveAt)
    an[IllegalArgumentException] should be thrownBy DataInterval(effectiveAt, effectiveAt)
    an[IllegalArgumentException] should be thrownBy pipeline.newRequest(
      "run-1",
      effectiveAt,
      inputReferences = Seq(
        InputReference("orders", "snapshot-1", "iceberg-snapshot"),
        InputReference("orders", "snapshot-2", "iceberg-snapshot")
      )
    )
  }

  it should "produce a stable digest for reordered maps and flow declarations" in {
    val a = TestFixtures.flowConfig("a")
    val b = TestFixtures.flowConfig("b")
    val first = definition(Seq(a, b), sourceOptions = Map("header" -> "true", "delimiter" -> ","))
    val second = definition(Seq(b, a), sourceOptions = Map("delimiter" -> ",", "header" -> "true"))

    first.configDigest shouldBe second.configDigest
  }

  it should "change the digest for semantic configuration or Spark behavior" in {
    val base = TestFixtures.flowConfig("orders")
    val changedSchema = base.copy(schema = base.schema.copy(enforceSchema = !base.schema.enforceSchema))

    definition(Seq(base)).configDigest should not be definition(Seq(changedSchema)).configDigest
    definition(sparkConfig = Map("spark.sql.session.timeZone" -> "UTC")).configDigest should not be
      definition(sparkConfig = Map("spark.sql.session.timeZone" -> "Europe/Rome")).configDigest
    definition(warehouse = "s3://warehouse-a").configDigest should not be
      definition(warehouse = "s3://warehouse-b").configDigest
  }

  it should "redact secret values from the digest input" in {
    val first = definition(sourceOptions = Map("user" -> "etl", "password" -> "first-secret"))
    val second = definition(sourceOptions = Map("user" -> "etl", "password" -> "second-secret"))
    val firstUrl = definition(sourceOptions = Map("url" -> "jdbc:postgresql://etl:first-secret@db/prod"))
    val secondUrl = definition(sourceOptions = Map("url" -> "jdbc:postgresql://etl:second-secret@db/prod"))

    first.configDigest shouldBe second.configDigest
    firstUrl.configDigest shouldBe secondUrl.configDigest
  }

  it should "use codeVersion to identify executable transformations" in {
    val transformed = TestFixtures
      .flowConfig("orders")
      .copy(preValidationTransformation = Some(context => context))

    definition(Seq(transformed), codeVersion = "artifact-v1").configDigest shouldBe
      definition(Seq(TestFixtures.flowConfig("orders")), codeVersion = "artifact-v2").configDigest
    definition(codeVersion = "artifact-v1").codeVersion should not be definition(codeVersion =
      "artifact-v2"
    ).codeVersion
  }
}
