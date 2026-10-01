package com.etl.framework.orchestration

import org.apache.hadoop.conf.Configuration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant

class ExecutionRequestJsonTest extends AnyFlatSpec with Matchers {

  private val request = ExecutionRequest(
    pipelineId = "orders-prod",
    logicalRunId = "scheduled-2026-10-01",
    attemptId = "attempt-1",
    effectiveAt = Instant.parse("2026-10-01T08:00:00Z"),
    codeVersion = "sha256:image-v1",
    configDigest = "a" * 64,
    dataInterval = Some(
      DataInterval(
        Instant.parse("2026-10-01T00:00:00Z"),
        Instant.parse("2026-10-02T00:00:00Z")
      )
    ),
    inputReferences = Seq(InputReference("orders", "snapshot-123", "iceberg-snapshot")),
    platformReferences = Map("job_id" -> "job-42", "task_id" -> "task-7")
  )

  "ExecutionRequestJson" should "round-trip the complete versioned contract" in {
    ExecutionRequestJson.parseJson(ExecutionRequestJson.toJson(request)) shouldBe Right(request)
  }

  it should "reject unknown, duplicate, and mistyped fields" in {
    val valid = ExecutionRequestJson.toJson(request)
    val unknown = valid.dropRight(1) + ",\"unexpected\":true}"
    val duplicate = valid.replaceFirst(
      "\"pipeline_id\":\"orders-prod\"",
      "\"pipeline_id\":\"a\",\"pipeline_id\":\"b\""
    )
    val mistyped = valid.replace("\"contract_version\":1", "\"contract_version\":\"1\"")

    ExecutionRequestJson.parseJson(unknown).left.get should include("Unknown fields")
    ExecutionRequestJson.parseJson(duplicate).left.get should include("Duplicate fields")
    ExecutionRequestJson.parseJson(mistyped).left.get should include("32-bit integer")
  }

  it should "reject unsupported retry profiles and invalid intervals" in {
    val valid = ExecutionRequestJson.toJson(request)
    val unsupported = valid.replace("manual-recovery", "automatic")
    val invalidInterval = valid.replace("2026-10-02T00:00:00Z", "2026-10-01T00:00:00Z")

    ExecutionRequestJson.parseJson(unsupported).left.get should include("Unsupported retry_profile")
    ExecutionRequestJson.parseJson(invalidInterval).left.get should include("non-empty")
  }

  it should "read through Hadoop FileSystem and enforce a bounded request size" in {
    val file = Files.createTempFile("floe-request", ".json")
    val oversized = Files.createTempFile("floe-request-oversized", ".json")
    val malformed = Files.createTempFile("floe-request-malformed", ".json")
    try {
      Files.write(file, ExecutionRequestJson.toJson(request).getBytes(StandardCharsets.UTF_8)) shouldBe file
      Files.write(oversized, new Array[Byte](ExecutionRequestJson.MaxRequestBytes + 1)) shouldBe oversized
      Files.write(malformed, Array[Byte](0xc3.toByte, 0x28.toByte)) shouldBe malformed

      ExecutionRequestJson.read(file.toString, new Configuration()) shouldBe Right(request)
      ExecutionRequestJson.read(oversized.toString, new Configuration()).left.get should include("exceeds")
      ExecutionRequestJson.read(malformed.toString, new Configuration()).left.get should include("valid UTF-8")
    } finally {
      Files.deleteIfExists(file) shouldBe true
      Files.deleteIfExists(oversized) shouldBe true
      Files.deleteIfExists(malformed) shouldBe true
      ()
    }
  }
}
