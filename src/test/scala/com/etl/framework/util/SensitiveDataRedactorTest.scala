package com.etl.framework.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SensitiveDataRedactorTest extends AnyFlatSpec with Matchers {

  "SensitiveDataRedactor" should "redact common credential representations without destroying context" in {
    val cases = Seq(
      "jdbc:postgresql://alice:s3cr3t@db.internal/orders" ->
        "jdbc:postgresql://alice:[REDACTED]@db.internal/orders",
      "password=s3cr3t host=db.internal" -> "password=[REDACTED] host=db.internal",
      "\"client_secret\": \"s3cr3t value\"" -> "\"client_secret\": \"[REDACTED]\"",
      "Authorization: Bearer abc.def-123" -> "Authorization: Bearer [REDACTED]",
      "request?api_key=s3cr3t&region=eu" -> "request?api_key=[REDACTED]&region=eu",
      "s3://bucket/object?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=deadbeef" ->
        "s3://bucket/object?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=[REDACTED]",
      "Cookie: session=abc; theme=dark" -> "Cookie: [REDACTED]",
      "spark.hadoop.fs.s3a.secret.key=s3cr3t" -> "spark.hadoop.fs.s3a.secret.key=[REDACTED]",
      "AWS key AKIA1234567890ABCDEF failed" -> "AWS key [REDACTED] failed",
      "GitHub token ghp_1234567890abcdefghij failed" -> "GitHub token [REDACTED] failed",
      "JWT eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.signature failed" -> "JWT [REDACTED] failed"
    )

    cases.foreach { case (input, expected) => SensitiveDataRedactor.redact(input) shouldBe expected }
    SensitiveDataRedactor.redact("secret_ref=vault/orders credential_version=42") shouldBe
      "secret_ref=vault/orders credential_version=42"
    SensitiveDataRedactor.redactCountKeys(Map("token=one" -> 2L, "token=two" -> 3L)) shouldBe
      Map("token=[REDACTED]" -> 5L)
  }

  it should "redact private keys and bound diagnostic size" in {
    val privateKey =
      "-----BEGIN PRIVATE KEY-----\nsecret-material\n-----END PRIVATE KEY-----"
    SensitiveDataRedactor.redact(s"invalid $privateKey value") shouldBe "invalid [REDACTED] value"

    val bounded = SensitiveDataRedactor.redact("x" * 10000)
    bounded.length should be < 10000
    bounded should include("[truncated 1808 chars]")
  }

  it should "create a logger-safe throwable without retaining secret-bearing causes" in {
    val cause = new IllegalArgumentException("password=s3cr3t")
    val error = new IllegalStateException("Bearer abc.def", cause)
    val frame = new StackTraceElement("example.Job", "run", "Job.scala", 42)
    error.setStackTrace(Array(frame))

    val safe = SensitiveDataRedactor.safeThrowable(error)

    safe.getMessage shouldBe "java.lang.IllegalStateException: Bearer [REDACTED]"
    safe.getStackTrace shouldBe Array(frame)
    safe.getCause shouldBe null
    safe.getSuppressed shouldBe empty
  }

  it should "sanitize failed flow results at their construction boundary" in {
    val result = com.etl.framework.orchestration.flow.FlowResult.failure(
      "orders",
      "attempt-1",
      "connection failed: password=s3cr3t"
    )

    result.error shouldBe Some("connection failed: password=[REDACTED]")
  }
}
