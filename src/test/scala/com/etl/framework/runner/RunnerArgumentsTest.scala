package com.etl.framework.runner

import com.etl.framework.orchestration.ExecutionStatus
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RunnerArgumentsTest extends AnyFlatSpec with Matchers {

  "RunnerArguments" should "parse validate and run commands" in {
    RunnerArguments.parse(
      Seq("validate", "--config", "config", "--pipeline-id", "orders", "--code-version", "v1")
    ) shouldBe Right(RunnerCommand.Validate("config", "orders", "v1"))
    RunnerArguments.parse(Seq("run", "--request", "request.json", "--config", "config")) shouldBe
      Right(RunnerCommand.Run("config", "request.json"))
    RunnerArguments.parse(Seq("inspect", "--request", "request.json", "--config", "config")) shouldBe
      Right(RunnerCommand.Inspect("config", "request.json"))
  }

  it should "reject missing, duplicate, and unknown options" in {
    RunnerArguments.parse(Seq("run", "--config", "config")).left.get should include("--request")
    RunnerArguments
      .parse(Seq("run", "--config", "a", "--config", "b", "--request", "request.json"))
      .left
      .get should include("Duplicate")
    RunnerArguments
      .parse(Seq("run", "--config", "a", "--request", "b", "--retry", "true"))
      .left
      .get should include("Unknown")
  }

  "FloeRunner exit mapping" should "preserve partial and unknown outcomes" in {
    FloeRunner.ExitCode.forStatus(ExecutionStatus.Succeeded) shouldBe 0
    FloeRunner.ExitCode.forStatus(ExecutionStatus.SucceededWithWarnings) shouldBe 0
    FloeRunner.ExitCode.forStatus(ExecutionStatus.Failed) shouldBe 10
    FloeRunner.ExitCode.forStatus(ExecutionStatus.FailedPartial) shouldBe 11
    FloeRunner.ExitCode.forStatus(ExecutionStatus.Unknown) shouldBe 12
  }
}
