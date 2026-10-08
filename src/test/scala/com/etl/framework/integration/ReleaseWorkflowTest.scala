package com.etl.framework.integration

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

class ReleaseWorkflowTest extends AnyFlatSpec with Matchers {

  "The release workflow" should "publish only from the semantic-release publish phase" in {
    val workflow = read(".github/workflows/release.yml")
    val releaseConfig = read(".releaserc.json")

    workflow should include("@semantic-release/exec")
    workflow should not include "- name: Publish to GitHub Packages"
    releaseConfig should include("\"@semantic-release/exec\"")
    val versionExpression = "$" + "{nextRelease.version}"
    releaseConfig should include("set ThisBuild / version :=")
    releaseConfig should include(versionExpression)
  }

  it should "avoid recursively triggering release CI from its changelog commit" in {
    val releaseConfig = read(".releaserc.json")

    releaseConfig should include(s"chore(release): $${nextRelease.version} [skip ci]")
  }

  it should "use a pinned Node and semantic-release toolchain" in {
    val workflow = read(".github/workflows/release.yml")

    workflow should include("node-version: 24.15.0")
    workflow should include("semantic-release@25.0.9")
    workflow should include("@semantic-release/exec@7.1.0")
  }

  it should "gate publication on the complete reusable quality workflow" in {
    val workflow = read(".github/workflows/release.yml")
    val quality = read(".github/workflows/quality.yml")

    workflow should include("uses: ./.github/workflows/quality.yml")
    workflow should include("needs: quality")
    quality should include("workflow_call:")
    quality should include("sbt -J-Xss4m scalafmtCheckAll")
    quality should include("sbt -J-Xss4m coverage test coverageReport")
    quality should include("mkdocs build --strict")
    quality should include("module.dag.validate()")
    quality should include("json.tool integrations/aws-step-functions/emr-serverless.asl.json")
  }

  it should "pin every remote GitHub Action to an immutable commit" in {
    val workflowDirectory = Paths.get(".github/workflows")
    val stream = Files.list(workflowDirectory)
    val workflowPaths =
      try stream.iterator().asScala.toSeq
      finally stream.close()
    val workflows = workflowPaths
      .filter(path => path.toString.endsWith(".yml") || path.toString.endsWith(".yaml"))
      .flatMap(path => read(path.toString).linesIterator)
      .map(_.trim)
      .filter(line => line.startsWith("uses:") || line.startsWith("- uses:"))
      .map(_.stripPrefix("-").trim.stripPrefix("uses:").trim)
      .filterNot(_.startsWith("./"))

    workflows should not be empty
    workflows.foreach { action =>
      withClue(s"Unpinned action '$action': ") {
        action.split("@", 2).lastOption.getOrElse("").takeWhile(!_.isWhitespace) should fullyMatch regex "[0-9a-f]{40}"
      }
    }
  }

  private def read(path: String): String =
    new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
}
