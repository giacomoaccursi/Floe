package com.etl.framework.integration

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

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

  private def read(path: String): String =
    new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
}
