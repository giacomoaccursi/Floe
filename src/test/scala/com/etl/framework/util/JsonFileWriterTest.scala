package com.etl.framework.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class JsonFileWriterTest extends AnyFlatSpec with Matchers {

  "JsonFileWriter" should "write a file URI through the configured filesystem" in {
    val directory = Files.createTempDirectory("json-file-writer-test")
    val target = directory.resolve("nested").resolve("metadata.json")

    JsonFileWriter.write(Map("batch_id" -> "test"), target.toUri.toString)

    Files.exists(target) shouldBe true
    new String(Files.readAllBytes(target), StandardCharsets.UTF_8) should include("\"batch_id\":\"test\"")
  }
}
