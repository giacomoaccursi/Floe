package com.etl.framework.util

import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{write => jsonWrite}

import java.nio.file.{Files, Paths, StandardOpenOption}

object JsonFileWriter {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def write(data: Map[String, Any], filePath: String): Unit = {
    val jsonString = jsonWrite(data)
    val path = Paths.get(filePath)
    Files.createDirectories(path.getParent)
    Files.write(path, jsonString.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }
}
