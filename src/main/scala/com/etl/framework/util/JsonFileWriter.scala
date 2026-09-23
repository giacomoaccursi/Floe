package com.etl.framework.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{write => jsonWrite}

import java.nio.charset.StandardCharsets

/** Writes a Scala map as JSON using the Hadoop filesystem selected by the path URI. */
object JsonFileWriter {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def write(data: Map[String, Any], filePath: String, configuration: Configuration = new Configuration()): Unit = {
    val jsonString = jsonWrite(data)
    val path = new Path(filePath)
    val filesystem = path.getFileSystem(configuration)
    Option(path.getParent).foreach(filesystem.mkdirs)
    val output = filesystem.create(path, true)
    try output.write(jsonString.getBytes(StandardCharsets.UTF_8))
    finally output.close()
  }
}
