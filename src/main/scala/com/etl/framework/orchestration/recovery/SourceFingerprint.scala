package com.etl.framework.orchestration.recovery

import com.etl.framework.config.{FlowConfig, SourceType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

object SourceFingerprint {
  def compute(flow: FlowConfig, hadoopConfiguration: Configuration): Option[String] =
    flow.source.options.get("replayToken").filter(_.nonEmpty).map(token => sha256(s"token:$token")).orElse {
      flow.source.`type` match {
        case SourceType.File => fileFingerprint(flow, hadoopConfiguration)
        case _               => None
      }
    }

  private def fileFingerprint(flow: FlowConfig, configuration: Configuration): Option[String] = {
    val root = new Path(flow.source.path)
    val fs = root.getFileSystem(configuration)
    val roots = Option(fs.globStatus(root)).map(_.toSeq).getOrElse(Seq.empty)
    if (roots.isEmpty) return None

    def files(status: FileStatus): Seq[FileStatus] =
      if (status.isFile) Seq(status)
      else fs.listStatus(status.getPath).toSeq.flatMap(files)

    val inventory = roots
      .flatMap(files)
      .sortBy(_.getPath.toString)
      .map(status => s"${status.getPath}|${status.getLen}|${status.getModificationTime}")
      .mkString("\n")
    val options = flow.source.options.toSeq.sortBy(_._1).mkString("|")
    Some(sha256(s"${flow.source.format.map(_.name).getOrElse("")}|$options\n$inventory"))
  }

  private def sha256(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"$byte%02x")
      .mkString
}
