package com.etl.framework.runner

sealed trait RunnerCommand
object RunnerCommand {
  final case class Validate(configDirectory: String, pipelineId: String, codeVersion: String) extends RunnerCommand
  final case class Run(configDirectory: String, requestPath: String) extends RunnerCommand
  final case class Inspect(configDirectory: String, requestPath: String) extends RunnerCommand
  final case class Maintenance(configDirectory: String, tableName: String) extends RunnerCommand
}

object RunnerArguments {
  val Usage: String =
    "Usage: FloeRunner validate --config <directory> --pipeline-id <id> --code-version <version>\n" +
      "       FloeRunner run --config <directory> --request <request.json>\n" +
      "       FloeRunner inspect --config <directory> --request <request.json>\n" +
      "       FloeRunner maintenance --config <directory> --table <logical-table>"

  def parse(args: Seq[String]): Either[String, RunnerCommand] = args.headOption match {
    case Some("validate") =>
      parseOptions(args.tail, Set("--config", "--pipeline-id", "--code-version")).flatMap { options =>
        for {
          config <- required(options, "--config")
          pipelineId <- required(options, "--pipeline-id")
          codeVersion <- required(options, "--code-version")
        } yield RunnerCommand.Validate(config, pipelineId, codeVersion)
      }
    case Some("run") =>
      parseOptions(args.tail, Set("--config", "--request")).flatMap { options =>
        for {
          config <- required(options, "--config")
          request <- required(options, "--request")
        } yield RunnerCommand.Run(config, request)
      }
    case Some("inspect") =>
      parseOptions(args.tail, Set("--config", "--request")).flatMap { options =>
        for {
          config <- required(options, "--config")
          request <- required(options, "--request")
        } yield RunnerCommand.Inspect(config, request)
      }
    case Some("maintenance") =>
      parseOptions(args.tail, Set("--config", "--table")).flatMap { options =>
        for {
          config <- required(options, "--config")
          table <- required(options, "--table")
        } yield RunnerCommand.Maintenance(config, table)
      }
    case Some(other) => Left(s"Unknown command '$other'\n$Usage")
    case None        => Left(Usage)
  }

  private def parseOptions(args: Seq[String], allowed: Set[String]): Either[String, Map[String, String]] = {
    if (args.size % 2 != 0) Left(s"Every option requires a value\n$Usage")
    else {
      val pairs = args.grouped(2).map(pair => pair.head -> pair(1)).toSeq
      val unknown = pairs.map(_._1).filterNot(allowed).distinct.sorted
      val duplicates = pairs.groupBy(_._1).collect { case (name, values) if values.size > 1 => name }.toSeq.sorted
      if (unknown.nonEmpty) Left(s"Unknown options: ${unknown.mkString(", ")}\n$Usage")
      else if (duplicates.nonEmpty) Left(s"Duplicate options: ${duplicates.mkString(", ")}\n$Usage")
      else if (pairs.exists { case (key, value) => !key.startsWith("--") || value.trim.isEmpty })
        Left(s"Options require non-blank values\n$Usage")
      else Right(pairs.toMap)
    }
  }

  private def required(options: Map[String, String], name: String): Either[String, String] =
    options.get(name).toRight(s"Missing required option $name\n$Usage")
}
