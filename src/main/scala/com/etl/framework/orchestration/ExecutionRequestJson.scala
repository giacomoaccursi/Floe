package com.etl.framework.orchestration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.{CharacterCodingException, CodingErrorAction, StandardCharsets}
import java.time.Instant
import scala.util.control.NonFatal

/** Strict, versioned JSON wire format for platform-owned execution requests. */
object ExecutionRequestJson {

  val MaxRequestBytes: Int = 1024 * 1024

  private val RootFields = Set(
    "contract_version",
    "pipeline_id",
    "logical_run_id",
    "attempt_id",
    "effective_at",
    "code_version",
    "config_digest",
    "data_interval",
    "input_references",
    "retry_profile",
    "platform_references"
  )

  def read(pathValue: String, configuration: Configuration = new Configuration()): Either[String, ExecutionRequest] =
    try {
      val path = new Path(pathValue)
      val filesystem = path.getFileSystem(configuration)
      val status = filesystem.getFileStatus(path)
      if (!status.isFile) Left(s"Execution request path is not a file: $pathValue")
      else if (status.getLen > MaxRequestBytes)
        Left(s"Execution request exceeds the $MaxRequestBytes-byte limit: $pathValue")
      else {
        val input = filesystem.open(path)
        try parseJson(readLimited(input, MaxRequestBytes))
        finally input.close()
      }
    } catch {
      case _: CharacterCodingException => Left(s"Execution request is not valid UTF-8: $pathValue")
      case NonFatal(error)             => Left(s"Failed to read execution request '$pathValue': ${message(error)}")
    }

  def parseJson(json: String): Either[String, ExecutionRequest] =
    try {
      parse(json) match {
        case root: JObject => parseRoot(root)
        case _             => Left("Execution request JSON must be an object")
      }
    } catch {
      case NonFatal(error) => Left(s"Invalid execution request JSON: ${message(error)}")
    }

  def toJson(request: ExecutionRequest): String = {
    val interval = request.dataInterval match {
      case Some(value) =>
        JObject(
          "start_inclusive" -> JString(value.startInclusive.toString),
          "end_exclusive" -> JString(value.endExclusive.toString)
        )
      case None => JNull
    }
    val inputs = request.inputReferences
      .map(reference =>
        JObject(
          "dataset_id" -> JString(reference.datasetId),
          "version" -> JString(reference.version),
          "read_mode" -> JString(reference.readMode)
        )
      )
      .toList
    val platform = request.platformReferences.toList.sortBy(_._1).map { case (key, value) => key -> JString(value) }

    compact(
      render(
        JObject(
          "contract_version" -> JInt(request.contractVersion),
          "pipeline_id" -> JString(request.pipelineId),
          "logical_run_id" -> JString(request.logicalRunId),
          "attempt_id" -> JString(request.attemptId),
          "effective_at" -> JString(request.effectiveAt.toString),
          "code_version" -> JString(request.codeVersion),
          "config_digest" -> JString(request.configDigest),
          "data_interval" -> interval,
          "input_references" -> JArray(inputs),
          "retry_profile" -> JString(request.retryProfile.name),
          "platform_references" -> JObject(platform)
        )
      )
    )
  }

  private def parseRoot(root: JObject): Either[String, ExecutionRequest] =
    for {
      fields <- validatedFields(root, RootFields, "execution request")
      contractVersion <- requiredInt(fields, "contract_version")
      pipelineId <- requiredString(fields, "pipeline_id")
      logicalRunId <- requiredString(fields, "logical_run_id")
      attemptId <- requiredString(fields, "attempt_id")
      effectiveAtText <- requiredString(fields, "effective_at")
      effectiveAt <- parseInstant("effective_at", effectiveAtText)
      codeVersion <- requiredString(fields, "code_version")
      configDigest <- requiredString(fields, "config_digest")
      dataInterval <- parseDataInterval(fields.get("data_interval"))
      inputReferences <- parseInputReferences(fields.get("input_references"))
      retryProfile <- parseRetryProfile(fields.get("retry_profile"))
      platformReferences <- parseStringMap(fields.get("platform_references"), "platform_references")
      request <- construct(
        ExecutionRequest(
          pipelineId = pipelineId,
          logicalRunId = logicalRunId,
          attemptId = attemptId,
          effectiveAt = effectiveAt,
          codeVersion = codeVersion,
          configDigest = configDigest,
          dataInterval = dataInterval,
          inputReferences = inputReferences,
          retryProfile = retryProfile,
          platformReferences = platformReferences,
          contractVersion = contractVersion
        )
      )
    } yield request

  private def parseDataInterval(value: Option[JValue]): Either[String, Option[DataInterval]] = value match {
    case None | Some(JNull) => Right(None)
    case Some(obj: JObject) =>
      for {
        fields <- validatedFields(obj, Set("start_inclusive", "end_exclusive"), "data_interval")
        startText <- requiredString(fields, "start_inclusive")
        endText <- requiredString(fields, "end_exclusive")
        start <- parseInstant("data_interval.start_inclusive", startText)
        end <- parseInstant("data_interval.end_exclusive", endText)
        interval <- construct(DataInterval(start, end))
      } yield Some(interval)
    case Some(_) => Left("Field 'data_interval' must be an object or null")
  }

  private def parseInputReferences(value: Option[JValue]): Either[String, Seq[InputReference]] = value match {
    case None => Right(Seq.empty)
    case Some(JArray(items)) =>
      sequence(items.zipWithIndex.map { case (item, index) => parseInputReference(item, index) })
    case Some(_) => Left("Field 'input_references' must be an array")
  }

  private def parseInputReference(value: JValue, index: Int): Either[String, InputReference] = value match {
    case obj: JObject =>
      for {
        fields <- validatedFields(obj, Set("dataset_id", "version", "read_mode"), s"input_references[$index]")
        datasetId <- requiredString(fields, "dataset_id")
        version <- requiredString(fields, "version")
        readMode <- requiredString(fields, "read_mode")
        reference <- construct(InputReference(datasetId, version, readMode))
      } yield reference
    case _ => Left(s"input_references[$index] must be an object")
  }

  private def parseRetryProfile(value: Option[JValue]): Either[String, RetryProfile] = value match {
    case Some(JString(RetryProfile.ManualRecovery.name)) => Right(RetryProfile.ManualRecovery)
    case Some(JString(other))                            => Left(s"Unsupported retry_profile '$other'")
    case None                                            => Left("Missing required field 'retry_profile'")
    case _                                               => Left("Field 'retry_profile' must be a string")
  }

  private def parseStringMap(value: Option[JValue], field: String): Either[String, Map[String, String]] = value match {
    case None => Right(Map.empty)
    case Some(obj: JObject) =>
      validatedFields(obj, obj.obj.map(_._1).toSet, field).flatMap { fields =>
        sequence(fields.toSeq.sortBy(_._1).map {
          case (key, JString(item)) => Right(key -> item)
          case (key, _)             => Left(s"Field '$field.$key' must be a string")
        }).map(_.toMap)
      }
    case Some(_) => Left(s"Field '$field' must be an object")
  }

  private def validatedFields(
      value: JObject,
      allowed: Set[String],
      context: String
  ): Either[String, Map[String, JValue]] = {
    val names = value.obj.map(_._1)
    val duplicates =
      names.groupBy(identity).collect { case (name, occurrences) if occurrences.size > 1 => name }.toSeq.sorted
    val unknown = names.toSet.diff(allowed).toSeq.sorted
    if (duplicates.nonEmpty) Left(s"Duplicate fields in $context: ${duplicates.mkString(", ")}")
    else if (unknown.nonEmpty) Left(s"Unknown fields in $context: ${unknown.mkString(", ")}")
    else Right(value.obj.toMap)
  }

  private def requiredString(fields: Map[String, JValue], name: String): Either[String, String] =
    fields.get(name) match {
      case Some(JString(value)) => Right(value)
      case Some(_)              => Left(s"Field '$name' must be a string")
      case None                 => Left(s"Missing required field '$name'")
    }

  private def requiredInt(fields: Map[String, JValue], name: String): Either[String, Int] =
    fields.get(name) match {
      case Some(JInt(value)) if value.isValidInt => Right(value.toInt)
      case Some(_)                               => Left(s"Field '$name' must be a 32-bit integer")
      case None                                  => Left(s"Missing required field '$name'")
    }

  private def parseInstant(field: String, value: String): Either[String, Instant] =
    try Right(Instant.parse(value))
    catch { case NonFatal(_) => Left(s"Field '$field' must be an ISO-8601 UTC instant") }

  private def construct[A](value: => A): Either[String, A] =
    try Right(value)
    catch { case NonFatal(error) => Left(message(error)) }

  private def sequence[A](values: Seq[Either[String, A]]): Either[String, Seq[A]] =
    values.foldLeft[Either[String, Vector[A]]](Right(Vector.empty)) { (acc, current) =>
      for {
        collected <- acc
        value <- current
      } yield collected :+ value
    }

  private def readLimited(input: java.io.InputStream, maximum: Int): String = {
    val output = new ByteArrayOutputStream(math.min(maximum, 8192))
    val buffer = new Array[Byte](8192)
    var total = 0
    var read = input.read(buffer)
    while (read >= 0) {
      total += read
      if (total > maximum) throw new IllegalArgumentException(s"Execution request exceeds the $maximum-byte limit")
      output.write(buffer, 0, read)
      read = input.read(buffer)
    }
    StandardCharsets.UTF_8
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
      .decode(ByteBuffer.wrap(output.toByteArray))
      .toString
  }

  private def message(error: Throwable): String = Option(error.getMessage).getOrElse(error.getClass.getName)
}
