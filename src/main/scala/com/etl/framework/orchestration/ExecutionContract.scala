package com.etl.framework.orchestration

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig, IcebergConfig, PathsConfig}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.util.UUID

sealed trait DataOutcome { def name: String }
object DataOutcome {
  case object NotAttempted extends DataOutcome { val name = "NOT_ATTEMPTED" }
  case object NotCommitted extends DataOutcome { val name = "NOT_COMMITTED" }
  case object NoChange extends DataOutcome { val name = "NO_CHANGE" }
  case object Committed extends DataOutcome { val name = "COMMITTED" }
  case object Unknown extends DataOutcome { val name = "UNKNOWN" }
}

sealed trait ExecutionStatus { def name: String; def terminal: Boolean }
object ExecutionStatus {
  case object Succeeded extends ExecutionStatus { val name = "SUCCEEDED"; val terminal = true }
  case object SucceededWithWarnings extends ExecutionStatus {
    val name = "SUCCEEDED_WITH_WARNINGS"
    val terminal = true
  }
  case object Failed extends ExecutionStatus { val name = "FAILED"; val terminal = true }
  case object FailedPartial extends ExecutionStatus { val name = "FAILED_PARTIAL"; val terminal = true }
  case object Unknown extends ExecutionStatus { val name = "UNKNOWN"; val terminal = false }
}

sealed trait RetryProfile { def name: String }
object RetryProfile {

  /** Floe never retries the whole job. The platform must inspect a failed/unknown attempt before resubmission. */
  case object ManualRecovery extends RetryProfile { val name = "manual-recovery" }
}

final case class DataInterval(startInclusive: Instant, endExclusive: Instant) {
  require(startInclusive != null, "dataInterval.startInclusive is required")
  require(endExclusive != null, "dataInterval.endExclusive is required")
  require(startInclusive.isBefore(endExclusive), "dataInterval must be a non-empty [start, end) interval")
}

final case class InputReference(datasetId: String, version: String, readMode: String) {
  ExecutionRequest.validateIdentifier("inputReference.datasetId", datasetId)
  require(version != null && version.trim.nonEmpty, "inputReference.version must not be blank")
  require(readMode != null && readMode.trim.nonEmpty, "inputReference.readMode must not be blank")
}

final case class PipelineDefinition(pipelineId: String, codeVersion: String, configDigest: String) {
  ExecutionRequest.validateIdentifier("pipelineId", pipelineId)
  require(codeVersion != null && codeVersion.trim.nonEmpty, "codeVersion must not be blank")
  require(PipelineDefinition.isSha256(configDigest), "configDigest must be a lowercase SHA-256 digest")

  def newRequest(
      logicalRunId: String,
      effectiveAt: Instant,
      dataInterval: Option[DataInterval] = None,
      inputReferences: Seq[InputReference] = Seq.empty,
      platformReferences: Map[String, String] = Map.empty
  ): ExecutionRequest =
    ExecutionRequest(
      pipelineId = pipelineId,
      logicalRunId = logicalRunId,
      attemptId = UUID.randomUUID().toString,
      effectiveAt = effectiveAt,
      codeVersion = codeVersion,
      configDigest = configDigest,
      dataInterval = dataInterval,
      inputReferences = inputReferences,
      platformReferences = platformReferences
    )
}

object PipelineDefinition {
  private val Sha256 = "[0-9a-f]{64}".r
  private[orchestration] def isSha256(value: String): Boolean =
    value != null && Sha256.pattern.matcher(value).matches()
}

/** Immutable request owned by the calling platform. `attemptId` identifies this driver invocation, while `logicalRunId`
  * remains stable across an operator-approved retry of the same frozen request.
  */
final case class ExecutionRequest(
    pipelineId: String,
    logicalRunId: String,
    attemptId: String,
    effectiveAt: Instant,
    codeVersion: String,
    configDigest: String,
    dataInterval: Option[DataInterval] = None,
    inputReferences: Seq[InputReference] = Seq.empty,
    retryProfile: RetryProfile = RetryProfile.ManualRecovery,
    platformReferences: Map[String, String] = Map.empty,
    contractVersion: Int = ExecutionRequest.CurrentContractVersion
) {
  ExecutionRequest.validateIdentifier("pipelineId", pipelineId)
  ExecutionRequest.validateIdentifier("logicalRunId", logicalRunId)
  ExecutionRequest.validateIdentifier("attemptId", attemptId)
  require(effectiveAt != null, "effectiveAt is required")
  require(codeVersion != null && codeVersion.trim.nonEmpty, "codeVersion must not be blank")
  require(PipelineDefinition.isSha256(configDigest), "configDigest must be a lowercase SHA-256 digest")
  require(inputReferences.map(_.datasetId).distinct.size == inputReferences.size, "input references must be unique")
  platformReferences.foreach { case (key, value) =>
    ExecutionRequest.validateIdentifier("platform reference key", key)
    require(value != null && value.nonEmpty && value.length <= 512, s"Invalid platform reference '$key'")
  }
  require(
    contractVersion == ExecutionRequest.CurrentContractVersion,
    s"Unsupported execution contract version $contractVersion"
  )

  def artifactKey: String = s"$pipelineId/$logicalRunId/$attemptId"
}

object ExecutionRequest {
  val CurrentContractVersion: Int = 1
  private val Identifier = "[A-Za-z0-9][A-Za-z0-9._:-]{0,127}".r

  def validateIdentifier(field: String, value: String): Unit = {
    require(value != null && Identifier.pattern.matcher(value).matches(), s"Invalid $field")
    require(value != "." && value != "..", s"Invalid $field")
  }
}

/** Computes a deterministic digest without serializing executable functions or secret values. The code version is a
  * separate mandatory identity for transformations, custom validators/readers and derived-table functions.
  */
object PipelineDefinitionBuilder {
  private val SecretKey = "(?i).*(password|passwd|secret|token|credential|access[._-]?key|private[._-]?key).*".r

  def build(
      pipelineId: String,
      codeVersion: String,
      globalConfig: GlobalConfig,
      flowConfigs: Seq[FlowConfig],
      domainsConfig: Option[DomainsConfig],
      derivedTableDefinitions: Seq[(String, Seq[String])],
      sparkSemanticConfig: Map[String, String]
  ): PipelineDefinition = {
    val safeGlobal = globalConfig.copy(
      paths = PathsConfig(
        sanitizeText(globalConfig.paths.outputPath),
        sanitizeText(globalConfig.paths.rejectedPath),
        sanitizeText(globalConfig.paths.metadataPath),
        globalConfig.paths.warningsPath.map(sanitizeText)
      ),
      iceberg = sanitizedIceberg(globalConfig.iceberg)
    )
    val safeFlows = flowConfigs
      .map(flow =>
        flow.copy(
          source = flow.source.copy(path = sanitizeText(flow.source.path), options = sanitizeMap(flow.source.options)),
          output = flow.output.copy(
            rejectedPath = flow.output.rejectedPath.map(sanitizeText),
            tableProperties = sanitizeMap(flow.output.tableProperties)
          ),
          preValidationTransformation = None,
          postValidationTransformation = None
        )
      )
      .sortBy(_.name)
    val identity = Seq(
      safeGlobal,
      safeFlows,
      domainsConfig.map(config => config.copy(domains = config.domains.toSeq.sortBy(_._1).toMap)),
      derivedTableDefinitions.map { case (name, dependencies) => name -> dependencies.sorted }.sortBy(_._1),
      sanitizeMap(sparkSemanticConfig)
    )
    PipelineDefinition(pipelineId, codeVersion.trim, sha256(CanonicalValue.render(identity)))
  }

  private def sanitizedIceberg(config: IcebergConfig): IcebergConfig =
    config.copy(
      warehouse = sanitizeText(config.warehouse),
      catalogProperties = sanitizeMap(config.catalogProperties),
      maintenance = config.maintenance.copy()
    )

  private def sanitizeMap(values: Map[String, String]): Map[String, String] =
    values.map { case (key, value) =>
      key -> (if (SecretKey.pattern.matcher(key).matches()) "<secret-reference>" else sanitizeText(value))
    }

  private def sanitizeText(value: String): String =
    Option(value)
      .getOrElse("")
      .replaceAll("(?i)(password|passwd|secret|token|credential|access[._-]?key)=([^&;\\s]+)", "$1=<secret-reference>")
      .replaceAll("(?i)(://[^/@:]+):[^/@]+@", "$1:<secret-reference>@")

  private def sha256(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
}

private object CanonicalValue {
  def render(value: Any): String = value match {
    case null        => "null"
    case None        => "option:none"
    case Some(inner) => frame("option:some", Seq(render(inner)))
    case map: Map[_, _] =>
      frame(
        "map",
        map.toSeq.map { case (k, v) => render(k) -> render(v) }.sortBy(_._1).map { case (key, item) =>
          frame("entry", Seq(key, item))
        }
      )
    case set: Set[_]      => frame("set", set.toSeq.map(render).sorted)
    case seq: Seq[_]      => frame("seq", seq.map(render))
    case product: Product => frame(s"product:${product.productPrefix}", product.productIterator.map(render).toSeq)
    case string: String   => frame("string", Seq(string))
    case boolean: Boolean => s"boolean:$boolean"
    case number: Byte     => s"byte:$number"
    case number: Short    => s"short:$number"
    case number: Int      => s"int:$number"
    case number: Long     => s"long:$number"
    case number: Float    => s"float:${java.lang.Float.toHexString(number)}"
    case number: Double   => s"double:${java.lang.Double.toHexString(number)}"
    case instant: Instant => s"instant:${instant.toString}"
    case other =>
      throw new IllegalArgumentException(s"Unsupported value in canonical pipeline definition: ${other.getClass}")
  }

  private def frame(kind: String, values: Seq[String]): String =
    kind + values.map(value => s"${value.getBytes(StandardCharsets.UTF_8).length}:$value").mkString("[", "", "]")
}
