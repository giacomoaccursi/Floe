package com.etl.framework.util

import java.util.regex.Pattern

/** Last-line-of-defence redaction for diagnostic text.
  *
  * This is intentionally applied at observability boundaries (logs, reports and runner output). It recognizes common
  * credential syntaxes; it is not a substitute for passing secret references instead of secret values.
  */
object SensitiveDataRedactor {

  val Replacement = "[REDACTED]"
  private val MaxDiagnosticLength = 8192

  private final case class Rule(pattern: Pattern, replacement: String)

  private val sensitiveKey =
    "(?:[a-z0-9_.-]*(?:password|passwd|pwd|secret(?:[._-]?(?:access[._-]?)?key)?|token|" +
      "api[._-]?key|client[._-]?secret|credential|access[._-]?key(?:[._-]?id)?))"
  private val sensitiveQueryKey =
    "(?:" + sensitiveKey + "|(?:x-amz|x-goog)[._-]?signature|signature|sig)"

  private val rules = Seq(
    Rule(
      Pattern.compile("(?is)-----BEGIN(?: [A-Z0-9]+)? PRIVATE KEY-----.*?-----END(?: [A-Z0-9]+)? PRIVATE KEY-----"),
      Replacement
    ),
    Rule(
      Pattern.compile("(?i)(\\bauthorization\\s*[:=]\\s*(?:bearer|basic)\\s+)([^\\s,;]+)"),
      "$1" + Replacement
    ),
    Rule(
      Pattern.compile("(?i)(\\bauthorization\\s*[:=]\\s*)(?!(?:bearer|basic)\\b)([^\\s,;]+)"),
      "$1" + Replacement
    ),
    Rule(Pattern.compile("(?i)(\\b(?:set-cookie|cookie)\\s*:\\s*)([^\\r\\n]+)"), "$1" + Replacement),
    Rule(Pattern.compile("(?i)(\\bbearer\\s+)([A-Za-z0-9._~+\\-/]+=*)"), "$1" + Replacement),
    Rule(
      Pattern.compile("(?i)(\\b[a-z][a-z0-9+.-]*://[^\\s/:@]+:)([^\\s/@]+)(@)"),
      "$1" + Replacement + "$3"
    ),
    Rule(
      Pattern.compile("(?i)([?&]" + sensitiveQueryKey + "=)([^&#\\s]+)"),
      "$1" + Replacement
    ),
    Rule(
      Pattern.compile("(?i)([\\\"']?\\b" + sensitiveKey + "\\b[\\\"']?\\s*[:=]\\s*[\\\"'])(.*?)([\\\"'])"),
      "$1" + Replacement + "$3"
    ),
    Rule(
      Pattern.compile("(?i)([\\\"']?\\b" + sensitiveKey + "\\b[\\\"']?\\s*[:=]\\s*)([^\\s,;}&\\\"']+)"),
      "$1" + Replacement
    ),
    Rule(Pattern.compile("\\b(?:AKIA|ASIA)[A-Z0-9]{16}\\b"), Replacement),
    Rule(Pattern.compile("\\bgh[pousr]_[A-Za-z0-9]{20,}\\b"), Replacement),
    Rule(Pattern.compile("\\bxox[baprs]-[A-Za-z0-9-]{10,}\\b"), Replacement),
    Rule(Pattern.compile("\\bsk_(?:live|test)_[A-Za-z0-9]{16,}\\b"), Replacement),
    Rule(Pattern.compile("\\bAIza[0-9A-Za-z_-]{35}\\b"), Replacement),
    Rule(
      Pattern.compile("\\beyJ[A-Za-z0-9_-]+\\.eyJ[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\b"),
      Replacement
    )
  )

  def redact(value: String): String = {
    val original = Option(value).getOrElse("")
    val sanitized = rules.foldLeft(original) { case (current, Rule(pattern, replacement)) =>
      pattern.matcher(current).replaceAll(replacement)
    }
    if (sanitized.length <= MaxDiagnosticLength) sanitized
    else sanitized.take(MaxDiagnosticLength) + s"…[truncated ${sanitized.length - MaxDiagnosticLength} chars]"
  }

  def redactAll(values: Seq[String]): Seq[String] = values.map(redact)

  def redactCountKeys(values: Map[String, Long]): Map[String, Long] =
    values.toSeq.groupBy { case (key, _) => redact(key) }.map { case (key, entries) =>
      key -> entries.map(_._2).sum
    }

  def message(error: Throwable): String =
    redact(Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getName))

  /** Returns a throwable safe to hand to a logger. Causes and suppressed exceptions are deliberately omitted because
    * logging frameworks traverse them and could otherwise print the original secret-bearing message.
    */
  def safeThrowable(error: Throwable): Throwable = {
    val sanitized = new RuntimeException(s"${error.getClass.getName}: ${message(error)}")
    sanitized.setStackTrace(error.getStackTrace)
    sanitized
  }
}
