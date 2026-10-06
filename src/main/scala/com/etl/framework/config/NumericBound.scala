package com.etl.framework.config

private[framework] object NumericBound {
  val MaxPrecision: Int = 38

  def parse(value: String, fieldName: String): Either[String, BigDecimal] = {
    try {
      val parsed = BigDecimal(value).bigDecimal.stripTrailingZeros
      val normalized = if (parsed.scale < 0) parsed.setScale(0) else parsed
      val requiredPrecision = math.max(normalized.precision, normalized.scale)
      if (requiredPrecision > MaxPrecision)
        Left(s"'$fieldName' value '$value' exceeds Spark's maximum decimal precision of $MaxPrecision digits")
      else Right(BigDecimal(normalized))
    } catch {
      case _: NumberFormatException =>
        val isNonFinite =
          try !java.lang.Double.isFinite(java.lang.Double.parseDouble(value))
          catch { case _: NumberFormatException => false }

        if (isNonFinite) Left(s"'$fieldName' value '$value' must be finite")
        else Left(s"'$fieldName' value '$value' is not numeric")
    }
  }
}
