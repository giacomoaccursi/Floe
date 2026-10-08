package com.etl.framework.config

import java.util.Locale

/** Parsed representation of the Iceberg partition transforms supported by Floe. */
private[framework] final case class IcebergPartitionTransform(
    function: String,
    column: String,
    width: Option[Int] = None
) {
  def canonical: String =
    width match {
      case Some(value)                    => s"$function[$value]($column)"
      case None if function == "identity" => s"identity($column)"
      case None                           => s"$function($column)"
    }

  def canonicalKey: String = canonical.toLowerCase(Locale.ROOT)

  def display: String =
    width match {
      case Some(value)                    => s"$function($value, $column)"
      case None if function == "identity" => column
      case None                           => s"$function($column)"
    }
}

private[framework] object IcebergPartitionTransform {
  private val TransformCall = """^([A-Za-z][A-Za-z0-9_]*)\((.*)\)$""".r
  private val TemporalTransforms = Set("year", "month", "day", "hour")
  private val WidthTransforms = Set("bucket", "truncate")

  def parse(value: String): Either[String, IcebergPartitionTransform] = {
    val original = Option(value).getOrElse("")
    val trimmed = original.trim
    trimmed match {
      case "" => Left("Iceberg partition transforms must not be blank")
      case TransformCall(rawFunction, rawArguments) =>
        val function = rawFunction.toLowerCase(Locale.ROOT)
        val arguments = rawArguments.split(",", -1).map(_.trim).toSeq
        if (TemporalTransforms.contains(function)) parseTemporal(original, function, arguments)
        else if (WidthTransforms.contains(function)) parseWidth(original, function, arguments)
        else Left(s"unsupported Iceberg partition transform '$original'")
      case _ if trimmed.contains("(") || trimmed.contains(")") =>
        Left(s"invalid Iceberg partition transform '$original'")
      case column => Right(IcebergPartitionTransform("identity", column))
    }
  }

  private def parseTemporal(
      original: String,
      function: String,
      arguments: Seq[String]
  ): Either[String, IcebergPartitionTransform] =
    arguments match {
      case Seq(column) if column.nonEmpty => Right(IcebergPartitionTransform(function, column))
      case _                              => Left(s"invalid Iceberg partition transform '$original'")
    }

  private def parseWidth(
      original: String,
      function: String,
      arguments: Seq[String]
  ): Either[String, IcebergPartitionTransform] =
    arguments match {
      case Seq(rawWidth, column) if rawWidth.forall(_.isDigit) && rawWidth.nonEmpty && column.nonEmpty =>
        scala.util.Try(BigInt(rawWidth)).toOption match {
          case Some(width) if width > 0 && width <= Int.MaxValue =>
            Right(IcebergPartitionTransform(function, column, Some(width.toInt)))
          case Some(_) => Left(s"Iceberg partition transform '$original' requires a positive width")
          case None    => Left(s"invalid Iceberg partition transform '$original'")
        }
      case _ => Left(s"invalid Iceberg partition transform '$original'")
    }
}
