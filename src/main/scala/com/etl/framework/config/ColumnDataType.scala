package com.etl.framework.config

import org.apache.spark.sql.types._

import java.util.Locale
import scala.util.{Failure, Success, Try}

private[framework] object ColumnDataType {
  private val DecimalPattern = """decimal\((\d+),\s*(\d+)\)""".r

  def parse(typeName: String): Either[String, DataType] =
    Option(typeName).map(_.trim.toLowerCase(Locale.ROOT)).getOrElse("") match {
      case "string" | "varchar" | "text" => Right(StringType)
      case "int" | "integer"             => Right(IntegerType)
      case "long" | "bigint"             => Right(LongType)
      case "float"                       => Right(FloatType)
      case "double"                      => Right(DoubleType)
      case "boolean" | "bool"            => Right(BooleanType)
      case "date"                        => Right(DateType)
      case "timestamp" | "datetime"      => Right(TimestampType)
      case "decimal"                     => Right(DecimalType(38, 18))
      case DecimalPattern(precision, scale) =>
        Try(DecimalType(precision.toInt, scale.toInt)) match {
          case Success(dataType) => Right(dataType)
          case Failure(_) =>
            Left(
              s"Invalid decimal type '$typeName': precision must be between 1 and 38 and scale between 0 and precision"
            )
        }
      case "binary"             => Right(BinaryType)
      case "byte" | "tinyint"   => Right(ByteType)
      case "short" | "smallint" => Right(ShortType)
      case _ =>
        Left(
          s"Unsupported column type '$typeName'. Supported types: string, int, long, float, double, boolean, date, timestamp, decimal, binary, byte, short"
        )
    }
}
