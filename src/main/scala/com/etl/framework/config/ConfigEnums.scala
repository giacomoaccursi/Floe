package com.etl.framework.config

import pureconfig.{ConfigReader, ConfigWriter}
import pureconfig.error.CannotConvert

// ═══════════════════════════════════════════════════════════════════════════
// JOIN STRATEGY
// ═══════════════════════════════════════════════════════════════════════════
sealed trait JoinStrategy extends Product with Serializable {
  def name: String
}

object JoinStrategy {
  case object Nest extends JoinStrategy { val name = "nest" }
  case object Flatten extends JoinStrategy { val name = "flatten" }
  case object Aggregate extends JoinStrategy { val name = "aggregate" }

  val values: Seq[JoinStrategy] = Seq(Nest, Flatten, Aggregate)

  def fromString(s: String): Either[String, JoinStrategy] =
    s.toLowerCase match {
      case "nest"      => Right(Nest)
      case "flatten"   => Right(Flatten)
      case "aggregate" => Right(Aggregate)
      case other       =>
        Left(
          s"Unknown join strategy: '$other'. Valid values: ${values.map(_.name).mkString(", ")}"
        )
    }

  implicit val reader: ConfigReader[JoinStrategy] =
    ConfigReader.fromString[JoinStrategy](s =>
      fromString(s).left.map(msg => CannotConvert(s, "JoinStrategy", msg))
    )

  implicit val writer: ConfigWriter[JoinStrategy] =
    ConfigWriter[String].contramap(_.name)
}

// ═══════════════════════════════════════════════════════════════════════════
// JOIN TYPE
// ═══════════════════════════════════════════════════════════════════════════
sealed trait JoinType extends Product with Serializable {
  def name: String
  def sparkType: String
}

object JoinType {
  case object Inner extends JoinType {
    val name = "inner"; val sparkType = "inner"
  }
  case object LeftOuter extends JoinType {
    val name = "left_outer"; val sparkType = "left"
  }
  case object RightOuter extends JoinType {
    val name = "right_outer"; val sparkType = "right"
  }
  case object FullOuter extends JoinType {
    val name = "full_outer"; val sparkType = "full"
  }

  val values: Seq[JoinType] = Seq(Inner, LeftOuter, RightOuter, FullOuter)

  def fromString(s: String): Either[String, JoinType] =
    s.toLowerCase match {
      case "inner"                 => Right(Inner)
      case "left" | "left_outer"   => Right(LeftOuter)
      case "right" | "right_outer" => Right(RightOuter)
      case "full" | "full_outer"   => Right(FullOuter)
      case other                   =>
        Left(
          s"Unknown join type: '$other'. Valid values: ${values.map(_.name).mkString(", ")}"
        )
    }

  implicit val reader: ConfigReader[JoinType] =
    ConfigReader.fromString[JoinType](s =>
      fromString(s).left.map(msg => CannotConvert(s, "JoinType", msg))
    )

  implicit val writer: ConfigWriter[JoinType] =
    ConfigWriter[String].contramap(_.name)
}

// ═══════════════════════════════════════════════════════════════════════════
// SOURCE TYPE
// ═══════════════════════════════════════════════════════════════════════════
sealed trait SourceType extends Product with Serializable {
  def name: String
}

object SourceType {
  case object File extends SourceType { val name = "file" }
  case object JDBC extends SourceType { val name = "jdbc" }

  val values: Seq[SourceType] = Seq(File, JDBC)

  def fromString(s: String): Either[String, SourceType] = s.toLowerCase match {
    case "file" => Right(File)
    case "jdbc" => Right(JDBC)
    case other  =>
      Left(
        s"Unknown source type: '$other'. Valid values: ${values.map(_.name).mkString(", ")}"
      )
  }

  implicit val reader: ConfigReader[SourceType] =
    ConfigReader.fromString[SourceType](s =>
      fromString(s).left.map(msg => CannotConvert(s, "SourceType", msg))
    )

  implicit val writer: ConfigWriter[SourceType] =
    ConfigWriter[String].contramap(_.name)
}

// ═══════════════════════════════════════════════════════════════════════════
// FILE FORMAT
// ═══════════════════════════════════════════════════════════════════════════
sealed trait FileFormat extends Product with Serializable {
  def name: String
  def sparkFormat: String
}

object FileFormat {
  case object CSV extends FileFormat {
    val name = "csv"; val sparkFormat = "csv"
  }
  case object Parquet extends FileFormat {
    val name = "parquet"; val sparkFormat = "parquet"
  }
  case object JSON extends FileFormat {
    val name = "json"; val sparkFormat = "json"
  }
  case object JDBC extends FileFormat {
    val name = "jdbc"; val sparkFormat = "jdbc"
  }

  val values: Seq[FileFormat] = Seq(CSV, Parquet, JSON, JDBC)

  def fromString(s: String): Either[String, FileFormat] = s.toLowerCase match {
    case "csv"     => Right(CSV)
    case "parquet" => Right(Parquet)
    case "json"    => Right(JSON)
    case "jdbc"    => Right(JDBC)
    case other     =>
      Left(
        s"Unknown file format: '$other'. Valid values: ${values.map(_.name).mkString(", ")}"
      )
  }

  implicit val reader: ConfigReader[FileFormat] =
    ConfigReader.fromString[FileFormat](s =>
      fromString(s).left.map(msg => CannotConvert(s, "FileFormat", msg))
    )

  implicit val writer: ConfigWriter[FileFormat] =
    ConfigWriter[String].contramap(_.name)
}

// ═══════════════════════════════════════════════════════════════════════════
// LOAD MODE
// ═══════════════════════════════════════════════════════════════════════════
sealed trait LoadMode extends Product with Serializable {
  def name: String
}

object LoadMode {
  case object Full extends LoadMode { val name = "full" }
  case object Delta extends LoadMode { val name = "delta" }
  case object SCD2 extends LoadMode { val name = "scd2" }

  val values: Seq[LoadMode] = Seq(Full, Delta, SCD2)

  def fromString(s: String): Either[String, LoadMode] = s.toLowerCase match {
    case "full"  => Right(Full)
    case "delta" => Right(Delta)
    case "scd2"  => Right(SCD2)
    case other   =>
      Left(
        s"Unknown load mode: '$other'. Valid values: ${values.map(_.name).mkString(", ")}"
      )
  }

  implicit val reader: ConfigReader[LoadMode] =
    ConfigReader.fromString[LoadMode](s =>
      fromString(s).left.map(msg => CannotConvert(s, "LoadMode", msg))
    )

  implicit val writer: ConfigWriter[LoadMode] =
    ConfigWriter[String].contramap(_.name)
}

// ═══════════════════════════════════════════════════════════════════════════
// VALIDATION RULE TYPE
// ═══════════════════════════════════════════════════════════════════════════
sealed trait ValidationRuleType extends Product with Serializable {
  def name: String
}

object ValidationRuleType {
  case object PKUniqueness extends ValidationRuleType {
    val name = "pk_uniqueness"
  }
  case object FKIntegrity extends ValidationRuleType {
    val name = "fk_integrity"
  }
  case object Regex extends ValidationRuleType { val name = "regex" }
  case object Range extends ValidationRuleType { val name = "range" }
  case object Domain extends ValidationRuleType { val name = "domain" }
  case object NotNull extends ValidationRuleType { val name = "not_null" }
  case object Custom extends ValidationRuleType { val name = "custom" }
  case object Schema extends ValidationRuleType { val name = "schema" }

  val values: Seq[ValidationRuleType] =
    Seq(
      PKUniqueness,
      FKIntegrity,
      Regex,
      Range,
      Domain,
      NotNull,
      Custom,
      Schema
    )

  def fromString(s: String): Either[String, ValidationRuleType] =
    s.toLowerCase match {
      case "pk_uniqueness" => Right(PKUniqueness)
      case "fk_integrity"  => Right(FKIntegrity)
      case "regex"         => Right(Regex)
      case "range"         => Right(Range)
      case "domain"        => Right(Domain)
      case "not_null"      => Right(NotNull)
      case "custom"        => Right(Custom)
      case "schema"        => Right(Schema)
      case other           =>
        Left(
          s"Unknown validation rule type: '$other'. Valid values: ${values.map(_.name).mkString(", ")}"
        )
    }

  implicit val reader: ConfigReader[ValidationRuleType] =
    ConfigReader.fromString[ValidationRuleType](s =>
      fromString(s).left.map(msg => CannotConvert(s, "ValidationRuleType", msg))
    )

  implicit val writer: ConfigWriter[ValidationRuleType] =
    ConfigWriter[String].contramap(_.name)
}

// ═══════════════════════════════════════════════════════════════════════════
// ON FAILURE ACTION
// ═══════════════════════════════════════════════════════════════════════════
sealed trait OnFailureAction extends Product with Serializable {
  def name: String
}

object OnFailureAction {
  case object Reject extends OnFailureAction { val name = "reject" }
  case object Warn extends OnFailureAction { val name = "warn" }
  case object Skip extends OnFailureAction { val name = "skip" }

  val values: Seq[OnFailureAction] = Seq(Reject, Warn, Skip)

  def fromString(s: String): Either[String, OnFailureAction] =
    s.toLowerCase match {
      case "reject" => Right(Reject)
      case "warn"   => Right(Warn)
      case "skip"   => Right(Skip)
      case other    =>
        Left(
          s"Unknown on-failure action: '$other'. Valid values: ${values.map(_.name).mkString(", ")}"
        )
    }

  implicit val reader: ConfigReader[OnFailureAction] =
    ConfigReader.fromString[OnFailureAction](s =>
      fromString(s).left.map(msg => CannotConvert(s, "OnFailureAction", msg))
    )

  implicit val writer: ConfigWriter[OnFailureAction] =
    ConfigWriter[String].contramap(_.name)
}

// ═══════════════════════════════════════════════════════════════════════════
// AGGREGATION FUNCTION
// ═══════════════════════════════════════════════════════════════════════════
sealed trait AggregationFunction extends Product with Serializable {
  def name: String
}

object AggregationFunction {
  case object Sum extends AggregationFunction { val name = "sum" }
  case object Count extends AggregationFunction { val name = "count" }
  case object Avg extends AggregationFunction { val name = "avg" }
  case object Min extends AggregationFunction { val name = "min" }
  case object Max extends AggregationFunction { val name = "max" }
  case object First extends AggregationFunction { val name = "first" }
  case object Last extends AggregationFunction { val name = "last" }
  case object CollectList extends AggregationFunction {
    val name = "collect_list"
  }
  case object CollectSet extends AggregationFunction {
    val name = "collect_set"
  }

  val values: Seq[AggregationFunction] =
    Seq(Sum, Count, Avg, Min, Max, First, Last, CollectList, CollectSet)

  def fromString(s: String): Either[String, AggregationFunction] =
    s.toLowerCase match {
      case "sum"             => Right(Sum)
      case "count"           => Right(Count)
      case "avg" | "average" => Right(Avg)
      case "min"             => Right(Min)
      case "max"             => Right(Max)
      case "first"           => Right(First)
      case "last"            => Right(Last)
      case "collect_list"    => Right(CollectList)
      case "collect_set"     => Right(CollectSet)
      case other             =>
        Left(
          s"Unknown aggregation function: '$other'. Valid values: ${values.map(_.name).mkString(", ")}"
        )
    }

  implicit val reader: ConfigReader[AggregationFunction] =
    ConfigReader.fromString[AggregationFunction](s =>
      fromString(s).left.map(msg =>
        CannotConvert(s, "AggregationFunction", msg)
      )
    )

  implicit val writer: ConfigWriter[AggregationFunction] =
    ConfigWriter[String].contramap(_.name)
}
