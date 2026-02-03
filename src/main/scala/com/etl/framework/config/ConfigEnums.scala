package com.etl.framework.config

import io.circe.{Decoder, Encoder}

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
  
  def fromString(s: String): Either[String, JoinStrategy] = s.toLowerCase match {
    case "nest" => Right(Nest)
    case "flatten" => Right(Flatten)
    case "aggregate" => Right(Aggregate)
    case other => Left(s"Unknown join strategy: '$other'. Valid values: ${values.map(_.name).mkString(", ")}")
  }
  
  implicit val decoder: Decoder[JoinStrategy] = Decoder.decodeString.emap(fromString)
  implicit val encoder: Encoder[JoinStrategy] = Encoder.encodeString.contramap(_.name)
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
    case other => Left(s"Unknown source type: '$other'. Valid values: ${values.map(_.name).mkString(", ")}")
  }
  
  implicit val decoder: Decoder[SourceType] = Decoder.decodeString.emap(fromString)
  implicit val encoder: Encoder[SourceType] = Encoder.encodeString.contramap(_.name)
}

// ═══════════════════════════════════════════════════════════════════════════
// FILE FORMAT
// ═══════════════════════════════════════════════════════════════════════════
sealed trait FileFormat extends Product with Serializable {
  def name: String
  def sparkFormat: String
}

object FileFormat {
  case object CSV extends FileFormat { val name = "csv"; val sparkFormat = "csv" }
  case object Parquet extends FileFormat { val name = "parquet"; val sparkFormat = "parquet" }
  case object JSON extends FileFormat { val name = "json"; val sparkFormat = "json" }
  
  val values: Seq[FileFormat] = Seq(CSV, Parquet, JSON)
  
  def fromString(s: String): Either[String, FileFormat] = s.toLowerCase match {
    case "csv" => Right(CSV)
    case "parquet" => Right(Parquet)
    case "json" => Right(JSON)
    case other => Left(s"Unknown file format: '$other'. Valid values: ${values.map(_.name).mkString(", ")}")
  }
  
  implicit val decoder: Decoder[FileFormat] = Decoder.decodeString.emap(fromString)
  implicit val encoder: Encoder[FileFormat] = Encoder.encodeString.contramap(_.name)
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
    case "full" => Right(Full)
    case "delta" => Right(Delta)
    case "scd2" => Right(SCD2)
    case other => Left(s"Unknown load mode: '$other'. Valid values: ${values.map(_.name).mkString(", ")}")
  }
  
  implicit val decoder: Decoder[LoadMode] = Decoder.decodeString.emap(fromString)
  implicit val encoder: Encoder[LoadMode] = Encoder.encodeString.contramap(_.name)
}

// ═══════════════════════════════════════════════════════════════════════════
// VALIDATION RULE TYPE
// ═══════════════════════════════════════════════════════════════════════════
sealed trait ValidationRuleType extends Product with Serializable {
  def name: String
}

object ValidationRuleType {
  case object PKUniqueness extends ValidationRuleType { val name = "pk_uniqueness" }
  case object FKIntegrity extends ValidationRuleType { val name = "fk_integrity" }
  case object Regex extends ValidationRuleType { val name = "regex" }
  case object Range extends ValidationRuleType { val name = "range" }
  case object Domain extends ValidationRuleType { val name = "domain" }
  case object NotNull extends ValidationRuleType { val name = "not_null" }
  case object Custom extends ValidationRuleType { val name = "custom" }
  
  val values: Seq[ValidationRuleType] = Seq(PKUniqueness, FKIntegrity, Regex, Range, Domain, NotNull, Custom)
  
  def fromString(s: String): Either[String, ValidationRuleType] = s.toLowerCase match {
    case "pk_uniqueness" => Right(PKUniqueness)
    case "fk_integrity" => Right(FKIntegrity)
    case "regex" => Right(Regex)
    case "range" => Right(Range)
    case "domain" => Right(Domain)
    case "not_null" => Right(NotNull)
    case "custom" => Right(Custom)
    case other => Left(s"Unknown validation rule type: '$other'. Valid values: ${values.map(_.name).mkString(", ")}")
  }
  
  implicit val decoder: Decoder[ValidationRuleType] = Decoder.decodeString.emap(fromString)
  implicit val encoder: Encoder[ValidationRuleType] = Encoder.encodeString.contramap(_.name)
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
  
  def fromString(s: String): Either[String, OnFailureAction] = s.toLowerCase match {
    case "reject" => Right(Reject)
    case "warn" => Right(Warn)
    case "skip" => Right(Skip)
    case other => Left(s"Unknown on-failure action: '$other'. Valid values: ${values.map(_.name).mkString(", ")}")
  }
  
  implicit val decoder: Decoder[OnFailureAction] = Decoder.decodeString.emap(fromString)
  implicit val encoder: Encoder[OnFailureAction] = Encoder.encodeString.contramap(_.name)
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
  case object CollectList extends AggregationFunction { val name = "collect_list" }
  case object CollectSet extends AggregationFunction { val name = "collect_set" }
  
  val values: Seq[AggregationFunction] = Seq(Sum, Count, Avg, Min, Max, First, Last, CollectList, CollectSet)
  
  def fromString(s: String): Either[String, AggregationFunction] = s.toLowerCase match {
    case "sum" => Right(Sum)
    case "count" => Right(Count)
    case "avg" | "average" => Right(Avg)
    case "min" => Right(Min)
    case "max" => Right(Max)
    case "first" => Right(First)
    case "last" => Right(Last)
    case "collect_list" => Right(CollectList)
    case "collect_set" => Right(CollectSet)
    case other => Left(s"Unknown aggregation function: '$other'. Valid values: ${values.map(_.name).mkString(", ")}")
  }
  
  implicit val decoder: Decoder[AggregationFunction] = Decoder.decodeString.emap(fromString)
  implicit val encoder: Encoder[AggregationFunction] = Encoder.encodeString.contramap(_.name)
}

// ═══════════════════════════════════════════════════════════════════════════
// MERGE STRATEGY
// ═══════════════════════════════════════════════════════════════════════════
sealed trait MergeStrategy extends Product with Serializable {
  def name: String
}

object MergeStrategy {
  case object Upsert extends MergeStrategy { val name = "upsert" }
  case object Append extends MergeStrategy { val name = "append" }
  case object Replace extends MergeStrategy { val name = "replace" }
  
  val values: Seq[MergeStrategy] = Seq(Upsert, Append, Replace)
  
  def fromString(s: String): Either[String, MergeStrategy] = s.toLowerCase match {
    case "upsert" => Right(Upsert)
    case "append" => Right(Append)
    case "replace" => Right(Replace)
    case other => Left(s"Unknown merge strategy: '$other'. Valid values: ${values.map(_.name).mkString(", ")}")
  }
  
  implicit val decoder: Decoder[MergeStrategy] = Decoder.decodeString.emap(fromString)
  implicit val encoder: Encoder[MergeStrategy] = Encoder.encodeString.contramap(_.name)
}
