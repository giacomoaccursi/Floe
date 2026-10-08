package com.etl.framework.config

import java.util.Locale
import java.util.regex.Pattern
import scala.util.Try

/** Structural validation shared by YAML loading and programmatic pipeline construction. */
object FlowConfigValidator {

  def validate(config: FlowConfig): Seq[String] = {
    val errors = Seq.newBuilder[String]
    val declaredColumns = config.schema.columns.map(_.name)
    val declaredSet = declaredColumns.toSet
    val normalizedDeclaredColumns = declaredColumns.map(_.toLowerCase(Locale.ROOT))

    if (config.name.trim.isEmpty) errors += "name must not be empty"
    else if (config.name != config.name.trim) errors += "name must not have leading or trailing whitespace"
    if (config.name.contains("."))
      errors += "name must be unqualified; configure catalog and namespace globally"
    config.source.`type` match {
      case SourceType.File =>
        if (config.source.path.trim.isEmpty) errors += "file source path must not be blank"
        if (config.source.format.isEmpty) errors += "file source format is required"
      case SourceType.JDBC =>
        if (config.source.options.get("url").forall(_.trim.isEmpty))
          errors += "JDBC source option 'url' is required"
        if (config.source.options.get("query").forall(_.trim.isEmpty) && config.source.path.trim.isEmpty)
          errors += "JDBC source requires a non-blank path or query"
      case SourceType.Custom(_) =>
    }
    if (config.schema.enforceSchema && config.schema.columns.isEmpty)
      errors += "schema.columns must be non-empty when enforceSchema is true"
    if (config.schema.columns.exists(_.name.trim.isEmpty)) errors += "schema column names must not be blank"
    if (config.schema.columns.exists(_.`type`.trim.isEmpty)) errors += "schema column types must not be blank"
    config.schema.columns.filter(_.`type`.trim.nonEmpty).foreach { column =>
      ColumnDataType.parse(column.`type`).left.foreach(error => errors += s"column '${column.name}': $error")
    }
    if (config.schema.columns.flatMap(_.sourceColumn).exists(_.trim.isEmpty))
      errors += "sourceColumn values must not be blank"
    if (declaredColumns.distinct.size != declaredColumns.size)
      errors += "schema contains duplicate column names"
    if (normalizedDeclaredColumns.distinct.size != normalizedDeclaredColumns.size)
      errors += "schema contains case-insensitive duplicate column names"
    val physicalColumns = config.schema.columns.map(column => column.sourceColumn.getOrElse(column.name))
    if (physicalColumns.distinct.size != physicalColumns.size)
      errors += "sourceColumn mappings contain duplicate physical column names"
    if (physicalColumns.map(_.toLowerCase(Locale.ROOT)).distinct.size != physicalColumns.size)
      errors += "sourceColumn mappings contain case-insensitive duplicate physical column names"
    config.maxRejectionRate.foreach { rate =>
      if (!java.lang.Double.isFinite(rate)) errors += "maxRejectionRate must be finite"
      else if (rate < 0.0 || rate > 1.0) errors += "maxRejectionRate must be between 0.0 and 1.0"
    }
    config.minInputRecords.foreach { minimum =>
      if (minimum < 0L) errors += "minInputRecords must be greater than or equal to zero"
    }

    if (config.validation.primaryKey.exists(_.trim.isEmpty)) errors += "primaryKey columns must not be blank"
    if (config.validation.primaryKey.distinct.size != config.validation.primaryKey.size)
      errors += "primaryKey columns must be unique"
    if (config.dependsOn.exists(_.trim.isEmpty)) errors += "dependsOn names must not be blank"
    if (config.dependsOn.map(_.toLowerCase(Locale.ROOT)).distinct.size != config.dependsOn.size)
      errors += "dependsOn names must be unique case-insensitively"
    if (config.dependsOn.exists(_.equalsIgnoreCase(config.name))) errors += "a flow cannot depend on itself"
    if (config.output.tableProperties.keys.exists(_.trim.isEmpty))
      errors += "tableProperties keys must not be blank"
    if (config.output.tableProperties.keys.exists(_.equalsIgnoreCase("format-version")))
      errors += "tableProperties must not override 'format-version'; Floe requires Iceberg format version 2"

    val sortColumns = config.output.sortOrder.map(_.trim)
    if (sortColumns.exists(_.isEmpty)) errors += "sortOrder columns must not be blank"
    if (sortColumns.map(_.toLowerCase(Locale.ROOT)).distinct.size != sortColumns.size)
      errors += "sortOrder columns must be unique case-insensitively"
    if (declaredSet.nonEmpty) {
      val declaredNormalized = normalizedDeclaredColumns.toSet
      val missingSortColumns = sortColumns
        .filter(_.nonEmpty)
        .filterNot(column => declaredNormalized.contains(column.toLowerCase(Locale.ROOT)))
        .distinct
      if (missingSortColumns.nonEmpty)
        errors += s"sortOrder columns are not declared in schema: ${missingSortColumns.mkString(", ")}"
    }

    val parsedPartitions = config.output.icebergPartitions.map(IcebergPartitionTransform.parse)
    parsedPartitions.collect { case Left(error) => error }.foreach(errors += _)
    val validPartitions = parsedPartitions.collect { case Right(transform) => transform }
    if (validPartitions.map(_.canonicalKey).distinct.size != validPartitions.size)
      errors += "Iceberg partition transforms must be unique case-insensitively"
    if (declaredSet.nonEmpty) {
      val declaredNormalized = normalizedDeclaredColumns.toSet
      validPartitions.foreach { transform =>
        if (!declaredNormalized.contains(transform.column.toLowerCase(Locale.ROOT)))
          errors +=
            s"Iceberg partition transform '${transform.display}' references undeclared column '${transform.column}'"
      }
    }

    if (declaredSet.nonEmpty) {
      val missingPk = config.validation.primaryKey.filterNot(declaredSet)
      if (missingPk.nonEmpty) errors += s"primaryKey columns are not declared in schema: ${missingPk.mkString(", ")}"
      val missingLocalFk = config.validation.foreignKeys.flatMap(_.columns).distinct.filterNot(declaredSet)
      if (missingLocalFk.nonEmpty)
        errors += s"foreign-key columns are not declared in schema: ${missingLocalFk.mkString(", ")}"
    }

    config.validation.foreignKeys.foreach { fk =>
      if (fk.onOrphan == OrphanAction.Delete)
        errors +=
          s"foreign key ${fk.displayName} uses unsupported onOrphan=delete; use warn or ignore and remediate explicitly"
      if (fk.columns.isEmpty || fk.references.columns.isEmpty)
        errors += s"foreign key ${fk.displayName} must contain at least one local and referenced column"
      else if (fk.columns.size != fk.references.columns.size)
        errors +=
          s"foreign key ${fk.displayName} has different local/reference arity (${fk.columns.size} != ${fk.references.columns.size})"
      if (fk.columns.exists(_.trim.isEmpty) || fk.references.columns.exists(_.trim.isEmpty))
        errors += s"foreign key ${fk.displayName} contains a blank column"
      if (
        fk.columns.distinct.size != fk.columns.size || fk.references.columns.distinct.size != fk.references.columns.size
      )
        errors += s"foreign key ${fk.displayName} contains duplicate columns"
      if (fk.references.flow.trim.isEmpty) errors += "foreign key referenced flow must not be blank"
    }

    config.validation.rules.zipWithIndex.foreach { case (rule, index) =>
      val prefix = s"validation rule ${index + 1} (${rule.`type`.name})"
      val ruleColumn = rule.column.filter(_.trim.nonEmpty)
      rule.`type` match {
        case ValidationRuleType.Regex =>
          if (ruleColumn.isEmpty) errors += s"$prefix requires a non-blank column"
          rule.pattern.filter(_.nonEmpty) match {
            case None => errors += s"$prefix requires a non-blank pattern"
            case Some(pattern) if Try(Pattern.compile(pattern)).isFailure =>
              errors += s"$prefix has an invalid regex pattern"
            case _ =>
          }
        case ValidationRuleType.Range =>
          if (ruleColumn.isEmpty) errors += s"$prefix requires a non-blank column"
          if (rule.min.forall(_.trim.isEmpty) && rule.max.forall(_.trim.isEmpty))
            errors += s"$prefix rule requires min or max"
          if (rule.min.exists(_.trim.isEmpty)) errors += s"$prefix min must not be blank"
          if (rule.max.exists(_.trim.isEmpty)) errors += s"$prefix max must not be blank"
          val parsedMin = rule.min.filter(_.trim.nonEmpty).map(NumericBound.parse(_, "min"))
          val parsedMax = rule.max.filter(_.trim.nonEmpty).map(NumericBound.parse(_, "max"))
          Seq(parsedMin, parsedMax).flatten.collect { case Left(error) => error }.foreach { error =>
            errors += s"$prefix $error"
          }
          (parsedMin, parsedMax) match {
            case (Some(Right(min)), Some(Right(max))) if min > max =>
              errors += s"$prefix min ($min) must not exceed max ($max)"
            case _ =>
          }
        case ValidationRuleType.Domain =>
          if (ruleColumn.isEmpty) errors += s"$prefix requires a non-blank column"
          if (rule.domainName.forall(_.trim.isEmpty)) errors += s"$prefix requires a non-blank domainName"
        case ValidationRuleType.Custom =>
          if (rule.`class`.forall(_.trim.isEmpty)) errors += s"$prefix custom rule requires a non-blank class"
        case managed =>
          errors += s"$prefix rule type '${managed.name}' is managed by the validation engine and cannot appear in rules"
      }
      ruleColumn.foreach { column =>
        if (declaredSet.nonEmpty && !declaredSet.contains(column))
          errors += s"$prefix column '$column' is not declared in schema"
      }
    }

    if (config.loadMode.`type` == LoadMode.Delta && config.validation.primaryKey.isEmpty)
      errors += "Delta requires non-empty primaryKey; implicit append is unsupported"

    if (config.loadMode.`type` == LoadMode.SCD2) {
      if (config.loadMode.compareColumns.isEmpty) errors += "SCD2 requires compareColumns to be non-empty"
      if (config.loadMode.compareColumns.exists(_.trim.isEmpty)) errors += "SCD2 compareColumns must not be blank"
      if (config.loadMode.compareColumns.distinct.size != config.loadMode.compareColumns.size)
        errors += "SCD2 compareColumns must be unique"
      if (config.validation.primaryKey.isEmpty) errors += "SCD2 requires non-empty primaryKey for record identification"
      val metadataColumns = Seq(
        config.loadMode.validFromColumn.getOrElse("valid_from"),
        config.loadMode.validToColumn.getOrElse("valid_to"),
        config.loadMode.isCurrentColumn.getOrElse("is_current")
      ) ++ (if (config.loadMode.detectDeletes) Seq(config.loadMode.isActiveColumn.getOrElse("is_active"))
            else config.loadMode.isActiveColumn.toSeq)
      if (metadataColumns.exists(_.trim.isEmpty)) errors += "SCD2 metadata column names must not be blank"
      if (metadataColumns.map(_.toLowerCase(Locale.ROOT)).distinct.size != metadataColumns.size)
        errors += "SCD2 metadata column names must be unique case-insensitively"
      if (declaredSet.nonEmpty) {
        val missingCompare = config.loadMode.compareColumns.filterNot(declaredSet)
        if (missingCompare.nonEmpty)
          errors += s"SCD2 compareColumns are not declared in schema: ${missingCompare.mkString(", ")}"
        val nullablePk = config.validation.primaryKey.filter { pk =>
          config.schema.columns.find(_.name == pk).exists(_.nullable)
        }
        if (nullablePk.nonEmpty)
          errors += s"SCD2 primary-key columns must be non-nullable: ${nullablePk.mkString(", ")}"

        val declaredByNormalizedName = declaredColumns.map(name => name.toLowerCase(Locale.ROOT) -> name).toMap
        val collisions = metadataColumns
          .flatMap(name => declaredByNormalizedName.get(name.toLowerCase(Locale.ROOT)))
          .distinct
        if (collisions.nonEmpty)
          errors += s"SCD2 metadata columns collide with schema columns: ${collisions.mkString(", ")}"
      }
    }

    errors.result()
  }
}
