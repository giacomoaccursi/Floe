package com.etl.framework.config

/** Structural validation shared by YAML loading and programmatic pipeline construction. */
object FlowConfigValidator {

  def validate(config: FlowConfig): Seq[String] = {
    val errors = Seq.newBuilder[String]
    val declaredColumns = config.schema.columns.map(_.name)
    val declaredSet = declaredColumns.toSet

    if (config.name.trim.isEmpty) errors += "name must not be empty"
    if (declaredColumns.distinct.size != declaredColumns.size)
      errors += "schema contains duplicate column names"
    val physicalColumns = config.schema.columns.map(column => column.sourceColumn.getOrElse(column.name))
    if (physicalColumns.distinct.size != physicalColumns.size)
      errors += "sourceColumn mappings contain duplicate physical column names"
    config.maxRejectionRate.foreach { rate =>
      if (rate < 0.0 || rate > 1.0) errors += "maxRejectionRate must be between 0.0 and 1.0"
    }
    config.minInputRecords.foreach { minimum =>
      if (minimum < 0L) errors += "minInputRecords must be greater than or equal to zero"
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
    }

    if (config.loadMode.`type` == LoadMode.Delta && config.validation.primaryKey.isEmpty)
      errors += "Delta requires non-empty primaryKey; implicit append is unsupported"

    if (config.loadMode.`type` == LoadMode.SCD2) {
      if (config.loadMode.compareColumns.isEmpty) errors += "SCD2 requires compareColumns to be non-empty"
      if (config.validation.primaryKey.isEmpty) errors += "SCD2 requires non-empty primaryKey for record identification"
      if (declaredSet.nonEmpty) {
        val missingCompare = config.loadMode.compareColumns.filterNot(declaredSet)
        if (missingCompare.nonEmpty)
          errors += s"SCD2 compareColumns are not declared in schema: ${missingCompare.mkString(", ")}"
        val nullablePk = config.validation.primaryKey.filter { pk =>
          config.schema.columns.find(_.name == pk).exists(_.nullable)
        }
        if (nullablePk.nonEmpty)
          errors += s"SCD2 primary-key columns must be non-nullable: ${nullablePk.mkString(", ")}"
      }
    }

    errors.result()
  }
}
