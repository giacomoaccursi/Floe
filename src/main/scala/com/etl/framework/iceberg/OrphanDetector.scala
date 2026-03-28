package com.etl.framework.iceberg

import com.etl.framework.config._
import com.etl.framework.orchestration.ExecutionPlan
import com.etl.framework.orchestration.flow.FlowResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable

case class OrphanReport(
    flowName: String,
    fkName: String,
    parentFlowName: String,
    orphanCount: Long,
    removedParentKeyCount: Long,
    actionTaken: String,
    deletedChildKeyCount: Long = 0
)

/** Detects and resolves orphaned records after batch execution. Uses Iceberg time travel to find PKs removed by parent
  * flows, then checks child flows for orphaned FK references. Processes flows in topological order to support cascade
  * propagation.
  */
class OrphanDetector(
    spark: SparkSession,
    icebergConfig: IcebergConfig,
    flowConfigs: Seq[FlowConfig],
    flowResults: Seq[FlowResult]
) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val tableManager = new IcebergTableManager(spark, icebergConfig)
  private val flowConfigMap = flowConfigs.map(fc => fc.name -> fc).toMap
  private val flowResultMap = flowResults.map(fr => fr.flowName -> fr).toMap

  /** Detects and resolves orphans for all flows in topological order. Cascade: when onOrphan=Delete, deleted child PKs
    * propagate to next level.
    */
  def detectAndResolveOrphans(
      plan: ExecutionPlan
  ): Seq[OrphanReport] = {
    val removedKeysByFlow = mutable.Map[String, DataFrame]()
    val reports = mutable.ArrayBuffer[OrphanReport]()

    // Process flows in topological order (groups are already ordered)
    plan.groups.flatMap(_.flows).foreach { flowConfig =>
      flowConfig.validation.foreignKeys.foreach { fk =>
        if (fk.onOrphan != OrphanAction.Ignore) {
          processFK(flowConfig, fk, removedKeysByFlow) match {
            case Some(report) => reports += report
            case None         =>
          }
        }
      }
    }

    reports.toSeq
  }

  private def processFK(
      childFlow: FlowConfig,
      fk: ForeignKeyConfig,
      removedKeysByFlow: mutable.Map[String, DataFrame]
  ): Option[OrphanReport] = {
    val parentFlowConfig = flowConfigMap.get(fk.references.flow)
    val parentResult = flowResultMap.get(fk.references.flow)

    (parentFlowConfig, parentResult) match {
      case (Some(parentCfg), Some(parentRes)) =>
        findRemovedParentKeys(parentCfg, parentRes, fk, removedKeysByFlow)
          .flatMap { removed =>
            val cached = removed.cache()
            try {
              val removedCount = cached.count()
              if (removedCount == 0) None
              else resolveOrphans(childFlow, fk, cached, removedCount, removedKeysByFlow)
            } finally {
              cached.unpersist()
            }
          }

      case _ =>
        logger.debug(
          s"Skipping orphan check for ${childFlow.name}.${fk.displayName}: " +
            s"parent flow '${fk.references.flow}' not found in results"
        )
        None
    }
  }

  /** Finds PKs removed from the parent flow using Iceberg time travel or cascade propagation from a previously
    * processed flow.
    */
  private def findRemovedParentKeys(
      parentCfg: FlowConfig,
      parentResult: FlowResult,
      fk: ForeignKeyConfig,
      removedKeysByFlow: mutable.Map[String, DataFrame]
  ): Option[DataFrame] = {
    // Check cascade first: if parent had deletes propagated from an earlier level
    removedKeysByFlow.get(parentCfg.name) match {
      case Some(cascadedKeys) =>
        // Map cascaded PKs to the FK reference column
        val parentPkCol = fk.references.column
        if (cascadedKeys.columns.contains(parentPkCol)) {
          Some(cascadedKeys.select(parentPkCol))
        } else {
          None
        }

      case None =>
        // Use time travel to find removed PKs
        findRemovedKeysViaTimeTravel(parentCfg, parentResult, fk)
    }
  }

  /** Uses Iceberg time travel to compare previous and current snapshot and find PKs that were removed.
    */
  private def findRemovedKeysViaTimeTravel(
      parentCfg: FlowConfig,
      parentResult: FlowResult,
      fk: ForeignKeyConfig
  ): Option[DataFrame] = {
    // Only check parents that can remove records:
    // - Full load (replaces all data)
    // - SCD2 with detectDeletes (soft-deletes missing records)
    val canRemoveRecords = parentCfg.loadMode.`type` match {
      case LoadMode.Full  => true
      case LoadMode.SCD2  => parentCfg.loadMode.detectDeletes
      case LoadMode.Delta => false
    }

    if (!canRemoveRecords) return None

    // Need previous snapshot for time travel comparison
    val parentSnapshotId = parentResult.icebergMetadata.flatMap(_.parentSnapshotId)
    parentSnapshotId match {
      case None =>
        // First execution, no previous snapshot to compare
        logger.debug(
          s"No previous snapshot for ${parentCfg.name}, skipping orphan check"
        )
        None

      case Some(prevSnapshotId) =>
        val tableName = tableManager.resolveTableName(parentCfg)
        val refCol = fk.references.column

        try {
          // Previous snapshot: all PKs that existed before this batch
          val previousPKs = spark
            .sql(
              s"SELECT $refCol FROM $tableName VERSION AS OF $prevSnapshotId"
            )
            .select(refCol)
            .distinct()

          // Current state: active PKs
          val currentPKs = parentCfg.loadMode.`type` match {
            case LoadMode.SCD2 =>
              val isCurrentCol =
                parentCfg.loadMode.isCurrentColumn.getOrElse("is_current")
              spark
                .sql(
                  s"SELECT $refCol FROM $tableName WHERE $isCurrentCol = true"
                )
                .select(refCol)
                .distinct()
            case _ =>
              spark
                .sql(s"SELECT $refCol FROM $tableName")
                .select(refCol)
                .distinct()
          }

          // PKs that were removed = in previous but not in current
          val removed = previousPKs.join(currentPKs, Seq(refCol), "left_anti")
          Some(removed)
        } catch {
          case e: Exception =>
            logger.warn(
              s"Failed to perform time travel on ${parentCfg.name}: ${e.getMessage}"
            )
            None
        }
    }
  }

  /** Resolves orphans based on the FK's onOrphan action.
    */
  private def resolveOrphans(
      childFlow: FlowConfig,
      fk: ForeignKeyConfig,
      removedParentKeys: DataFrame,
      removedCount: Long,
      removedKeysByFlow: mutable.Map[String, DataFrame]
  ): Option[OrphanReport] = {
    val childTableName = tableManager.resolveTableName(childFlow)
    val fkCol = fk.column
    val refCol = fk.references.column

    // Find orphaned child records
    val childTable = spark.sql(s"SELECT * FROM $childTableName")
    val orphans = childTable.join(
      removedParentKeys.withColumnRenamed(refCol, fkCol),
      Seq(fkCol),
      "inner"
    )
    val orphanCount = orphans.count()

    if (orphanCount == 0) return None

    fk.onOrphan match {
      case OrphanAction.Warn =>
        logger.warn(
          s"Orphan detection: ${childFlow.name}.${fk.displayName} has $orphanCount " +
            s"orphaned records (${removedCount} parent keys removed from ${fk.references.flow})"
        )
        // Warn does NOT propagate cascade
        Some(
          OrphanReport(
            flowName = childFlow.name,
            fkName = fk.displayName,
            parentFlowName = fk.references.flow,
            orphanCount = orphanCount,
            removedParentKeyCount = removedCount,
            actionTaken = "warn"
          )
        )

      case OrphanAction.Delete =>
        logger.info(
          s"Orphan detection: deleting $orphanCount orphaned records " +
            s"from ${childFlow.name} (FK: ${fk.displayName})"
        )

        // Collect orphaned child PKs for cascade before deleting
        val childPkCols = childFlow.validation.primaryKey
        val deletedChildKeys =
          if (childPkCols.nonEmpty) {
            Some(orphans.select(childPkCols.map(orphans(_)): _*).distinct())
          } else None

        // Delete orphaned records from child table
        val removedPksView = s"_orphan_removed_pks_${childFlow.name}_${fk.column}"
        removedParentKeys
          .withColumnRenamed(refCol, fkCol)
          .createOrReplaceTempView(removedPksView)

        try {
          spark.sql(
            s"DELETE FROM $childTableName WHERE $fkCol IN (SELECT $fkCol FROM $removedPksView)"
          )
        } finally {
          spark.catalog.dropTempView(removedPksView)
        }

        // Propagate cascade: save deleted child PKs for next level
        deletedChildKeys.foreach { dck =>
          removedKeysByFlow(childFlow.name) = dck
        }

        Some(
          OrphanReport(
            flowName = childFlow.name,
            fkName = fk.displayName,
            parentFlowName = fk.references.flow,
            orphanCount = orphanCount,
            removedParentKeyCount = removedCount,
            actionTaken = "delete",
            deletedChildKeyCount = orphanCount
          )
        )

      case OrphanAction.Ignore =>
        // Should not reach here (filtered earlier), but handle gracefully
        None
    }
  }
}
