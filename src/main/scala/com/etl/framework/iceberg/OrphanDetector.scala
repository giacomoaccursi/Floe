package com.etl.framework.iceberg

import com.etl.framework.config._
import com.etl.framework.orchestration.ExecutionPlan
import com.etl.framework.orchestration.flow.FlowResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import scala.collection.mutable

case class OrphanReport(
    flowName: String,
    fkName: String,
    parentFlowName: String,
    orphanCount: Long,
    removedParentKeyCount: Long,
    actionTaken: String,
    deletedChildKeyCount: Long = 0,
    cascadeSource: Option[String] = None
)

sealed trait OrphanDetectionResult {
  def reports: Seq[OrphanReport]
}

object OrphanDetectionResult {
  case class Completed(reports: Seq[OrphanReport]) extends OrphanDetectionResult
  case object Skipped extends OrphanDetectionResult { val reports: Seq[OrphanReport] = Seq.empty }
  case class Failed(error: String, reports: Seq[OrphanReport]) extends OrphanDetectionResult
}

/** Detects orphaned child records after a batch by comparing pre-batch and post-batch parent keys via Iceberg time
  * travel. Supports cascade detection across multiple FK levels.
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

  /** Detects orphaned child records caused by parent key removal during this batch. Uses Iceberg time travel to compare
    * pre-batch and post-batch parent keys. Resolves orphans according to each FK's onOrphan action (warn or delete).
    */
  def detectAndResolveOrphans(plan: ExecutionPlan): OrphanDetectionResult = {
    val removedKeysByFlow = mutable.Map[String, DataFrame]()
    val reports = mutable.ArrayBuffer[OrphanReport]()

    try {
      plan.groups.flatMap(_.flows).foreach { flowConfig =>
        flowConfig.validation.foreignKeys.foreach { fk =>
          if (fk.onOrphan != OrphanAction.Ignore) {
            processFK(flowConfig, fk, removedKeysByFlow).foreach(reports += _)
          }
        }
      }

      OrphanDetectionResult.Completed(reports.toSeq)
    } catch {
      case e: Exception =>
        logger.error(s"Orphan detection failed after ${reports.size} reports: ${e.getMessage}")
        OrphanDetectionResult.Failed(e.getMessage, reports.toSeq)
    } finally {
      removedKeysByFlow.values.foreach { df =>
        try { df.unpersist() }
        catch { case _: Exception => }
      }
    }
  }

  /** Processes a single FK relationship: finds removed parent keys and resolves orphans. */
  private def processFK(
      childFlow: FlowConfig,
      fk: ForeignKeyConfig,
      removedKeysByFlow: mutable.Map[String, DataFrame]
  ): Option[OrphanReport] = {
    val parentFlowConfig = flowConfigMap.get(fk.references.flow)
    val parentResult = flowResultMap.get(fk.references.flow)

    (parentFlowConfig, parentResult) match {
      case (Some(parentCfg), Some(parentRes)) =>
        val isCascade = removedKeysByFlow.contains(parentCfg.name)
        findRemovedParentKeys(parentCfg, parentRes, fk, removedKeysByFlow).flatMap { removed =>
          val cached = removed.cache()
          try {
            val removedCount = cached.count()
            if (removedCount == 0) None
            else {
              val cascadeSrc = if (isCascade) Some(parentCfg.name) else None
              resolveOrphans(childFlow, fk, cached, removedCount, removedKeysByFlow, cascadeSrc)
            }
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

  /** Finds parent keys that were removed during this batch, either via time travel or cascade map. */
  private def findRemovedParentKeys(
      parentCfg: FlowConfig,
      parentResult: FlowResult,
      fk: ForeignKeyConfig,
      removedKeysByFlow: mutable.Map[String, DataFrame]
  ): Option[DataFrame] = {
    removedKeysByFlow.get(parentCfg.name) match {
      case Some(cascadedKeys) =>
        val refCols = fk.references.columns
        if (refCols.forall(cascadedKeys.columns.contains)) {
          Some(cascadedKeys.select(refCols.map(col): _*).distinct())
        } else None
      case None =>
        findRemovedKeysViaTimeTravel(parentCfg, parentResult, fk)
    }
  }

  /** Compares pre-batch and post-batch parent table via Iceberg time travel to find removed keys. */
  private def findRemovedKeysViaTimeTravel(
      parentCfg: FlowConfig,
      parentResult: FlowResult,
      fk: ForeignKeyConfig
  ): Option[DataFrame] = {
    val canRemoveRecords = parentCfg.loadMode.`type` match {
      case LoadMode.Full  => true
      case LoadMode.SCD2  => parentCfg.loadMode.detectDeletes
      case LoadMode.Delta => false
    }
    if (!canRemoveRecords) return None

    val parentSnapshotId = parentResult.icebergMetadata.flatMap(_.parentSnapshotId)
    parentSnapshotId match {
      case None =>
        logger.debug(s"No previous snapshot for ${parentCfg.name}, skipping orphan check")
        None
      case Some(prevSnapshotId) =>
        val tableName = tableManager.resolveTableName(parentCfg)
        val refCols = fk.references.columns
        val selectExpr = refCols.map(c => s"`$c`").mkString(", ")

        try {
          val previousPKs = spark
            .sql(s"SELECT $selectExpr FROM $tableName VERSION AS OF $prevSnapshotId")
            .distinct()

          val currentPKs = parentCfg.loadMode.`type` match {
            case LoadMode.SCD2 =>
              val isCurrentCol = parentCfg.loadMode.isCurrentColumn.getOrElse("is_current")
              spark.sql(s"SELECT $selectExpr FROM $tableName WHERE `$isCurrentCol` = true").distinct()
            case _ =>
              spark.sql(s"SELECT $selectExpr FROM $tableName").distinct()
          }

          val removed = previousPKs.join(currentPKs, refCols, "left_anti")
          Some(removed)
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to perform time travel on ${parentCfg.name}: ${e.getMessage}")
            None
        }
    }
  }

  /** Resolves orphaned records: warns or deletes depending on FK config. For delete, saves removed keys in cascade map
    * for downstream grandchild detection.
    */
  private def resolveOrphans(
      childFlow: FlowConfig,
      fk: ForeignKeyConfig,
      removedParentKeys: DataFrame,
      removedCount: Long,
      removedKeysByFlow: mutable.Map[String, DataFrame],
      cascadeSource: Option[String]
  ): Option[OrphanReport] = {
    val childTableName = tableManager.resolveTableName(childFlow)
    val fkCols = fk.columns
    val refCols = fk.references.columns
    val colPairs = fkCols.zip(refCols)

    // Rename ref columns to match child FK columns for join
    val renamedKeys = colPairs.foldLeft(removedParentKeys) { case (df, (fkCol, refCol)) =>
      if (fkCol != refCol) df.withColumnRenamed(refCol, fkCol) else df
    }

    // Select only FK + PK columns from child table instead of SELECT *
    val childPkCols = childFlow.validation.primaryKey
    val childTableCols = spark.table(childTableName).columns.toSet
    val columnsNeeded = (fkCols ++ childPkCols).distinct.filter(childTableCols.contains)
    val selectCols = columnsNeeded.map(c => s"`$c`").mkString(", ")
    val childProjection = spark.sql(s"SELECT $selectCols FROM $childTableName")
    val orphans = childProjection.join(renamedKeys, fkCols, "inner")

    fk.onOrphan match {
      case OrphanAction.Warn =>
        val orphanCount = orphans.count()
        if (orphanCount == 0) return None
        logger.warn(
          s"Orphan detection: ${childFlow.name}.${fk.displayName} has $orphanCount " +
            s"orphaned records ($removedCount parent keys removed from ${fk.references.flow})"
        )
        Some(
          OrphanReport(
            childFlow.name,
            fk.displayName,
            fk.references.flow,
            orphanCount,
            removedCount,
            "warn",
            cascadeSource = cascadeSource
          )
        )

      case OrphanAction.Delete =>
        // Materialize cascade keys BEFORE delete (delete mutates the table, invalidating lazy lineage).
        // Use localCheckpoint to break lineage dependency on the parent removed-keys DataFrame,
        // which will be unpersisted by the caller after this method returns.
        val cascadeCols = columnsNeeded
        val cachedOrphans = orphans.select(cascadeCols.map(col): _*).distinct().localCheckpoint()
        val orphanCount = cachedOrphans.count()
        if (orphanCount == 0) {
          cachedOrphans.unpersist()
          return None
        }

        logger.info(
          s"Orphan detection: deleting $orphanCount orphaned records " +
            s"from ${childFlow.name} (FK: ${fk.displayName})"
        )

        // Build DELETE condition using temp view
        val viewName = s"_orphan_removed_pks_${childFlow.name}_${fkCols.mkString("_")}"
        renamedKeys.createOrReplaceTempView(viewName)
        try {
          val deleteCondition = fkCols
            .map(c => s"$childTableName.`$c` = $viewName.`$c`")
            .mkString(" AND ")
          spark.sql(
            s"DELETE FROM $childTableName WHERE EXISTS (SELECT 1 FROM $viewName WHERE $deleteCondition)"
          )
        } finally {
          spark.catalog.dropTempView(viewName)
        }

        // Save cascade keys for downstream grandchild detection
        if (columnsNeeded.nonEmpty) {
          removedKeysByFlow.get(childFlow.name) match {
            case Some(existing) =>
              val merged = existing
                .unionByName(cachedOrphans, allowMissingColumns = true)
                .distinct()
                .cache()
              merged.count()
              existing.unpersist()
              cachedOrphans.unpersist()
              removedKeysByFlow(childFlow.name) = merged
            case None =>
              removedKeysByFlow(childFlow.name) = cachedOrphans
          }
        } else {
          cachedOrphans.unpersist()
        }

        Some(
          OrphanReport(
            childFlow.name,
            fk.displayName,
            fk.references.flow,
            orphanCount,
            removedCount,
            "delete",
            orphanCount,
            cascadeSource
          )
        )

      case OrphanAction.Ignore => None
    }
  }
}
