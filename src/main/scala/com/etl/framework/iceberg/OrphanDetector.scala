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
    deletedChildKeyCount: Long = 0
)

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

  def detectAndResolveOrphans(plan: ExecutionPlan): Seq[OrphanReport] = {
    val removedKeysByFlow = mutable.Map[String, DataFrame]()
    val reports = mutable.ArrayBuffer[OrphanReport]()

    plan.groups.flatMap(_.flows).foreach { flowConfig =>
      flowConfig.validation.foreignKeys.foreach { fk =>
        if (fk.onOrphan != OrphanAction.Ignore) {
          processFK(flowConfig, fk, removedKeysByFlow).foreach(reports += _)
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
        findRemovedParentKeys(parentCfg, parentRes, fk, removedKeysByFlow).flatMap { removed =>
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
          Some(cascadedKeys.select(refCols.map(col): _*))
        } else None
      case None =>
        findRemovedKeysViaTimeTravel(parentCfg, parentResult, fk)
    }
  }

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
        val selectExpr = refCols.mkString(", ")

        try {
          val previousPKs = spark
            .sql(s"SELECT $selectExpr FROM $tableName VERSION AS OF $prevSnapshotId")
            .distinct()

          val currentPKs = parentCfg.loadMode.`type` match {
            case LoadMode.SCD2 =>
              val isCurrentCol = parentCfg.loadMode.isCurrentColumn.getOrElse("is_current")
              spark.sql(s"SELECT $selectExpr FROM $tableName WHERE $isCurrentCol = true").distinct()
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

  private def resolveOrphans(
      childFlow: FlowConfig,
      fk: ForeignKeyConfig,
      removedParentKeys: DataFrame,
      removedCount: Long,
      removedKeysByFlow: mutable.Map[String, DataFrame]
  ): Option[OrphanReport] = {
    val childTableName = tableManager.resolveTableName(childFlow)
    val fkCols = fk.columns
    val refCols = fk.references.columns
    val colPairs = fkCols.zip(refCols)

    // Rename ref columns to match child FK columns for join
    var renamedKeys = removedParentKeys
    colPairs.foreach { case (fkCol, refCol) =>
      if (fkCol != refCol) renamedKeys = renamedKeys.withColumnRenamed(refCol, fkCol)
    }

    val childTable = spark.sql(s"SELECT * FROM $childTableName")
    val orphans = childTable.join(renamedKeys, fkCols, "inner")
    val orphanCount = orphans.count()

    if (orphanCount == 0) return None

    fk.onOrphan match {
      case OrphanAction.Warn =>
        logger.warn(
          s"Orphan detection: ${childFlow.name}.${fk.displayName} has $orphanCount " +
            s"orphaned records ($removedCount parent keys removed from ${fk.references.flow})"
        )
        Some(OrphanReport(childFlow.name, fk.displayName, fk.references.flow, orphanCount, removedCount, "warn"))

      case OrphanAction.Delete =>
        logger.info(
          s"Orphan detection: deleting $orphanCount orphaned records " +
            s"from ${childFlow.name} (FK: ${fk.displayName})"
        )

        val childPkCols = childFlow.validation.primaryKey
        val deletedChildKeys =
          if (childPkCols.nonEmpty) Some(orphans.select(childPkCols.map(col): _*).distinct())
          else None

        // Build multi-column DELETE condition
        val viewName = s"_orphan_removed_pks_${childFlow.name}_${fkCols.mkString("_")}"
        renamedKeys.createOrReplaceTempView(viewName)

        try {
          val deleteCondition = fkCols
            .map(c => s"$childTableName.$c = $viewName.$c")
            .mkString(" AND ")
          spark.sql(
            s"DELETE FROM $childTableName WHERE EXISTS (SELECT 1 FROM $viewName WHERE $deleteCondition)"
          )
        } finally {
          spark.catalog.dropTempView(viewName)
        }

        deletedChildKeys.foreach(dck => removedKeysByFlow(childFlow.name) = dck)

        Some(
          OrphanReport(
            childFlow.name,
            fk.displayName,
            fk.references.flow,
            orphanCount,
            removedCount,
            "delete",
            orphanCount
          )
        )

      case OrphanAction.Ignore => None
    }
  }
}
