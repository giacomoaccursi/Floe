package com.etl.framework.orchestration.maintenance

import com.etl.framework.config.IcebergConfig
import com.etl.framework.iceberg.{IcebergMaintenanceRunner, MaintenanceResult, MaintenanceStatus}
import com.etl.framework.orchestration.state.{MaintenanceTaskRecord, RunStatus, RunStore}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/** Claims durable maintenance tasks and executes them outside the ingestion critical path.
  *
  * A failed task remains retryable until `maxAttempts`; it never causes ingestion data to be replayed.
  */
class MaintenanceWorker(
    icebergConfig: IcebergConfig,
    runStore: RunStore,
    maxAttempts: Int = 3,
    executor: Option[String => Unit] = None
)(implicit spark: SparkSession) {
  require(maxAttempts > 0, "maxAttempts must be positive")

  private val logger = LoggerFactory.getLogger(getClass)

  def runPending(limit: Int = Int.MaxValue): Seq[MaintenanceResult] = {
    runStore.initialize()
    val candidates = runStore
      .getMaintenanceTasks()
      .filter(task =>
        (task.status == MaintenanceStatus.Queued || task.status == MaintenanceStatus.Failed) &&
          task.attempts < maxAttempts &&
          runStore
            .getRun(task.batchId)
            .exists(run => run.status == RunStatus.Published || run.status == RunStatus.SucceededWithWarnings)
      )
      .take(limit)

    candidates.flatMap(runTask)
  }

  private def runTask(task: MaintenanceTaskRecord): Option[MaintenanceResult] = {
    val claimed = runStore.transitionMaintenance(
      task.batchId,
      task.targetType,
      task.targetName,
      task.version,
      MaintenanceStatus.Running
    )
    if (!claimed) None
    else {
      val running = currentTask(task)
      try {
        executor match {
          case Some(execute) => execute(task.tableName)
          case None => new IcebergMaintenanceRunner(spark, icebergConfig).run(task.tableName, icebergConfig.maintenance)
        }
        transition(running, MaintenanceStatus.Succeeded, None)
        Some(MaintenanceResult(task.targetName, task.targetType, MaintenanceStatus.Succeeded))
      } catch {
        case error: Exception =>
          logger.error(s"Maintenance failed for ${task.tableName}: ${error.getMessage}", error)
          transition(running, MaintenanceStatus.Failed, Some(error.getMessage))
          markRunWarning(task.batchId, task.targetName, error)
          Some(MaintenanceResult(task.targetName, task.targetType, MaintenanceStatus.Failed, Some(error.getMessage)))
      }
    }
  }

  private def currentTask(task: MaintenanceTaskRecord): MaintenanceTaskRecord =
    runStore
      .getMaintenanceTasks(Some(task.batchId))
      .find(current => current.targetType == task.targetType && current.targetName == task.targetName)
      .getOrElse(throw new IllegalStateException(s"Maintenance task disappeared for ${task.targetName}"))

  private def transition(
      task: MaintenanceTaskRecord,
      status: MaintenanceStatus,
      error: Option[String]
  ): Unit =
    if (
      !runStore.transitionMaintenance(
        task.batchId,
        task.targetType,
        task.targetName,
        task.version,
        status,
        error
      )
    )
      throw new IllegalStateException(s"Concurrent maintenance transition for ${task.targetName}")

  private def markRunWarning(batchId: String, targetName: String, error: Exception): Unit = {
    val run = runStore.getRun(batchId).getOrElse(throw new IllegalStateException(s"Missing batch '$batchId'"))
    if (run.status == RunStatus.Published) {
      val message = s"Maintenance failed for $targetName: ${error.getMessage}"
      if (!runStore.transitionRun(batchId, run.version, RunStatus.SucceededWithWarnings, error = Some(message)))
        logger.warn(s"Could not mark batch $batchId as succeeded with warnings due to a concurrent state change")
    }
  }
}
