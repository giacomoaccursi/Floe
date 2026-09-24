package com.etl.framework.orchestration.state

import java.time.{Duration, Instant}
import com.etl.framework.iceberg.MaintenanceStatus

trait RunStore {
  def initialize(): Unit
  def createRun(run: RunRecord): Unit
  def getRun(batchId: String): Option[RunRecord]
  def transitionRun(
      batchId: String,
      expectedVersion: Long,
      status: RunStatus,
      releaseManifest: Option[String] = None,
      error: Option[String] = None
  ): Boolean
  def acquireLease(batchId: String, owner: String, now: Instant, ttl: Duration): Option[Lease]
  def renewLease(batchId: String, owner: String, fencingToken: Long, now: Instant, ttl: Duration): Boolean
  def releaseLease(batchId: String, owner: String, fencingToken: Long): Boolean
  def createOperation(operation: OperationRecord): Unit
  def getOperations(batchId: String): Seq[OperationRecord]
  def transitionOperation(
      batchId: String,
      operationId: String,
      expectedVersion: Long,
      status: OperationStatus,
      snapshotId: Option[Long] = None,
      error: Option[String] = None
  ): Boolean
  def enqueueMaintenance(task: MaintenanceTaskRecord): Unit
  def getMaintenanceTasks(batchId: Option[String] = None): Seq[MaintenanceTaskRecord]
  def transitionMaintenance(
      batchId: String,
      targetType: String,
      targetName: String,
      expectedVersion: Long,
      status: MaintenanceStatus,
      error: Option[String] = None
  ): Boolean
}

/** Process-local store intended for tests and local development. It provides the same CAS and fencing semantics but no
  * durability across JVM restarts.
  */
class InMemoryRunStore extends RunStore {
  private var runs = Map.empty[String, RunRecord]
  private var operations = Map.empty[(String, String), OperationRecord]
  private var maintenanceTasks = Map.empty[(String, String, String), MaintenanceTaskRecord]

  override def initialize(): Unit = ()

  override def createRun(run: RunRecord): Unit = synchronized {
    require(!runs.contains(run.batchId), s"Run '${run.batchId}' already exists")
    runs += run.batchId -> run
  }

  override def getRun(batchId: String): Option[RunRecord] = synchronized(runs.get(batchId))

  override def transitionRun(
      batchId: String,
      expectedVersion: Long,
      status: RunStatus,
      releaseManifest: Option[String],
      error: Option[String]
  ): Boolean = synchronized {
    runs.get(batchId) match {
      case Some(current) if current.version == expectedVersion =>
        runs += batchId -> current.copy(
          status = status,
          version = current.version + 1,
          releaseManifest = releaseManifest.orElse(current.releaseManifest),
          error = error,
          updatedAt = Instant.now()
        )
        true
      case _ => false
    }
  }

  override def acquireLease(batchId: String, owner: String, now: Instant, ttl: Duration): Option[Lease] = synchronized {
    runs.get(batchId).flatMap { current =>
      if (current.lease.exists(lease => lease.owner != owner && lease.expiresAt.isAfter(now))) None
      else {
        val lease = Lease(owner, now.plus(ttl), current.lease.map(_.fencingToken + 1).getOrElse(1L))
        runs += batchId -> current.copy(lease = Some(lease), version = current.version + 1, updatedAt = now)
        Some(lease)
      }
    }
  }

  override def releaseLease(batchId: String, owner: String, fencingToken: Long): Boolean = synchronized {
    runs.get(batchId) match {
      case Some(current) if current.lease.exists(l => l.owner == owner && l.fencingToken == fencingToken) =>
        runs += batchId -> current.copy(lease = None, version = current.version + 1, updatedAt = Instant.now())
        true
      case _ => false
    }
  }

  override def renewLease(
      batchId: String,
      owner: String,
      fencingToken: Long,
      now: Instant,
      ttl: Duration
  ): Boolean = synchronized {
    runs.get(batchId) match {
      case Some(current) if current.lease.exists(l => l.owner == owner && l.fencingToken == fencingToken) =>
        val renewed = current.lease.get.copy(expiresAt = now.plus(ttl))
        runs += batchId -> current.copy(lease = Some(renewed), updatedAt = now)
        true
      case _ => false
    }
  }

  override def createOperation(operation: OperationRecord): Unit = synchronized {
    val key = operation.batchId -> operation.operationId
    require(!operations.contains(key), s"Operation '${operation.operationId}' already exists")
    operations += key -> operation
  }

  override def getOperations(batchId: String): Seq[OperationRecord] = synchronized {
    operations.values.filter(_.batchId == batchId).toSeq.sortBy(_.targetName)
  }

  override def transitionOperation(
      batchId: String,
      operationId: String,
      expectedVersion: Long,
      status: OperationStatus,
      snapshotId: Option[Long],
      error: Option[String]
  ): Boolean = synchronized {
    val key = batchId -> operationId
    operations.get(key) match {
      case Some(current) if current.version == expectedVersion =>
        operations += key -> current.copy(
          status = status,
          version = current.version + 1,
          snapshotId = snapshotId.orElse(current.snapshotId),
          error = error,
          updatedAt = Instant.now()
        )
        true
      case _ => false
    }
  }

  override def enqueueMaintenance(task: MaintenanceTaskRecord): Unit = synchronized {
    val key = (task.batchId, task.targetType, task.targetName)
    maintenanceTasks.get(key) match {
      case Some(existing) =>
        require(
          existing.tableName == task.tableName,
          s"Maintenance task '${key.productIterator.mkString("/")}' changed table"
        )
      case None => maintenanceTasks += key -> task
    }
  }

  override def getMaintenanceTasks(batchId: Option[String]): Seq[MaintenanceTaskRecord] = synchronized {
    maintenanceTasks.values
      .filter(task => batchId.forall(_ == task.batchId))
      .toSeq
      .sortBy(task => (task.batchId, task.targetType, task.targetName))
  }

  override def transitionMaintenance(
      batchId: String,
      targetType: String,
      targetName: String,
      expectedVersion: Long,
      status: MaintenanceStatus,
      error: Option[String]
  ): Boolean = synchronized {
    val key = (batchId, targetType, targetName)
    maintenanceTasks.get(key) match {
      case Some(current) if current.version == expectedVersion =>
        maintenanceTasks += key -> current.copy(
          status = status,
          attempts = current.attempts + (if (status == MaintenanceStatus.Running) 1 else 0),
          version = current.version + 1,
          error = error,
          updatedAt = Instant.now()
        )
        true
      case _ => false
    }
  }
}
