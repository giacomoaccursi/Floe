package com.etl.framework.orchestration.state

import com.etl.framework.iceberg.MaintenanceStatus
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.time.{Duration, Instant}

/** JDBC-backed durable run coordinator. SQL is intentionally limited to portable primitives supported by PostgreSQL and
  * H2: primary keys, conditional UPDATE and transactions.
  */
class JdbcRunStore(connectionFactory: () => Connection, tablePrefix: String = "floe_") extends RunStore {
  require(tablePrefix.matches("[A-Za-z][A-Za-z0-9_]*"), "tablePrefix must be a simple SQL identifier prefix")

  private val runsTable = s"${tablePrefix}runs"
  private val operationsTable = s"${tablePrefix}operations"
  private val maintenanceTable = s"${tablePrefix}maintenance_tasks"

  override def initialize(): Unit = withConnection { connection =>
    val statement = connection.createStatement()
    try {
      statement.executeUpdate(
        s"""CREATE TABLE IF NOT EXISTS $runsTable (
           |  batch_id VARCHAR(255) PRIMARY KEY,
           |  pipeline_id VARCHAR(255) NOT NULL,
           |  effective_at_ms BIGINT NOT NULL,
           |  status VARCHAR(64) NOT NULL,
           |  version BIGINT NOT NULL,
           |  lease_owner VARCHAR(255),
           |  lease_until_ms BIGINT,
           |  fencing_token BIGINT NOT NULL,
           |  replay_of VARCHAR(255),
           |  release_manifest TEXT,
           |  error_message TEXT,
           |  created_at_ms BIGINT NOT NULL,
           |  updated_at_ms BIGINT NOT NULL
           |)""".stripMargin
      )
      statement.executeUpdate(
        s"""CREATE TABLE IF NOT EXISTS $maintenanceTable (
           |  batch_id VARCHAR(255) NOT NULL,
           |  target_type VARCHAR(64) NOT NULL,
           |  target_name VARCHAR(512) NOT NULL,
           |  table_name VARCHAR(1024) NOT NULL,
           |  status VARCHAR(64) NOT NULL,
           |  attempts INTEGER NOT NULL,
           |  version BIGINT NOT NULL,
           |  error_message TEXT,
           |  updated_at_ms BIGINT NOT NULL,
           |  PRIMARY KEY (batch_id, target_type, target_name),
           |  FOREIGN KEY (batch_id) REFERENCES $runsTable(batch_id)
           |)""".stripMargin
      )
      statement.executeUpdate(
        s"""CREATE TABLE IF NOT EXISTS $operationsTable (
           |  batch_id VARCHAR(255) NOT NULL,
           |  operation_id VARCHAR(255) NOT NULL,
           |  target_name VARCHAR(512) NOT NULL,
           |  target_type VARCHAR(64) NOT NULL,
           |  status VARCHAR(64) NOT NULL,
           |  version BIGINT NOT NULL,
           |  snapshot_id BIGINT,
           |  input_fingerprint VARCHAR(128),
           |  error_message TEXT,
           |  updated_at_ms BIGINT NOT NULL,
           |  PRIMARY KEY (batch_id, operation_id),
           |  FOREIGN KEY (batch_id) REFERENCES $runsTable(batch_id)
           |)""".stripMargin
      )
      ()
    } finally statement.close()
  }

  override def createRun(run: RunRecord): Unit = withConnection { connection =>
    val sql =
      s"INSERT INTO $runsTable " +
        "(batch_id, pipeline_id, effective_at_ms, status, version, lease_owner, lease_until_ms, fencing_token, " +
        "replay_of, release_manifest, error_message, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    withPrepared(connection, sql) { statement =>
      statement.setString(1, run.batchId)
      statement.setString(2, run.pipelineId)
      statement.setLong(3, run.effectiveAt.toEpochMilli)
      statement.setString(4, run.status.name)
      statement.setLong(5, run.version)
      setOptionalString(statement, 6, run.lease.map(_.owner))
      setOptionalLong(statement, 7, run.lease.map(_.expiresAt.toEpochMilli))
      statement.setLong(8, run.lease.map(_.fencingToken).getOrElse(0L))
      setOptionalString(statement, 9, run.replayOf)
      setOptionalString(statement, 10, run.releaseManifest)
      setOptionalString(statement, 11, run.error)
      statement.setLong(12, run.createdAt.toEpochMilli)
      statement.setLong(13, run.updatedAt.toEpochMilli)
      statement.executeUpdate()
      ()
    }
  }

  override def getRun(batchId: String): Option[RunRecord] = withConnection { connection =>
    withPrepared(connection, s"SELECT * FROM $runsTable WHERE batch_id = ?") { statement =>
      statement.setString(1, batchId)
      val results = statement.executeQuery()
      try if (results.next()) Some(readRun(results)) else None
      finally results.close()
    }
  }

  override def transitionRun(
      batchId: String,
      expectedVersion: Long,
      status: RunStatus,
      releaseManifest: Option[String],
      error: Option[String]
  ): Boolean = withConnection { connection =>
    val sql =
      s"UPDATE $runsTable SET status = ?, version = version + 1, release_manifest = COALESCE(?, release_manifest), " +
        "error_message = ?, updated_at_ms = ? WHERE batch_id = ? AND version = ?"
    withPrepared(connection, sql) { statement =>
      statement.setString(1, status.name)
      setOptionalString(statement, 2, releaseManifest)
      setOptionalString(statement, 3, error)
      statement.setLong(4, Instant.now().toEpochMilli)
      statement.setString(5, batchId)
      statement.setLong(6, expectedVersion)
      statement.executeUpdate() == 1
    }
  }

  override def acquireLease(batchId: String, owner: String, now: Instant, ttl: Duration): Option[Lease] =
    withTransaction { connection =>
      val sql =
        s"UPDATE $runsTable SET lease_owner = ?, lease_until_ms = ?, fencing_token = fencing_token + 1, " +
          "version = version + 1, updated_at_ms = ? WHERE batch_id = ? AND " +
          "(lease_owner IS NULL OR lease_until_ms <= ? OR lease_owner = ?)"
      val updated = withPrepared(connection, sql) { statement =>
        statement.setString(1, owner)
        statement.setLong(2, now.plus(ttl).toEpochMilli)
        statement.setLong(3, now.toEpochMilli)
        statement.setString(4, batchId)
        statement.setLong(5, now.toEpochMilli)
        statement.setString(6, owner)
        statement.executeUpdate()
      }
      if (updated != 1) None
      else {
        withPrepared(
          connection,
          s"SELECT lease_until_ms, fencing_token FROM $runsTable WHERE batch_id = ? AND lease_owner = ?"
        ) { statement =>
          statement.setString(1, batchId)
          statement.setString(2, owner)
          val results = statement.executeQuery()
          try {
            if (!results.next()) throw new SQLException(s"Lease disappeared for run '$batchId'")
            Some(Lease(owner, Instant.ofEpochMilli(results.getLong(1)), results.getLong(2)))
          } finally results.close()
        }
      }
    }

  override def releaseLease(batchId: String, owner: String, fencingToken: Long): Boolean = withConnection {
    connection =>
      val sql =
        s"UPDATE $runsTable SET lease_owner = NULL, lease_until_ms = NULL, version = version + 1, updated_at_ms = ? " +
          "WHERE batch_id = ? AND lease_owner = ? AND fencing_token = ?"
      withPrepared(connection, sql) { statement =>
        statement.setLong(1, Instant.now().toEpochMilli)
        statement.setString(2, batchId)
        statement.setString(3, owner)
        statement.setLong(4, fencingToken)
        statement.executeUpdate() == 1
      }
  }

  override def renewLease(
      batchId: String,
      owner: String,
      fencingToken: Long,
      now: Instant,
      ttl: Duration
  ): Boolean = withConnection { connection =>
    val sql =
      s"UPDATE $runsTable SET lease_until_ms = ?, updated_at_ms = ? " +
        "WHERE batch_id = ? AND lease_owner = ? AND fencing_token = ?"
    withPrepared(connection, sql) { statement =>
      statement.setLong(1, now.plus(ttl).toEpochMilli)
      statement.setLong(2, now.toEpochMilli)
      statement.setString(3, batchId)
      statement.setString(4, owner)
      statement.setLong(5, fencingToken)
      statement.executeUpdate() == 1
    }
  }

  override def createOperation(operation: OperationRecord): Unit = withConnection { connection =>
    val sql =
      s"INSERT INTO $operationsTable " +
        "(batch_id, operation_id, target_name, target_type, status, version, snapshot_id, input_fingerprint, error_message, updated_at_ms) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    withPrepared(connection, sql) { statement =>
      statement.setString(1, operation.batchId)
      statement.setString(2, operation.operationId)
      statement.setString(3, operation.targetName)
      statement.setString(4, operation.targetType)
      statement.setString(5, operation.status.name)
      statement.setLong(6, operation.version)
      setOptionalLong(statement, 7, operation.snapshotId)
      setOptionalString(statement, 8, operation.inputFingerprint)
      setOptionalString(statement, 9, operation.error)
      statement.setLong(10, operation.updatedAt.toEpochMilli)
      statement.executeUpdate()
      ()
    }
  }

  override def getOperations(batchId: String): Seq[OperationRecord] = withConnection { connection =>
    withPrepared(
      connection,
      s"SELECT * FROM $operationsTable WHERE batch_id = ? ORDER BY target_name, operation_id"
    ) { statement =>
      statement.setString(1, batchId)
      val results = statement.executeQuery()
      val builder = Seq.newBuilder[OperationRecord]
      try while (results.next()) builder += readOperation(results)
      finally results.close()
      builder.result()
    }
  }

  override def transitionOperation(
      batchId: String,
      operationId: String,
      expectedVersion: Long,
      status: OperationStatus,
      snapshotId: Option[Long],
      error: Option[String]
  ): Boolean = withConnection { connection =>
    val sql =
      s"UPDATE $operationsTable SET status = ?, version = version + 1, snapshot_id = COALESCE(?, snapshot_id), " +
        "error_message = ?, updated_at_ms = ? WHERE batch_id = ? AND operation_id = ? AND version = ?"
    withPrepared(connection, sql) { statement =>
      statement.setString(1, status.name)
      setOptionalLong(statement, 2, snapshotId)
      setOptionalString(statement, 3, error)
      statement.setLong(4, Instant.now().toEpochMilli)
      statement.setString(5, batchId)
      statement.setString(6, operationId)
      statement.setLong(7, expectedVersion)
      statement.executeUpdate() == 1
    }
  }

  override def enqueueMaintenance(task: MaintenanceTaskRecord): Unit = withTransaction { connection =>
    val existing = withPrepared(
      connection,
      s"SELECT table_name FROM $maintenanceTable WHERE batch_id = ? AND target_type = ? AND target_name = ?"
    ) { statement =>
      statement.setString(1, task.batchId)
      statement.setString(2, task.targetType)
      statement.setString(3, task.targetName)
      val results = statement.executeQuery()
      try if (results.next()) Some(results.getString(1)) else None
      finally results.close()
    }
    existing match {
      case Some(tableName) =>
        require(tableName == task.tableName, s"Maintenance task for '${task.targetName}' changed table")
      case None =>
        val sql =
          s"INSERT INTO $maintenanceTable " +
            "(batch_id, target_type, target_name, table_name, status, attempts, version, error_message, updated_at_ms) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        withPrepared(connection, sql) { statement =>
          statement.setString(1, task.batchId)
          statement.setString(2, task.targetType)
          statement.setString(3, task.targetName)
          statement.setString(4, task.tableName)
          statement.setString(5, task.status.name)
          statement.setInt(6, task.attempts)
          statement.setLong(7, task.version)
          setOptionalString(statement, 8, task.error)
          statement.setLong(9, task.updatedAt.toEpochMilli)
          statement.executeUpdate()
          ()
        }
    }
  }

  override def getMaintenanceTasks(batchId: Option[String]): Seq[MaintenanceTaskRecord] = withConnection { connection =>
    val (sql, bind) = batchId match {
      case Some(_) =>
        (s"SELECT * FROM $maintenanceTable WHERE batch_id = ? ORDER BY target_type, target_name", true)
      case None =>
        (s"SELECT * FROM $maintenanceTable ORDER BY batch_id, target_type, target_name", false)
    }
    withPrepared(connection, sql) { statement =>
      if (bind) statement.setString(1, batchId.get)
      val results = statement.executeQuery()
      val builder = Seq.newBuilder[MaintenanceTaskRecord]
      try while (results.next()) builder += readMaintenanceTask(results)
      finally results.close()
      builder.result()
    }
  }

  override def transitionMaintenance(
      batchId: String,
      targetType: String,
      targetName: String,
      expectedVersion: Long,
      status: MaintenanceStatus,
      error: Option[String]
  ): Boolean = withConnection { connection =>
    val sql =
      s"UPDATE $maintenanceTable SET status = ?, attempts = attempts + ?, version = version + 1, " +
        "error_message = ?, updated_at_ms = ? WHERE batch_id = ? AND target_type = ? AND target_name = ? AND version = ?"
    withPrepared(connection, sql) { statement =>
      statement.setString(1, status.name)
      statement.setInt(2, if (status == MaintenanceStatus.Running) 1 else 0)
      setOptionalString(statement, 3, error)
      statement.setLong(4, Instant.now().toEpochMilli)
      statement.setString(5, batchId)
      statement.setString(6, targetType)
      statement.setString(7, targetName)
      statement.setLong(8, expectedVersion)
      statement.executeUpdate() == 1
    }
  }

  private def readRun(results: ResultSet): RunRecord = {
    val owner = Option(results.getString("lease_owner"))
    val lease = owner.map(o =>
      Lease(o, Instant.ofEpochMilli(results.getLong("lease_until_ms")), results.getLong("fencing_token"))
    )
    RunRecord(
      batchId = results.getString("batch_id"),
      pipelineId = results.getString("pipeline_id"),
      effectiveAt = Instant.ofEpochMilli(results.getLong("effective_at_ms")),
      status = RunStatus.fromName(results.getString("status")),
      version = results.getLong("version"),
      lease = lease,
      replayOf = Option(results.getString("replay_of")),
      releaseManifest = Option(results.getString("release_manifest")),
      error = Option(results.getString("error_message")),
      createdAt = Instant.ofEpochMilli(results.getLong("created_at_ms")),
      updatedAt = Instant.ofEpochMilli(results.getLong("updated_at_ms"))
    )
  }

  private def readOperation(results: ResultSet): OperationRecord = {
    val snapshot = results.getLong("snapshot_id")
    val snapshotId = if (results.wasNull()) None else Some(snapshot)
    OperationRecord(
      batchId = results.getString("batch_id"),
      operationId = results.getString("operation_id"),
      targetName = results.getString("target_name"),
      targetType = results.getString("target_type"),
      status = OperationStatus.fromName(results.getString("status")),
      version = results.getLong("version"),
      snapshotId = snapshotId,
      inputFingerprint = Option(results.getString("input_fingerprint")),
      error = Option(results.getString("error_message")),
      updatedAt = Instant.ofEpochMilli(results.getLong("updated_at_ms"))
    )
  }

  private def readMaintenanceTask(results: ResultSet): MaintenanceTaskRecord =
    MaintenanceTaskRecord(
      batchId = results.getString("batch_id"),
      targetName = results.getString("target_name"),
      targetType = results.getString("target_type"),
      tableName = results.getString("table_name"),
      status = MaintenanceStatus.fromName(results.getString("status")),
      attempts = results.getInt("attempts"),
      version = results.getLong("version"),
      error = Option(results.getString("error_message")),
      updatedAt = Instant.ofEpochMilli(results.getLong("updated_at_ms"))
    )

  private def setOptionalString(statement: PreparedStatement, index: Int, value: Option[String]): Unit =
    value match {
      case Some(actual) => statement.setString(index, actual)
      case None         => statement.setNull(index, java.sql.Types.VARCHAR)
    }

  private def setOptionalLong(statement: PreparedStatement, index: Int, value: Option[Long]): Unit =
    value match {
      case Some(actual) => statement.setLong(index, actual)
      case None         => statement.setNull(index, java.sql.Types.BIGINT)
    }

  private def withPrepared[A](connection: Connection, sql: String)(use: PreparedStatement => A): A = {
    val statement = connection.prepareStatement(sql)
    try use(statement)
    finally statement.close()
  }

  private def withConnection[A](use: Connection => A): A = {
    val connection = connectionFactory()
    try use(connection)
    finally connection.close()
  }

  private def withTransaction[A](use: Connection => A): A = withConnection { connection =>
    val previousAutoCommit = connection.getAutoCommit
    connection.setAutoCommit(false)
    try {
      val result = use(connection)
      connection.commit()
      result
    } catch {
      case error: Throwable =>
        connection.rollback()
        throw error
    } finally connection.setAutoCommit(previousAutoCommit)
  }
}
