package com.etl.framework.orchestration.state

import com.etl.framework.iceberg.MaintenanceStatus
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.DriverManager
import java.time.{Duration, Instant}
import java.util.UUID

class JdbcRunStoreTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var jdbcUrl: String = _
  private var store: JdbcRunStore = _

  override def beforeEach(): Unit = {
    jdbcUrl = s"jdbc:h2:mem:${UUID.randomUUID()};DB_CLOSE_DELAY=-1"
    store = new JdbcRunStore(() => DriverManager.getConnection(jdbcUrl))
    store.initialize()
  }

  "JdbcRunStore" should "apply versioned run transitions atomically" in {
    val run = RunRecord.planned("batch-1", "pipeline-a", Instant.parse("2026-09-23T10:15:30Z"))
    store.createRun(run)

    store.transitionRun("batch-1", expectedVersion = 0L, RunStatus.Running) shouldBe true
    store.transitionRun("batch-1", expectedVersion = 0L, RunStatus.Failed) shouldBe false

    val persisted = store.getRun("batch-1").get
    persisted.status shouldBe RunStatus.Running
    persisted.version shouldBe 1L
    persisted.effectiveAt shouldBe run.effectiveAt
  }

  it should "issue fencing tokens and reject a competing live lease" in {
    val now = Instant.parse("2026-09-23T10:15:30Z")
    store.createRun(RunRecord.planned("batch-lease", "pipeline-a", now))

    val first = store.acquireLease("batch-lease", "owner-a", now, Duration.ofMinutes(5)).get
    first.fencingToken shouldBe 1L
    store.acquireLease("batch-lease", "owner-b", now.plusSeconds(30), Duration.ofMinutes(5)) shouldBe None

    val second = store.acquireLease("batch-lease", "owner-b", now.plusSeconds(301), Duration.ofMinutes(5)).get
    second.fencingToken shouldBe 2L
    second.owner shouldBe "owner-b"
  }

  it should "persist operation state and reject stale updates" in {
    val now = Instant.parse("2026-09-23T10:15:30Z")
    store.createRun(RunRecord.planned("batch-ops", "pipeline-a", now))
    store.createOperation(
      OperationRecord.pending("batch-ops", "op-1", "customers", "flow")
    )

    store.transitionOperation(
      "batch-ops",
      "op-1",
      expectedVersion = 0L,
      OperationStatus.Committed,
      snapshotId = Some(42L)
    ) shouldBe true
    store.transitionOperation(
      "batch-ops",
      "op-1",
      expectedVersion = 0L,
      OperationStatus.FailedPreWrite
    ) shouldBe false

    val operation = store.getOperations("batch-ops").head
    operation.status shouldBe OperationStatus.Committed
    operation.snapshotId shouldBe Some(42L)
    operation.version shouldBe 1L
  }

  it should "persist and atomically claim idempotent maintenance tasks" in {
    val now = Instant.parse("2026-09-23T10:15:30Z")
    store.createRun(RunRecord.planned("batch-maintenance", "pipeline-a", now))
    val task = MaintenanceTaskRecord.queued(
      "batch-maintenance",
      "customers",
      "flow",
      "floe.default.customers"
    )

    store.enqueueMaintenance(task)
    store.enqueueMaintenance(task)
    store.getMaintenanceTasks(Some("batch-maintenance")) should have size 1
    store.transitionMaintenance(
      "batch-maintenance",
      "flow",
      "customers",
      expectedVersion = 0L,
      MaintenanceStatus.Running
    ) shouldBe true
    store.transitionMaintenance(
      "batch-maintenance",
      "flow",
      "customers",
      expectedVersion = 0L,
      MaintenanceStatus.Running
    ) shouldBe false

    val claimed = store.getMaintenanceTasks(Some("batch-maintenance")).head
    claimed.status shouldBe MaintenanceStatus.Running
    claimed.attempts shouldBe 1
    claimed.version shouldBe 1L
  }
}
