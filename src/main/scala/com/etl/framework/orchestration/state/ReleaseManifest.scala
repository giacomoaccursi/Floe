package com.etl.framework.orchestration.state

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.iceberg.IcebergTableManager
import com.etl.framework.orchestration.flow.FlowResult
import com.etl.framework.pipeline.DerivedTableResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant

case class ReleaseTarget(
    targetName: String,
    targetType: String,
    tableName: String,
    snapshotId: Option[Long]
)

case class ReleaseManifest(
    batchId: String,
    pipelineId: String,
    effectiveAt: String,
    targets: Seq[ReleaseTarget]
) {
  def target(name: String): ReleaseTarget =
    targets.find(_.targetName == name).getOrElse(throw new NoSuchElementException(s"Target '$name' is not published"))
}

object ReleaseManifest {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def toJson(manifest: ReleaseManifest): String = Serialization.write(manifest)
  def fromJson(json: String): ReleaseManifest = Serialization.read[ReleaseManifest](json)

  def pipelineId(globalConfig: GlobalConfig, flowConfigs: Seq[FlowConfig], derivedNames: Seq[String]): String = {
    val identity = Seq(
      globalConfig.iceberg.catalogName,
      globalConfig.iceberg.namespace,
      flowConfigs.sortBy(_.name).mkString(","),
      derivedNames.sorted.mkString(",")
    ).mkString("|")
    MessageDigest
      .getInstance("SHA-256")
      .digest(identity.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"$byte%02x")
      .mkString
  }
}

class ReleaseManifestBuilder(globalConfig: GlobalConfig, flowConfigs: Seq[FlowConfig])(implicit spark: SparkSession) {
  private val tableManager = new IcebergTableManager(spark, globalConfig.iceberg)

  def build(
      batchId: String,
      pipelineId: String,
      effectiveAt: Instant,
      flowResults: Seq[FlowResult],
      derivedResults: Seq[DerivedTableResult],
      operations: Seq[OperationRecord]
  ): ReleaseManifest = {
    require(flowResults.forall(_.success), "Cannot publish a release containing failed flows")
    require(derivedResults.forall(_.success), "Cannot publish a release containing failed derived tables")

    val configs = flowConfigs.map(flow => flow.name -> flow).toMap
    val operationSnapshots =
      operations.map(operation => (operation.targetType, operation.targetName) -> operation.snapshotId).toMap
    val flows = flowResults.map { result =>
      val config =
        configs.getOrElse(result.flowName, throw new IllegalArgumentException(s"Unknown flow ${result.flowName}"))
      val tableName = tableManager.resolveTableName(config)
      ReleaseTarget(
        result.flowName,
        "flow",
        tableName,
        operationSnapshots.getOrElse(
          ("flow", result.flowName),
          throw new IllegalStateException(s"Missing published operation for flow ${result.flowName}")
        )
      )
    }
    val derived = derivedResults.map { result =>
      val tableName = globalConfig.iceberg.fullTableName(result.tableName)
      ReleaseTarget(
        result.tableName,
        "derived",
        tableName,
        operationSnapshots.getOrElse(
          ("derived", result.tableName),
          throw new IllegalStateException(s"Missing published operation for derived table ${result.tableName}")
        )
      )
    }
    ReleaseManifest(
      batchId,
      pipelineId,
      effectiveAt.toString,
      (flows ++ derived).sortBy(t => (t.targetType, t.targetName))
    )
  }
}

/** Reads all published targets at the exact snapshots recorded by one release manifest. */
class SnapshotPinnedReader(manifest: ReleaseManifest)(implicit spark: SparkSession) {
  def table(targetName: String): DataFrame = {
    val published = manifest.target(targetName)
    published.snapshotId match {
      case Some(snapshotId) => spark.read.option("snapshot-id", snapshotId).table(published.tableName)
      case None             => spark.table(published.tableName).limit(0)
    }
  }
}
