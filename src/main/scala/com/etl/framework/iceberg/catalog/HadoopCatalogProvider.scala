package com.etl.framework.iceberg.catalog

import com.etl.framework.config.IcebergConfig
import org.apache.spark.sql.SparkSession

/** Configures the Spark catalog for Hadoop-based Iceberg (local filesystem or HDFS; S3 needs a lock manager). */
class HadoopCatalogProvider extends CatalogProvider {

  private val icebergExtensions =
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

  override def catalogType: String = "hadoop"

  override def configureCatalog(
      spark: SparkSession,
      config: IcebergConfig
  ): Unit = {
    val registeredExtensions =
      spark.conf.getOption("spark.sql.extensions").getOrElse("")
    if (!registeredExtensions.contains(icebergExtensions)) {
      throw new IllegalStateException(
        s"Iceberg SQL extensions not registered. " +
          s"Add 'spark.sql.extensions=$icebergExtensions' before creating the SparkSession: " +
          s"via SparkSession.builder().config(...), spark-submit --conf, or cluster-level config."
      )
    }

    val catalogPrefix = s"spark.sql.catalog.${config.catalogName}"
    spark.conf.set(catalogPrefix, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(s"$catalogPrefix.type", "hadoop")
    spark.conf.set(s"$catalogPrefix.warehouse", config.warehouse)

    config.catalogProperties.foreach { case (key, value) =>
      spark.conf.set(s"$catalogPrefix.$key", value)
    }
  }

  override def validateConfig(
      config: IcebergConfig
  ): Either[String, Unit] = {
    val objectStore = config.warehouse.matches("(?i)^s3(a|n)?://.*")
    val lockImpl = config.catalogProperties.get("lock-impl").filter(_.nonEmpty)
    if (config.warehouse.isEmpty) {
      Left("warehouse path is required for hadoop catalog")
    } else if (objectStore && lockImpl.isEmpty) {
      Left("Hadoop catalog on S3 requires a catalog lock-impl (for example DynamoDbLockManager); " +
        "consider GlueCatalog with optimistic locking instead")
    } else if (objectStore && lockImpl.exists(_.endsWith("DynamoDbLockManager")) &&
      !config.catalogProperties.get("lock.table").exists(_.nonEmpty)) {
      Left("DynamoDbLockManager requires catalogProperties.lock.table")
    } else {
      Right(())
    }
  }
}
