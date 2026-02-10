package com.etl.framework.example

import com.etl.framework.config._
import com.etl.framework.orchestration.IngestionResult
import com.etl.framework.pipeline.IngestionPipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import java.nio.file.Paths

/**
 * Example ingestion pipeline demonstrating the ETL framework capabilities.
 *
 * This example includes:
 * - 3 flows: customers (full), products (delta upsert), orders (delta append with FK)
 * - Validation rules: regex, domain, range, not_null, primary key, foreign key
 * - Pre-validation transformation: trim + uppercase on customers
 * - Post-validation transformation: derived column on products
 * - Different load modes: full, delta upsert, delta append
 * - Partitioning and compression
 *
 * Run with:
 *   sbt "runMain com.etl.framework.example.IngestionExample"
 */
object IngestionExample {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("ETL Framework - Ingestion Example")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    try {
      logger.info("=" * 80)
      logger.info("ETL Framework - Ingestion Example")
      logger.info("=" * 80)

      val configBasePath = "example/config"

      val globalConfig = loadGlobalConfig(configBasePath)
      val domainsConfig = loadDomainsConfig(configBasePath)
      val flowConfigs = loadFlowConfigs(configBasePath)

      val pipeline = IngestionPipeline.builder()
        .withGlobalConfig(globalConfig)
        .withFlowConfigs(flowConfigs)
        .withDomainsConfig(domainsConfig)
        // Pre-validation: trim whitespace and uppercase country codes
        .withPreValidationTransformation("customers", { ctx =>
          ctx.currentData
            .withColumn("first_name", trim(col("first_name")))
            .withColumn("last_name", trim(col("last_name")))
            .withColumn("email", lower(trim(col("email"))))
            .withColumn("country", upper(trim(col("country"))))
        })
        // Post-validation: add price tier classification
        .withPostValidationTransformation("products", { ctx =>
          ctx.currentData.withColumn("price_tier",
            when(col("price") < 50, "BUDGET")
              .when(col("price") < 200, "MID_RANGE")
              .when(col("price") < 1000, "PREMIUM")
              .otherwise("LUXURY")
          )
        })
        .build()

      val result = pipeline.execute()

      printResults(result)

      if (result.success) {
        logger.info("Ingestion completed successfully!")
      } else {
        logger.error(s"Ingestion failed: ${result.error.getOrElse("Unknown error")}")
        System.exit(1)
      }

    } catch {
      case e: Exception =>
        logger.error("Fatal error during ingestion", e)
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  private def loadGlobalConfig(basePath: String): GlobalConfig = {
    logger.info("Loading global configuration...")
    val configPath = Paths.get(basePath, "global.yaml").toString
    val loader = new GlobalConfigLoader()
    loader.load(configPath) match {
      case Right(config) => config
      case Left(error) => throw error
    }
  }

  private def loadDomainsConfig(basePath: String): DomainsConfig = {
    logger.info("Loading domains configuration...")
    val configPath = Paths.get(basePath, "domains.yaml").toString
    val loader = new DomainsConfigLoader()
    loader.load(configPath) match {
      case Right(config) => config
      case Left(error) => throw error
    }
  }

  private def loadFlowConfigs(basePath: String): Seq[FlowConfig] = {
    logger.info("Loading flow configurations...")
    val flowNames = Seq("customers", "products", "orders")
    val loader = new FlowConfigLoader()
    val flowConfigs = flowNames.map { flowName =>
      val configPath = Paths.get(basePath, s"$flowName.yaml").toString
      logger.info(s"  - Loading flow: $flowName")
      loader.load(configPath) match {
        case Right(config) => config
        case Left(error) => throw error
      }
    }
    logger.info(s"Loaded ${flowConfigs.size} flow configurations")
    flowConfigs
  }

  private def printResults(result: IngestionResult): Unit = {
    logger.info("")
    logger.info("=" * 80)
    logger.info("INGESTION RESULTS")
    logger.info("=" * 80)
    logger.info(f"Batch ID:        ${result.batchId}")
    logger.info(f"Status:          ${if (result.success) "SUCCESS" else "FAILED"}")
    logger.info("")

    if (result.flowResults.nonEmpty) {
      logger.info("Flow Results:")
      logger.info("-" * 80)

      result.flowResults.foreach { flowResult =>
        val status = if (flowResult.success) "OK" else "KO"
        logger.info(f"[$status] ${flowResult.flowName}%-20s | " +
          f"Input: ${flowResult.inputRecords}%6d | " +
          f"Valid: ${flowResult.validRecords}%6d | " +
          f"Rejected: ${flowResult.rejectedRecords}%6d | " +
          f"Time: ${flowResult.executionTimeMs}%6d ms")

        if (flowResult.rejectedRecords > 0 && flowResult.rejectionReasons.nonEmpty) {
          flowResult.rejectionReasons.foreach { case (reason, count) =>
            logger.info(f"       $reason: $count records")
          }
        }

        if (!flowResult.success && flowResult.error.isDefined) {
          logger.error(s"       Error: ${flowResult.error.get}")
        }
      }

      logger.info("-" * 80)

      val totalInput = result.flowResults.map(_.inputRecords).sum
      val totalValid = result.flowResults.map(_.validRecords).sum
      val totalRejected = result.flowResults.map(_.rejectedRecords).sum
      val rejectionRate = if (totalInput > 0) (totalRejected.toDouble / totalInput * 100) else 0.0

      logger.info("")
      logger.info("Summary:")
      logger.info(f"  Total Input Records:    $totalInput%,d")
      logger.info(f"  Total Valid Records:    $totalValid%,d")
      logger.info(f"  Total Rejected Records: $totalRejected%,d")
      logger.info(f"  Rejection Rate:         $rejectionRate%.2f%%")
    }

    logger.info("=" * 80)

    if (result.success) {
      logger.info("")
      logger.info("Output locations:")
      logger.info(s"  Full:      example/output/full/")
      logger.info(s"  Delta:     example/output/delta/")
      logger.info(s"  Rejected:  example/output/rejected/")
      logger.info(s"  Metadata:  example/output/metadata/")
    }
  }
}
