package com.etl.framework.example

import com.etl.framework.config._
import com.etl.framework.orchestration.{FlowOrchestrator, IngestionResult}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Example ingestion pipeline demonstrating the ETL framework capabilities.
 *
 * This example includes:
 * - 3 flows: customers (master data), products (delta upsert), orders (with foreign keys)
 * - Various validation rules: regex, domain, range, not_null, primary key, foreign key
 * - Pre and post validation transformations
 * - Different load modes: full, delta upsert, delta append
 * - Partitioning and compression
 *
 * Run this example with:
 *   sbt "runMain com.etl.framework.example.IngestionExample"
 */
object IngestionExample {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    // Initialize Spark
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("ETL Framework - Ingestion Example")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    try {
//      logger.info("=" * 80)
//      logger.info("ETL Framework - Ingestion Example")
//      logger.info("=" * 80)

      // Load configurations
      val configBasePath = "example/config"
      val globalConfig = loadGlobalConfig(configBasePath)
      val domainsConfig = loadDomainsConfig(configBasePath)
      val flowConfigs = loadFlowConfigs(configBasePath)

      // Generate batch ID
      val batchId = generateBatchId(globalConfig)
      //logger.info(s"Starting ingestion with batch ID: $batchId")

      // Execute ingestion
      val orchestrator = FlowOrchestrator(
        globalConfig = globalConfig,
        flowConfigs = flowConfigs,
        domainsConfig = Some(domainsConfig)
      )

      val result = orchestrator.execute()
      result

      // Print results
      //printResults(result)

      // Exit with appropriate code
//      if (result.success) {
//        logger.info("Ingestion completed successfully!")
//        System.exit(0)
//      } else {
//        logger.error(s"Ingestion failed: ${result.error.getOrElse("Unknown error")}")
//        System.exit(1)
//      }

    } catch {
      case e: Exception =>
        logger.error("Fatal error during ingestion", e)
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  /**
   * Loads global configuration from YAML
   */
  private def loadGlobalConfig(basePath: String): GlobalConfig = {
    logger.info("Loading global configuration...")
    val configPath = Paths.get(basePath, "global.yaml").toString
    val loader = new GlobalConfigLoader()
    loader.load(configPath) match {
      case Right(config) => config
      case Left(error) => throw error
    }
  }

  /**
   * Loads domains configuration from YAML
   */
  private def loadDomainsConfig(basePath: String): DomainsConfig = {
    logger.info("Loading domains configuration...")
    val configPath = Paths.get(basePath, "domains.yaml").toString
    val loader = new DomainsConfigLoader()
    loader.load(configPath) match {
      case Right(config) => config
      case Left(error) => throw error
    }
  }

  /**
   * Loads all flow configurations from YAML files
   */
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

  /**
   * Generates batch ID based on global config format
   */
  private def generateBatchId(globalConfig: GlobalConfig): String = {
    val formatter = DateTimeFormatter.ofPattern(globalConfig.processing.batchIdFormat)
    LocalDateTime.now().format(formatter)
  }

  /**
   * Prints ingestion results in a formatted way
   */
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
        val status = if (flowResult.success) "✓" else "✗"
        logger.info(f"$status ${flowResult.flowName}%-20s | " +
          f"Input: ${flowResult.inputRecords}%6d | " +
          f"Valid: ${flowResult.validRecords}%6d | " +
          f"Rejected: ${flowResult.rejectedRecords}%6d | " +
          f"Time: ${flowResult.executionTimeMs}%6d ms")

        if (flowResult.rejectedRecords > 0 && flowResult.rejectionReasons.nonEmpty) {
          flowResult.rejectionReasons.foreach { case (reason, count) =>
            logger.info(f"    └─ $reason: $count records")
          }
        }

        if (!flowResult.success && flowResult.error.isDefined) {
          logger.error(s"    └─ Error: ${flowResult.error.get}")
        }
      }

      logger.info("-" * 80)

      // Summary statistics
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
    logger.info("")

    // Print output locations
    if (result.success) {
      logger.info("Output locations:")
      logger.info(s"  Full:      example/output/full/")
      logger.info(s"  Delta:     example/output/delta/")
      logger.info(s"  Rejected:  example/output/rejected/")
      logger.info(s"  Metadata:  example/output/metadata/")
      logger.info("")
    }
  }
}
