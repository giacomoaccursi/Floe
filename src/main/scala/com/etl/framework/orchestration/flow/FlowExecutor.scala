package com.etl.framework.orchestration.flow

import com.etl.framework.config.{
  DomainsConfig,
  FlowConfig,
  GlobalConfig,
  LoadMode
}
import com.etl.framework.core.AdditionalTableInfo
import com.etl.framework.exceptions.InvariantViolationException
import com.etl.framework.io.readers.DataReaderFactory
import com.etl.framework.merge.DeltaMergerFactory
import com.etl.framework.util.TimingUtil
import com.etl.framework.validation.ValidationColumns._
import com.etl.framework.validation.{ValidationEngine, ValidationResult}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** Executes a single flow through Read → Merge → Validate → Transform → Write
  * Focused on orchestrating the flow execution pipeline
  */
class FlowExecutor(
    flowConfig: FlowConfig,
    globalConfig: GlobalConfig,
    validatedFlows: Map[String, DataFrame] = Map.empty,
    domainsConfig: Option[DomainsConfig] = None
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  // Storage for additional tables created during transformations
  private val additionalTables = mutable.Map[String, AdditionalTableInfo]()

  // Specialized components
  private val dataWriter = new FlowDataWriter(flowConfig, globalConfig)
  private val metadataWriter = new FlowMetadataWriter(flowConfig, globalConfig)
  private val transformer = new FlowTransformer(flowConfig, additionalTables)

  /** Executes the complete flow
    */
  def execute(batchId: String): FlowResult = {
    val (result, executionTimeMs) =
      TimingUtil.timedWithDuration(logger, s"Execute flow ${flowConfig.name}") {
        executeFlow(batchId) match {
          case Success(metrics) => createSuccessResult(batchId, metrics)
          case Failure(error)   => createFailureResult(batchId, error)
        }
      }

    // Update result with actual execution time
    val finalResult = result.copy(executionTimeMs = executionTimeMs)

    // Write metadata and log summary
    metadataWriter.writeFlowMetadata(finalResult, batchId)
    logFlowSummary(finalResult)

    finalResult
  }

  /** Core flow execution logic - returns Try[FlowMetrics]
    */
  private def executeFlow(batchId: String): Try[FlowMetrics] = Try {
    logger.info(
      s"Starting flow ${flowConfig.name} - batchId: $batchId, loadMode: ${flowConfig.loadMode.`type`.name}"
    )

    // 1. Read data from source
    val rawData = TimingUtil.timed(
      logger,
      s"Read ${flowConfig.source.`type`.name} from ${flowConfig.source.path}"
    ) {
      readData()
    }

    // 2. Apply pre-validation transformations
    val preTransformedData =
      transformer.applyPreValidationTransformation(rawData, batchId)
    val inputCount = preTransformedData.count()

    // 3. Merge with existing data (delta mode only)
    val mergedData = applyMerge(preTransformedData)
    val mergedCount = mergedData.count()

    // 4. Validate data
    val validationResult = TimingUtil.timed(logger, "Validate data") {
      validateData(mergedData)
    }
    val validCount = validationResult.valid.count()
    val rejectedCount = validationResult.rejected.map(_.count()).getOrElse(0L)

    // Log validation results
    logValidationResults(validationResult, mergedCount, rejectedCount)

    // 5. Apply post-validation transformations
    val postTransformedData = transformer.applyPostValidationTransformation(
      validationResult.valid,
      batchId,
      validatedFlows
    )

    // 6. Verify invariant: input = valid + rejected
    verifyInvariant(inputCount, validCount, rejectedCount)

    // 7. Write all data
    writeAllData(postTransformedData, validationResult, batchId, rejectedCount)

    // 8. Return metrics
    FlowMetrics(
      inputCount = inputCount,
      mergedCount = mergedCount,
      validCount = validCount,
      rejectedCount = rejectedCount,
      rejectionReasons = validationResult.rejectionReasons
    )
  }

  /** Reads data from source
    */
  private def readData(): DataFrame = {
    logger.debug(
      s"Creating reader for source type: ${flowConfig.source.`type`.name}"
    )
    val reader =
      DataReaderFactory.create(flowConfig.source, Some(flowConfig.schema))
    reader.read()
  }

  /** Applies merge logic based on load mode
    */
  private def applyMerge(data: DataFrame): DataFrame = {
    flowConfig.loadMode.`type` match {
      case LoadMode.Full =>
        logger.debug("Full load mode, skipping merge")
        data
      case _ =>
        TimingUtil.timed(logger, "Merge with existing data") {
          mergeWithExisting(data)
        }
    }
  }

  /** Merges new data with existing data
    */
  private def mergeWithExisting(newData: DataFrame): DataFrame = {
    val outputPath = flowConfig.output.path.getOrElse(
      s"${globalConfig.paths.validatedPath}/${flowConfig.name}"
    )

    val existingData = loadExistingData(outputPath)
    val merger = DeltaMergerFactory.create(
      flowConfig.loadMode,
      flowConfig.validation.primaryKey
    )
    val result = merger.merge(newData, existingData)

    logMergeStats(
      newData.count(),
      existingData.map(_.count()).getOrElse(0L),
      result.count()
    )

    result
  }

  /** Loads existing data if available
    */
  private def loadExistingData(path: String): Option[DataFrame] = {
    try {
      Some(spark.read.parquet(path).drop(WARNINGS))
    } catch {
      case _: Exception =>
        logger.info(
          s"No existing data found at $path, treating as initial load"
        )
        None
    }
  }

  /** Validates data
    */
  private def validateData(data: DataFrame): ValidationResult = {
    logger.debug(s"Starting validation on ${data.count()} records")
    val engine = new ValidationEngine(domainsConfig)
    engine.validate(data, flowConfig, validatedFlows)
  }

  /** Logs validation results
    */
  private def logValidationResults(
      validationResult: ValidationResult,
      mergedCount: Long,
      rejectedCount: Long
  ): Unit = {
    if (rejectedCount > 0) {
      val rejectionRate = (rejectedCount.toDouble / mergedCount * 100)
      logger.warn(
        f"Validation rejected $rejectedCount/$mergedCount records ($rejectionRate%.2f%%)"
      )
      validationResult.rejectionReasons.foreach { case (reason, count) =>
        logger.warn(s"  - $reason: $count records")
      }
    }
  }

  /** Writes all data (validated, rejected, additional tables)
    */
  private def writeAllData(
      validatedData: DataFrame,
      validationResult: ValidationResult,
      batchId: String,
      rejectedCount: Long
  ): Unit = {
    // Write validated data
    dataWriter.writeValidated(validatedData, batchId)

    // Write rejected data if any
    if (rejectedCount > 0) {
      dataWriter.writeRejected(validationResult.rejected.get, batchId)
    }

    // Write additional tables
    writeAdditionalTables(batchId)
  }

  /** Writes additional tables created during transformations
    */
  private def writeAdditionalTables(batchId: String): Unit = {
    additionalTables.foreach { case (tableName, tableInfo) =>
      val outputPath = tableInfo.outputPath.getOrElse(
        s"${globalConfig.paths.validatedPath}/${flowConfig.name}_${tableName}"
      )

      dataWriter.writeAdditionalTable(
        tableName,
        tableInfo.data,
        Some(outputPath),
        tableInfo.dagMetadata
      )

      metadataWriter.writeAdditionalTableMetadata(
        tableName,
        tableInfo.data,
        outputPath,
        tableInfo.dagMetadata,
        batchId
      )
    }
  }

  /** Verifies invariant: input = valid + rejected
    */
  private def verifyInvariant(
      input: Long,
      valid: Long,
      rejected: Long
  ): Unit = {
    if (input != valid + rejected) {
      val message = s"Invariant violation in flow ${flowConfig.name}: " +
        s"input_count ($input) != valid_count ($valid) + rejected_count ($rejected)"
      logger.error(message)
      throw InvariantViolationException(
        flowName = flowConfig.name,
        inputCount = input,
        validCount = valid,
        rejectedCount = rejected
      )
    }
    logger.debug(s"Invariant verified: $input = $valid + $rejected")
  }

  /** Creates a success result from metrics
    */
  private def createSuccessResult(
      batchId: String,
      metrics: FlowMetrics
  ): FlowResult = {
    FlowResult.success(
      flowConfig.name,
      batchId,
      metrics.inputCount,
      metrics.mergedCount,
      metrics.validCount,
      metrics.rejectedCount,
      metrics.rejectionReasons
    )
  }

  /** Creates a failure result from exception
    */
  private def createFailureResult(
      batchId: String,
      error: Throwable
  ): FlowResult = {
    logger.error(s"Flow ${flowConfig.name} failed: ${error.getMessage}", error)
    FlowResult.failure(flowConfig.name, batchId, error.getMessage)
  }

  /** Logs flow summary
    */
  private def logFlowSummary(result: FlowResult): Unit = {
    val rejectionRate = if (result.inputRecords > 0) {
      (result.rejectedRecords.toDouble / result.inputRecords * 100)
    } else 0.0

    logger.info(
      f"Flow ${flowConfig.name} completed in ${result.executionTimeMs}ms - " +
        f"input: ${result.inputRecords}, valid: ${result.validRecords}, " +
        f"rejected: ${result.rejectedRecords} ($rejectionRate%.2f%%)"
    )
  }

  /** Logs merge statistics
    */
  private def logMergeStats(
      newCount: Long,
      existingCount: Long,
      resultCount: Long
  ): Unit = {
    logger.info(
      s"Merge ${flowConfig.loadMode.`type`.name}: existing $existingCount + new $newCount → result $resultCount records"
    )
  }
}

/** Holds metrics collected during flow execution
  */
private case class FlowMetrics(
    inputCount: Long,
    mergedCount: Long,
    validCount: Long,
    rejectedCount: Long,
    rejectionReasons: Map[String, Long]
)
