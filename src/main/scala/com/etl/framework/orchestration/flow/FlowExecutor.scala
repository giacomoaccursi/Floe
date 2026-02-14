package com.etl.framework.orchestration.flow

import com.etl.framework.config.{
  DomainsConfig,
  FlowConfig,
  GlobalConfig,
  LoadMode
}
import com.etl.framework.core.AdditionalTableInfo
import com.etl.framework.exceptions.InvariantViolationException
import com.etl.framework.iceberg.{IcebergFlowMetadata, IcebergTableManager, IcebergTableWriter, WriteResult}
import com.etl.framework.io.readers.DataReaderFactory
import com.etl.framework.merge.DeltaMergerFactory
import com.etl.framework.util.TimingUtil
import com.etl.framework.validation.ValidationColumns._
import com.etl.framework.validation.{ValidationEngine, ValidationResult}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** Executes a single flow through Read -> Validate -> Transform -> Write
  * With Iceberg: validation on new data only, merge via MERGE INTO during write
  * Without Iceberg: Read -> Merge -> Validate -> Transform -> Write (Parquet fallback)
  */
class FlowExecutor(
    flowConfig: FlowConfig,
    globalConfig: GlobalConfig,
    validatedFlows: Map[String, DataFrame] = Map.empty,
    domainsConfig: Option[DomainsConfig] = None
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  private val additionalTables = mutable.Map[String, AdditionalTableInfo]()

  private val icebergEnabled = globalConfig.iceberg.isDefined

  private val icebergTableWriter: Option[IcebergTableWriter] =
    globalConfig.iceberg.map { icebergConfig =>
      val tableManager = new IcebergTableManager(spark, icebergConfig)
      new IcebergTableWriter(spark, icebergConfig, tableManager)
    }

  private val dataWriter =
    new FlowDataWriter(flowConfig, globalConfig, icebergTableWriter)
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

    val finalResult = result.copy(executionTimeMs = executionTimeMs)

    metadataWriter.writeFlowMetadata(finalResult, batchId)
    logFlowSummary(finalResult)

    finalResult
  }

  /** Core flow execution logic
    */
  private def executeFlow(batchId: String): Try[FlowMetrics] = Try {
    logger.info(
      s"Starting flow ${flowConfig.name} - batchId: $batchId, " +
        s"loadMode: ${flowConfig.loadMode.`type`.name}, iceberg: $icebergEnabled"
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

    if (icebergEnabled) {
      // Iceberg pipeline: validate new data only, merge happens during write
      executeIcebergPipeline(preTransformedData, inputCount, batchId)
    } else {
      // Parquet fallback: merge in-memory, then validate merged data
      executeParquetPipeline(preTransformedData, inputCount, batchId)
    }
  }

  /** Iceberg pipeline: Read -> PreTransform -> Validate (new only) -> PostTransform -> Write (MERGE INTO)
    */
  private def executeIcebergPipeline(
      preTransformedData: DataFrame,
      inputCount: Long,
      batchId: String
  ): FlowMetrics = {
    // Validate new data only (existing data in Iceberg table was already validated)
    val validationResult = TimingUtil.timed(logger, "Validate data") {
      validateData(preTransformedData)
    }
    val validCount = validationResult.valid.count()
    val rejectedCount = validationResult.rejected.map(_.count()).getOrElse(0L)

    logValidationResults(validationResult, inputCount, rejectedCount)

    val postTransformedData = transformer.applyPostValidationTransformation(
      validationResult.valid,
      batchId,
      validatedFlows
    )

    verifyInvariant(inputCount, validCount, rejectedCount)

    val writeResult = writeAllData(postTransformedData, validationResult, batchId, rejectedCount)

    FlowMetrics(
      inputCount = inputCount,
      mergedCount = inputCount,
      validCount = validCount,
      rejectedCount = rejectedCount,
      rejectionReasons = validationResult.rejectionReasons,
      icebergMetadata = writeResult.icebergMetadata
    )
  }

  /** Parquet fallback pipeline: Read -> PreTransform -> Merge -> Validate -> PostTransform -> Write
    */
  private def executeParquetPipeline(
      preTransformedData: DataFrame,
      inputCount: Long,
      batchId: String
  ): FlowMetrics = {
    // Merge with existing data (delta/scd2 mode only)
    val mergedData = applyMerge(preTransformedData)
    val mergedCount = mergedData.count()

    val validationResult = TimingUtil.timed(logger, "Validate data") {
      validateData(mergedData)
    }
    val validCount = validationResult.valid.count()
    val rejectedCount = validationResult.rejected.map(_.count()).getOrElse(0L)

    logValidationResults(validationResult, mergedCount, rejectedCount)

    val postTransformedData = transformer.applyPostValidationTransformation(
      validationResult.valid,
      batchId,
      validatedFlows
    )

    verifyInvariant(inputCount, validCount, rejectedCount)

    val writeResult = writeAllData(postTransformedData, validationResult, batchId, rejectedCount)

    FlowMetrics(
      inputCount = inputCount,
      mergedCount = mergedCount,
      validCount = validCount,
      rejectedCount = rejectedCount,
      rejectionReasons = validationResult.rejectionReasons,
      icebergMetadata = writeResult.icebergMetadata
    )
  }

  protected def readData(): DataFrame = {
    logger.debug(
      s"Creating reader for source type: ${flowConfig.source.`type`.name}"
    )
    val reader =
      DataReaderFactory.create(flowConfig.source, Some(flowConfig.schema))
    reader.read()
  }

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

  private def mergeWithExisting(newData: DataFrame): DataFrame = {
    val existingPath = flowConfig.output.path.getOrElse(
      s"${globalConfig.paths.outputPath}/${flowConfig.name}"
    )

    val existingData = loadExistingData(existingPath)
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

  protected def loadExistingData(path: String): Option[DataFrame] = {
    try {
      Some(spark.read.parquet(path).drop(WARNINGS))
    } catch {
      case _: org.apache.spark.sql.AnalysisException =>
        logger.info(
          s"No existing data found at $path, treating as initial load"
        )
        None
    }
  }

  protected def validateData(data: DataFrame): ValidationResult = {
    logger.debug(s"Starting validation on ${data.count()} records")
    val engine = new ValidationEngine(domainsConfig)
    engine.validate(data, flowConfig, validatedFlows)
  }

  private def logValidationResults(
      validationResult: ValidationResult,
      totalCount: Long,
      rejectedCount: Long
  ): Unit = {
    if (rejectedCount > 0) {
      val rejectionRate = (rejectedCount.toDouble / totalCount * 100)
      logger.warn(
        f"Validation rejected $rejectedCount/$totalCount records ($rejectionRate%.2f%%)"
      )
      validationResult.rejectionReasons.foreach { case (reason, count) =>
        logger.warn(s"  - $reason: $count records")
      }
    }
  }

  private def writeAllData(
      validatedData: DataFrame,
      validationResult: ValidationResult,
      batchId: String,
      rejectedCount: Long
  ): WriteResult = {
    val writeResult = dataWriter.writeValidated(validatedData, batchId)

    if (rejectedCount > 0) {
      dataWriter.writeRejected(validationResult.rejected.get, batchId)
    }

    writeAdditionalTables(batchId)
    writeResult
  }

  private def writeAdditionalTables(batchId: String): Unit = {
    additionalTables.foreach { case (tableName, tableInfo) =>
      val outputPath = tableInfo.outputPath.getOrElse(
        s"${globalConfig.paths.outputPath}/${flowConfig.name}_${tableName}"
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
      metrics.rejectionReasons,
      metrics.icebergMetadata
    )
  }

  private def createFailureResult(
      batchId: String,
      error: Throwable
  ): FlowResult = {
    logger.error(s"Flow ${flowConfig.name} failed: ${error.getMessage}", error)
    FlowResult.failure(flowConfig.name, batchId, error.getMessage)
  }

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

  private def logMergeStats(
      newCount: Long,
      existingCount: Long,
      resultCount: Long
  ): Unit = {
    logger.info(
      s"Merge ${flowConfig.loadMode.`type`.name}: existing $existingCount + new $newCount -> result $resultCount records"
    )
  }
}

private case class FlowMetrics(
    inputCount: Long,
    mergedCount: Long,
    validCount: Long,
    rejectedCount: Long,
    rejectionReasons: Map[String, Long],
    icebergMetadata: Option[IcebergFlowMetadata] = None
)
