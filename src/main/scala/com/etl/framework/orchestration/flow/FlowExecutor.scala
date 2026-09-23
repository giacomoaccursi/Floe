package com.etl.framework.orchestration.flow

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig}
import com.etl.framework.iceberg.{IcebergFlowMetadata, IcebergTableManager, IcebergTableWriter, WriteResult}
import com.etl.framework.io.readers.DataReaderFactory
import com.etl.framework.orchestration.RejectionThresholdPolicy
import com.etl.framework.exceptions.MaxRejectionRateExceededException
import com.etl.framework.util.TimingUtil
import com.etl.framework.validation.{ValidationEngine, ValidationResult, Validator}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/** Executes a single flow through Read -> Validate -> Transform -> Write (Iceberg)
  */
class FlowExecutor(
    flowConfig: FlowConfig,
    globalConfig: GlobalConfig,
    validatedFlows: Map[String, DataFrame] = Map.empty,
    domainsConfig: Option[DomainsConfig] = None,
    customValidators: Map[String, () => Validator] = Map.empty,
    customReaders: Map[String, DataReaderFactory.ReaderFactory] = Map.empty
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  private lazy val icebergTableWriter: IcebergTableWriter = {
    val tableManager = new IcebergTableManager(spark, globalConfig.iceberg)
    new IcebergTableWriter(spark, globalConfig.iceberg, tableManager)
  }

  private val dataWriter =
    new FlowDataWriter(flowConfig, globalConfig, icebergTableWriter)
  private val metadataWriter = new FlowMetadataWriter(flowConfig, globalConfig)
  private val transformer = new FlowTransformer(flowConfig)
  private var writeAttempted = false

  /** Executes the complete flow
    */
  def execute(batchId: String): FlowResult = {
    writeAttempted = false
    val (result, executionTimeMs) =
      TimingUtil.timedWithDuration(logger, s"Execute flow ${flowConfig.name}") {
        executeFlow(batchId) match {
          case Success(metrics) if metrics.thresholdError.isDefined =>
            createThresholdFailureResult(batchId, metrics)
          case Success(metrics) => createSuccessResult(batchId, metrics)
          case Failure(error)   => createFailureResult(batchId, error)
        }
      }

    val finalResult = result.copy(executionTimeMs = executionTimeMs, writeAttempted = writeAttempted)

    val reportedResult = try {
      metadataWriter.writeFlowMetadata(finalResult, batchId)
      finalResult
    } catch {
      case e: Exception =>
        logger.error(s"Flow metadata write failed for ${flowConfig.name}: ${e.getMessage}", e)
        finalResult.copy(
          success = false,
          retryable = false,
          error = Some(s"Flow metadata write failed: ${e.getMessage}")
        )
    }
    logFlowSummary(reportedResult)

    reportedResult
  }

  /** Core flow execution logic
    */
  private def executeFlow(batchId: String): Try[FlowMetrics] = Try {
    logger.info(
      s"Starting flow ${flowConfig.name} - batchId: $batchId, " +
        s"loadMode: ${flowConfig.loadMode.`type`.name}"
    )

    // 1. Read data from source
    val rawData = TimingUtil.timed(
      logger,
      s"Read ${flowConfig.source.`type`.name} from ${flowConfig.source.path}"
    ) {
      readData()
    }

    // 1b. Apply column renames from sourceColumn mappings
    val renamedData = applyColumnRenames(rawData)

    // 2. Apply pre-validation transformations
    val preTransformedData =
      transformer.applyPreValidationTransformation(renamedData, batchId)

    val cachedInput = preTransformedData.cache()
    try {

      val inputCount = cachedInput.count()
      flowConfig.minInputRecords.foreach { minimum =>
        require(
          inputCount >= minimum,
          s"Flow ${flowConfig.name} input contains $inputCount records, below configured minInputRecords=$minimum"
        )
      }

      // 3. Validate new data only, merge happens during Iceberg write
      val validationResult = TimingUtil.timed(logger, "Validate data") {
        validateData(cachedInput)
      }
      val rejectedCount = validationResult.rejectionReasons.values.sum
      val validCount = inputCount - rejectedCount
      val rejectionRate = if (inputCount > 0) rejectedCount.toDouble / inputCount else 0.0

      logValidationResults(validationResult, inputCount, rejectedCount)

      if (RejectionThresholdPolicy.exceeded(rejectionRate, rejectedCount, flowConfig, globalConfig)) {
        // Keep the diagnostic side outputs, but never mutate the target table.
        validationResult.rejected.foreach(dataWriter.writeRejected(_, batchId))
        validationResult.warned.foreach(dataWriter.writeWarnings(_, batchId))
        val maxRate = RejectionThresholdPolicy.threshold(flowConfig, globalConfig).get
        FlowMetrics(
          inputCount = inputCount,
          mergedCount = 0L,
          validCount = validCount,
          rejectedCount = rejectedCount,
          rejectionReasons = validationResult.rejectionReasons,
          thresholdError = Some(
            MaxRejectionRateExceededException(
              flowConfig.name,
              rejectionRate,
              maxRate,
              rejectedCount,
              inputCount
            ).getMessage
          )
        )
      } else {
        // 4. Apply post-validation transformations
        val postTransformedData = transformer.applyPostValidationTransformation(
          validationResult.valid,
          batchId,
          validatedFlows
        )

        val cachedOutput = postTransformedData.cache()
        try {
          val outputCount = cachedOutput.count()

          // 5. Write to Iceberg
          val writeResult = writeAllData(cachedOutput, validationResult, batchId, rejectedCount)

          FlowMetrics(
            inputCount = inputCount,
            mergedCount = outputCount,
            validCount = validCount,
            rejectedCount = rejectedCount,
            rejectionReasons = validationResult.rejectionReasons,
            icebergMetadata = writeResult.icebergMetadata
          )
        } finally {
          cachedOutput.unpersist()
        }
      }

    } finally {
      cachedInput.unpersist()
    }
  }

  protected def readData(): DataFrame = {
    logger.debug(
      s"Creating reader for source type: ${flowConfig.source.`type`.name}"
    )
    val reader =
      DataReaderFactory.create(flowConfig.source, Some(flowConfig.schema), customReaders)
    reader.read()
  }

  /** Applies sourceColumn renames defined in schema config (e.g. sourceColumn: "old_name" → name: "new_name"). */
  private def applyColumnRenames(df: DataFrame): DataFrame = {
    val renames = flowConfig.schema.columns.flatMap(column => column.sourceColumn.map(_ -> column.name)).toMap
    val finalNames = df.columns.map(name => renames.getOrElse(name, name))
    require(
      finalNames.distinct.length == finalNames.length,
      s"Flow ${flowConfig.name} sourceColumn mappings produce duplicate columns: " +
        finalNames.groupBy(identity).collect { case (name, occurrences) if occurrences.length > 1 => name }.mkString(", ")
    )
    df.select(df.columns.map(name => col(name).as(renames.getOrElse(name, name))): _*)
  }

  protected def validateData(data: DataFrame): ValidationResult = {
    val engine = new ValidationEngine(domainsConfig, customValidators)
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

  /** Writes validated data to Iceberg, rejected records to Parquet, and warnings if present. */
  private def writeAllData(
      validatedData: DataFrame,
      validationResult: ValidationResult,
      batchId: String,
      rejectedCount: Long
  ): WriteResult = {
    // A failed target write may already have committed; never replay the whole flow automatically.
    writeAttempted = true
    val writeResult = dataWriter.writeValidated(validatedData, batchId)

    if (rejectedCount > 0) {
      validationResult.rejected.foreach(rejDf => dataWriter.writeRejected(rejDf, batchId))
    }

    validationResult.warned.foreach { warnedDf =>
      dataWriter.writeWarnings(warnedDf, batchId)
    }

    writeResult
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

  private def createThresholdFailureResult(batchId: String, metrics: FlowMetrics): FlowResult =
    FlowResult(
      flowName = flowConfig.name,
      batchId = batchId,
      success = false,
      inputRecords = metrics.inputCount,
      mergedRecords = 0L,
      validRecords = metrics.validCount,
      rejectedRecords = metrics.rejectedCount,
      rejectionRate = if (metrics.inputCount > 0) metrics.rejectedCount.toDouble / metrics.inputCount else 0.0,
      rejectionReasons = metrics.rejectionReasons,
      error = metrics.thresholdError
    )

  private def createFailureResult(
      batchId: String,
      error: Throwable
  ): FlowResult = {
    logger.error(s"Flow ${flowConfig.name} failed: ${error.getMessage}", error)
    FlowResult.failure(flowConfig.name, batchId, error.getMessage).copy(
      writeAttempted = writeAttempted,
      retryable = !writeAttempted
    )
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

}

private case class FlowMetrics(
    inputCount: Long,
    mergedCount: Long,
    validCount: Long,
    rejectedCount: Long,
    rejectionReasons: Map[String, Long],
    icebergMetadata: Option[IcebergFlowMetadata] = None,
    thresholdError: Option[String] = None
)
