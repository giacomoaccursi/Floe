package com.etl.framework.orchestration.flow

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig}
import com.etl.framework.core.AdditionalTableInfo
import com.etl.framework.iceberg.{IcebergFlowMetadata, IcebergTableManager, IcebergTableWriter, WriteResult}
import com.etl.framework.io.readers.DataReaderFactory
import com.etl.framework.util.TimingUtil
import com.etl.framework.validation.ValidationColumns._
import com.etl.framework.validation.{ValidationEngine, ValidationResult}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** Executes a single flow through Read -> Validate -> Transform -> Write (Iceberg)
  */
class FlowExecutor(
    flowConfig: FlowConfig,
    globalConfig: GlobalConfig,
    validatedFlows: Map[String, DataFrame] = Map.empty,
    domainsConfig: Option[DomainsConfig] = None
)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  private val additionalTables = mutable.Map[String, AdditionalTableInfo]()

  private val icebergTableWriter: IcebergTableWriter = {
    val tableManager = new IcebergTableManager(spark, globalConfig.iceberg)
    new IcebergTableWriter(spark, globalConfig.iceberg, tableManager)
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
        s"loadMode: ${flowConfig.loadMode.`type`.name}"
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

    val cachedInput = preTransformedData.cache()
    try {

      val inputCount = cachedInput.count()

      // 3. Validate new data only, merge happens during Iceberg write
      val validationResult = TimingUtil.timed(logger, "Validate data") {
        validateData(cachedInput)
      }
      val rejectedCount = validationResult.rejectionReasons.values.sum
      val validCount = inputCount - rejectedCount

      logValidationResults(validationResult, inputCount, rejectedCount)

      // 4. Apply post-validation transformations
      val postTransformedData = transformer.applyPostValidationTransformation(
        validationResult.valid,
        batchId,
        validatedFlows
      )

      // 5. Write to Iceberg
      val writeResult = writeAllData(postTransformedData, validationResult, batchId, rejectedCount)

      FlowMetrics(
        inputCount = inputCount,
        mergedCount = inputCount,
        validCount = validCount,
        rejectedCount = rejectedCount,
        rejectionReasons = validationResult.rejectionReasons,
        icebergMetadata = writeResult.icebergMetadata
      )

    } finally {
      cachedInput.unpersist()
    }
  }

  protected def readData(): DataFrame = {
    logger.debug(
      s"Creating reader for source type: ${flowConfig.source.`type`.name}"
    )
    val reader =
      DataReaderFactory.create(flowConfig.source, Some(flowConfig.schema))
    reader.read()
  }

  protected def validateData(data: DataFrame): ValidationResult = {
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

    validationResult.warned.foreach { warnedDf =>
      dataWriter.writeWarnings(warnedDf, batchId)
    }

    writeAdditionalTables(batchId)
    writeResult
  }

  private def writeAdditionalTables(batchId: String): Unit = {
    additionalTables.foreach { case (tableName, tableInfo) =>
      val outputPath = tableInfo.outputPath.getOrElse(
        s"${globalConfig.paths.outputPath}/${flowConfig.name}_${tableName}"
      )

      // Cache before write: the write action populates the cache, count() reuses it
      // instead of re-executing the full DAG transformation plan
      val cachedData = tableInfo.data.cache()
      try {
        dataWriter.writeAdditionalTable(
          tableName,
          cachedData,
          Some(outputPath),
          tableInfo.dagMetadata
        )

        val recordCount = cachedData.count()
        metadataWriter.writeAdditionalTableMetadata(
          tableName,
          cachedData,
          recordCount,
          outputPath,
          tableInfo.dagMetadata,
          batchId
        )
      } finally {
        cachedData.unpersist()
      }
    }
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

}

private case class FlowMetrics(
    inputCount: Long,
    mergedCount: Long,
    validCount: Long,
    rejectedCount: Long,
    rejectionReasons: Map[String, Long],
    icebergMetadata: Option[IcebergFlowMetadata] = None
)
