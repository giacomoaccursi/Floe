package com.etl.framework.orchestration.flow

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig}
import com.etl.framework.core.AdditionalTableMetadata
import com.etl.framework.io.readers.DataReaderFactory
import com.etl.framework.merge.DeltaMergerFactory
import com.etl.framework.util.TimingUtil
import com.etl.framework.validation.{ValidationEngine, ValidationResult}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Executes a single flow through Read → Merge → Validate → Transform → Write
 * Coordinates the flow execution using specialized components
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

  /**
   * Executes the complete flow
   */
  def execute(batchId: String): FlowResult = {
    val startTime = System.nanoTime()
    
    try {
      logger.info(s"Starting flow ${flowConfig.name} - batchId: $batchId, loadMode: ${flowConfig.loadMode.`type`}")
      
      // 1. Read data from source
      val rawData = TimingUtil.timed(logger, s"Read ${flowConfig.source.`type`} from ${flowConfig.source.path}") {
        readData()
      }

      // 2. Apply pre-validation transformations
      val preTransformedData = transformer.applyPreValidation(rawData, batchId)


      val inputCount = preTransformedData.count()

      // 3. Merge with existing data (delta mode)
      val mergedData = TimingUtil.timed(logger, "Merge with existing data") {
        mergeWithExisting(preTransformedData)
      }

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
      val postTransformedData = transformer.applyPostValidation(
        validationResult.valid,
        batchId,
        validatedFlows
      )
      
      // 6. Verify invariant: input = valid + rejected
      verifyInvariant(inputCount, validCount, rejectedCount)
      
      // 7. Write all data
      writeAllData(postTransformedData, validationResult, batchId, rejectedCount)
      
      // 8. Create and write result
      val executionTimeMs = (System.nanoTime() - startTime) / 1000000
      val result = createFlowResult(
        batchId, inputCount, mergedCount, validCount, rejectedCount,
        executionTimeMs, validationResult.rejectionReasons
      )
      
      metadataWriter.writeFlowMetadata(result, batchId)
      logFlowSummary(result)
      
      result
      
    } catch {
      case e: InvariantViolationException =>
        handleFailure(batchId, startTime, e, isInvariantViolation = true)
      
      case e: Exception =>
        handleFailure(batchId, startTime, e, isInvariantViolation = false)
    }
  }
  
  /**
   * Reads data from source
   */
  private def readData(): DataFrame = {
    logger.debug(s"Creating reader for source type: ${flowConfig.source.`type`}")
    val reader = DataReaderFactory.create(flowConfig.source)
    reader.read()
  }
  
  /**
   * Merges with existing data (delta mode)
   */
  private def mergeWithExisting(newData: DataFrame): DataFrame = {
    if (flowConfig.loadMode.`type` == "full") {
      logger.debug("Full load mode, skipping merge")
      return newData
    }
    
    val outputPath = flowConfig.output.path.getOrElse(
      s"${globalConfig.paths.validatedPath}/${flowConfig.name}"
    )
    
    val existingData = loadExistingData(outputPath)
    val newCount = newData.count()
    val existingCount = existingData.map(_.count()).getOrElse(0L)
    
    val merger = DeltaMergerFactory.create(flowConfig.loadMode)
    val result = merger.merge(newData, existingData)
    val resultCount = result.count()
    
    logger.info(s"Merge ${flowConfig.loadMode.`type`}: existing $existingCount + new $newCount → result $resultCount records")
    
    result
  }
  
  /**
   * Loads existing data if available
   */
  private def loadExistingData(path: String): Option[DataFrame] = {
    try {
      Some(spark.read.parquet(path))
    } catch {
      case _: Exception =>
        logger.info(s"No existing data found at $path, treating as initial load")
        None
    }
  }
  
  /**
   * Validates data
   */
  private def validateData(data: DataFrame): ValidationResult = {
    logger.debug(s"Starting validation on ${data.count()} records")
    val engine = new ValidationEngine(domainsConfig)
    engine.validate(data, flowConfig, validatedFlows)
  }
  
  /**
   * Logs validation results
   */
  private def logValidationResults(
    validationResult: ValidationResult,
    mergedCount: Long,
    rejectedCount: Long
  ): Unit = {
    if (rejectedCount > 0) {
      val rejectionRate = (rejectedCount.toDouble / mergedCount * 100)
      logger.warn(f"Validation rejected $rejectedCount/$mergedCount records ($rejectionRate%.2f%%)")
      validationResult.rejectionReasons.foreach { case (reason, count) =>
        logger.warn(s"  - $reason: $count records")
      }
    }
  }
  
  /**
   * Writes all data (validated, rejected, additional tables)
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
  
  /**
   * Writes additional tables created during transformations
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
  
  /**
   * Creates flow result
   */
  private def createFlowResult(
    batchId: String,
    inputCount: Long,
    mergedCount: Long,
    validCount: Long,
    rejectedCount: Long,
    executionTimeMs: Long,
    rejectionReasons: Map[String, Long]
  ): FlowResult = {
    FlowResult(
      flowName = flowConfig.name,
      batchId = batchId,
      success = true,
      inputRecords = inputCount,
      mergedRecords = mergedCount,
      validRecords = validCount,
      rejectedRecords = rejectedCount,
      rejectionRate = if (inputCount > 0) rejectedCount.toDouble / inputCount else 0.0,
      executionTimeMs = executionTimeMs,
      rejectionReasons = rejectionReasons
    )
  }
  
  /**
   * Logs flow summary
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
  
  /**
   * Handles flow execution failure
   */
  private def handleFailure(
    batchId: String,
    startTime: Long,
    error: Exception,
    isInvariantViolation: Boolean
  ): FlowResult = {
    val executionTimeMs = (System.nanoTime() - startTime) / 1000000
    val errorType = if (isInvariantViolation) "invariant violation" else "error"
    
    logger.error(
      s"Flow ${flowConfig.name} failed after ${executionTimeMs}ms - $errorType: ${error.getMessage}",
      error
    )
    
    FlowResult(
      flowName = flowConfig.name,
      batchId = batchId,
      success = false,
      error = Some(if (isInvariantViolation) s"Invariant violation: ${error.getMessage}" else error.getMessage)
    )
  }
  
  /**
   * Verifies invariant: input = valid + rejected
   */
  private def verifyInvariant(input: Long, valid: Long, rejected: Long): Unit = {
    if (input != valid + rejected) {
      val message = s"Invariant violation in flow ${flowConfig.name}: " +
        s"input_count ($input) != valid_count ($valid) + rejected_count ($rejected)"
      logger.error(message)
      throw new InvariantViolationException(message)
    }
    logger.debug(s"Invariant verified: $input = $valid + $rejected")
  }
}

/**
 * Result of flow execution
 */
case class FlowResult(
  flowName: String,
  batchId: String,
  success: Boolean,
  inputRecords: Long = 0,
  mergedRecords: Long = 0,
  validRecords: Long = 0,
  rejectedRecords: Long = 0,
  rejectionRate: Double = 0.0,
  executionTimeMs: Long = 0,
  rejectionReasons: Map[String, Long] = Map.empty,
  error: Option[String] = None
)

/**
 * Information about an additional table
 */
case class AdditionalTableInfo(
  tableName: String,
  data: DataFrame,
  outputPath: Option[String],
  dagMetadata: Option[AdditionalTableMetadata]
)

/**
 * Exception thrown when record invariant is violated
 */
class InvariantViolationException(message: String) extends Exception(message)
