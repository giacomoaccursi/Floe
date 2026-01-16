package com.etl.framework.logging

import org.slf4j.{Logger, LoggerFactory}
import com.etl.framework.exceptions.FrameworkException

/**
 * Centralized logging utility for the ETL framework
 * Provides structured logging with context and performance metrics
 */
trait FrameworkLogger {
  
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Log operation start with context
   */
  def logOperationStart(operation: String, context: Map[String, Any] = Map.empty): Unit = {
    val contextStr = formatContext(context)
    logger.info(s"[START] $operation$contextStr")
  }
  
  /**
   * Log operation completion with duration
   */
  def logOperationComplete(
    operation: String,
    durationMs: Long,
    context: Map[String, Any] = Map.empty
  ): Unit = {
    val contextStr = formatContext(context)
    logger.info(s"[COMPLETE] $operation (${durationMs}ms)$contextStr")
  }
  
  /**
   * Log operation failure
   */
  def logOperationFailure(
    operation: String,
    error: Throwable,
    context: Map[String, Any] = Map.empty
  ): Unit = {
    val contextStr = formatContext(context)
    error match {
      case fe: FrameworkException =>
        logger.error(s"[FAILURE] $operation - ${fe.getMessage}$contextStr", fe)
      case _ =>
        logger.error(s"[FAILURE] $operation - ${error.getMessage}$contextStr", error)
    }
  }
  
  /**
   * Log validation warning
   */
  def logValidationWarning(
    flowName: String,
    validationType: String,
    warningCount: Long,
    details: String = ""
  ): Unit = {
    val detailsStr = if (details.nonEmpty) s" - $details" else ""
    logger.warn(s"[VALIDATION_WARNING] Flow: $flowName, Type: $validationType, Count: $warningCount$detailsStr")
  }
  
  /**
   * Log validation rejection
   */
  def logValidationRejection(
    flowName: String,
    validationType: String,
    rejectedCount: Long,
    totalCount: Long,
    details: String = ""
  ): Unit = {
    val rate = if (totalCount > 0) (rejectedCount.toDouble / totalCount * 100) else 0.0
    val detailsStr = if (details.nonEmpty) s" - $details" else ""
    logger.warn(
      f"[VALIDATION_REJECTION] Flow: $flowName, Type: $validationType, " +
      f"Rejected: $rejectedCount/$totalCount ($rate%.2f%%)$detailsStr"
    )
  }
  
  /**
   * Log performance metrics
   */
  def logPerformanceMetrics(
    operation: String,
    metrics: PerformanceMetrics
  ): Unit = {
    logger.info(
      f"[PERFORMANCE] $operation - " +
      f"Duration: ${metrics.durationMs}ms, " +
      f"Records: ${metrics.recordCount}, " +
      f"Throughput: ${metrics.throughput}%.2f records/sec" +
      (if (metrics.additionalMetrics.nonEmpty) {
        s", ${metrics.additionalMetrics.map { case (k, v) => s"$k: $v" }.mkString(", ")}"
      } else "")
    )
  }
  
  /**
   * Log flow execution summary
   */
  def logFlowSummary(
    flowName: String,
    batchId: String,
    inputRecords: Long,
    validRecords: Long,
    rejectedRecords: Long,
    durationMs: Long
  ): Unit = {
    val rejectionRate = if (inputRecords > 0) {
      rejectedRecords.toDouble / inputRecords * 100
    } else 0.0
    
    logger.info(
      f"[FLOW_SUMMARY] Flow: $flowName, Batch: $batchId, " +
      f"Input: $inputRecords, Valid: $validRecords, Rejected: $rejectedRecords, " +
      f"Rejection Rate: $rejectionRate%.2f%%, Duration: ${durationMs}ms"
    )
  }
  
  /**
   * Log batch execution summary
   */
  def logBatchSummary(
    batchId: String,
    flowsProcessed: Int,
    totalInput: Long,
    totalValid: Long,
    totalRejected: Long,
    durationMs: Long,
    success: Boolean
  ): Unit = {
    val status = if (success) "SUCCESS" else "FAILURE"
    val rejectionRate = if (totalInput > 0) {
      totalRejected.toDouble / totalInput * 100
    } else 0.0
    
    logger.info(
      f"[BATCH_SUMMARY] Batch: $batchId, Status: $status, " +
      f"Flows: $flowsProcessed, Input: $totalInput, Valid: $totalValid, " +
      f"Rejected: $totalRejected, Rejection Rate: $rejectionRate%.2f%%, Duration: ${durationMs}ms"
    )
  }
  
  /**
   * Log configuration loading
   */
  def logConfigurationLoaded(
    configType: String,
    configPath: String,
    itemCount: Int = 0
  ): Unit = {
    val countStr = if (itemCount > 0) s" ($itemCount items)" else ""
    logger.info(s"[CONFIG] Loaded $configType from '$configPath'$countStr")
  }
  
  /**
   * Log data source reading
   */
  def logDataSourceRead(
    sourceType: String,
    sourcePath: String,
    recordCount: Long,
    durationMs: Long
  ): Unit = {
    logger.info(
      s"[DATA_READ] Source: $sourceType, Path: $sourcePath, " +
      s"Records: $recordCount, Duration: ${durationMs}ms"
    )
  }
  
  /**
   * Log data write
   */
  def logDataWrite(
    outputType: String,
    outputPath: String,
    recordCount: Long,
    durationMs: Long
  ): Unit = {
    logger.info(
      s"[DATA_WRITE] Type: $outputType, Path: $outputPath, " +
      s"Records: $recordCount, Duration: ${durationMs}ms"
    )
  }
  
  /**
   * Log merge operation
   */
  def logMergeOperation(
    flowName: String,
    mergeStrategy: String,
    existingRecords: Long,
    newRecords: Long,
    resultRecords: Long,
    durationMs: Long
  ): Unit = {
    logger.info(
      s"[MERGE] Flow: $flowName, Strategy: $mergeStrategy, " +
      s"Existing: $existingRecords, New: $newRecords, Result: $resultRecords, " +
      s"Duration: ${durationMs}ms"
    )
  }
  
  /**
   * Log transformation execution
   */
  def logTransformation(
    flowName: String,
    transformationType: String,
    inputRecords: Long,
    outputRecords: Long,
    durationMs: Long
  ): Unit = {
    logger.info(
      s"[TRANSFORM] Flow: $flowName, Type: $transformationType, " +
      s"Input: $inputRecords, Output: $outputRecords, Duration: ${durationMs}ms"
    )
  }
  
  /**
   * Log DAG node execution
   */
  def logDAGNodeExecution(
    nodeId: String,
    dependencies: Seq[String],
    recordCount: Long,
    durationMs: Long
  ): Unit = {
    val depsStr = if (dependencies.nonEmpty) {
      s", Dependencies: [${dependencies.mkString(", ")}]"
    } else ""
    logger.info(
      s"[DAG_NODE] Node: $nodeId$depsStr, Records: $recordCount, Duration: ${durationMs}ms"
    )
  }
  
  /**
   * Log join operation
   */
  def logJoinOperation(
    parentNode: String,
    childNode: String,
    joinStrategy: String,
    parentRecords: Long,
    childRecords: Long,
    resultRecords: Long,
    durationMs: Long
  ): Unit = {
    logger.info(
      s"[JOIN] Parent: $parentNode ($parentRecords), Child: $childNode ($childRecords), " +
      s"Strategy: $joinStrategy, Result: $resultRecords, Duration: ${durationMs}ms"
    )
  }
  
  /**
   * Log rollback operation
   */
  def logRollback(
    batchId: String,
    flowsToRollback: Seq[String],
    reason: String
  ): Unit = {
    logger.warn(
      s"[ROLLBACK] Batch: $batchId, Flows: [${flowsToRollback.mkString(", ")}], Reason: $reason"
    )
  }
  
  /**
   * Log checkpoint
   */
  def logCheckpoint(
    batchId: String,
    completedFlows: Seq[String],
    remainingFlows: Seq[String]
  ): Unit = {
    logger.info(
      s"[CHECKPOINT] Batch: $batchId, " +
      s"Completed: [${completedFlows.mkString(", ")}], " +
      s"Remaining: [${remainingFlows.mkString(", ")}]"
    )
  }
  
  /**
   * Log invariant verification
   */
  def logInvariantVerification(
    flowName: String,
    inputCount: Long,
    validCount: Long,
    rejectedCount: Long,
    passed: Boolean
  ): Unit = {
    val status = if (passed) "PASSED" else "FAILED"
    logger.info(
      s"[INVARIANT] Flow: $flowName, Status: $status, " +
      s"Input: $inputCount, Valid: $validCount, Rejected: $rejectedCount"
    )
  }
  
  /**
   * Log additional table creation
   */
  def logAdditionalTable(
    tableName: String,
    createdByFlow: String,
    recordCount: Long,
    outputPath: String
  ): Unit = {
    logger.info(
      s"[ADDITIONAL_TABLE] Table: $tableName, Created by: $createdByFlow, " +
      s"Records: $recordCount, Path: $outputPath"
    )
  }
  
  /**
   * Log auto-discovery
   */
  def logAutoDiscovery(
    discoveredTables: Seq[String],
    nodesCreated: Int
  ): Unit = {
    logger.info(
      s"[AUTO_DISCOVERY] Discovered ${discoveredTables.size} additional tables: " +
      s"[${discoveredTables.mkString(", ")}], Created $nodesCreated DAG nodes"
    )
  }
  
  /**
   * Debug logging
   */
  def logDebug(message: String, context: Map[String, Any] = Map.empty): Unit = {
    if (logger.isDebugEnabled) {
      val contextStr = formatContext(context)
      logger.debug(s"$message$contextStr")
    }
  }
  
  /**
   * Info logging
   */
  def logInfo(message: String, context: Map[String, Any] = Map.empty): Unit = {
    val contextStr = formatContext(context)
    logger.info(s"$message$contextStr")
  }
  
  /**
   * Warning logging
   */
  def logWarning(message: String, context: Map[String, Any] = Map.empty): Unit = {
    val contextStr = formatContext(context)
    logger.warn(s"$message$contextStr")
  }
  
  /**
   * Error logging
   */
  def logError(message: String, error: Option[Throwable] = None, context: Map[String, Any] = Map.empty): Unit = {
    val contextStr = formatContext(context)
    error match {
      case Some(e) => logger.error(s"$message$contextStr", e)
      case None => logger.error(s"$message$contextStr")
    }
  }
  
  /**
   * Format context map for logging
   */
  private def formatContext(context: Map[String, Any]): String = {
    if (context.isEmpty) ""
    else s" [${context.map { case (k, v) => s"$k=$v" }.mkString(", ")}]"
  }
  
  /**
   * Execute operation with timing and logging
   */
  def withTiming[T](
    operation: String,
    context: Map[String, Any] = Map.empty
  )(block: => T): T = {
    logOperationStart(operation, context)
    val startTime = System.currentTimeMillis()
    try {
      val result = block
      val duration = System.currentTimeMillis() - startTime
      logOperationComplete(operation, duration, context)
      result
    } catch {
      case e: Throwable =>
        val duration = System.currentTimeMillis() - startTime
        logOperationFailure(operation, e, context + ("durationMs" -> duration))
        throw e
    }
  }
}

/**
 * Performance metrics container
 */
case class PerformanceMetrics(
  durationMs: Long,
  recordCount: Long,
  additionalMetrics: Map[String, Any] = Map.empty
) {
  def throughput: Double = {
    if (durationMs > 0) recordCount.toDouble / (durationMs.toDouble / 1000.0)
    else 0.0
  }
}

/**
 * Companion object for creating loggers
 */
object FrameworkLogger {
  
  /**
   * Create a logger for a specific class
   */
  def apply(clazz: Class[_]): Logger = {
    LoggerFactory.getLogger(clazz)
  }
  
  /**
   * Create a logger for a specific name
   */
  def apply(name: String): Logger = {
    LoggerFactory.getLogger(name)
  }
}
