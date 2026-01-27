package com.etl.framework.orchestration

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig}
import com.etl.framework.orchestration.batch.{BatchMetadataWriter, FlowGroupExecutor}
import com.etl.framework.orchestration.flow.FlowResult
import com.etl.framework.orchestration.planning.DependencyGraphBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Coordinates execution of all flows respecting dependencies
 * Uses specialized components for dependency analysis, execution, and metadata
 */
class FlowOrchestrator(
  globalConfig: GlobalConfig,
  flowConfigs: Seq[FlowConfig],
  domainsConfig: Option[DomainsConfig] = None
)(implicit spark: SparkSession) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Specialized components
  private val dependencyBuilder = new DependencyGraphBuilder(flowConfigs)
  private val groupExecutor = new FlowGroupExecutor(globalConfig, domainsConfig)
  private val metadataWriter = new BatchMetadataWriter(globalConfig, flowConfigs)
  
  /**
   * Builds execution plan based on FK dependencies
   */
  def buildExecutionPlan(): ExecutionPlan = {
    logger.info(s"Building execution plan for ${flowConfigs.size} flows")
    
    // Build dependency graph from Foreign Keys
    val dependencyGraph = dependencyBuilder.buildGraph()
    
    // Perform topological sort to determine execution order
    val sortedFlows = dependencyBuilder.topologicalSort(dependencyGraph)
    
    // Group flows into execution groups for parallel execution
    val groups = dependencyBuilder.groupForParallelExecution(
      sortedFlows,
      dependencyGraph,
      globalConfig.performance.parallelFlows
    )
    
    logger.info(s"Execution plan created with ${groups.size} groups (${flowConfigs.size} flows)")
    
    ExecutionPlan(groups)
  }
  
  /**
   * Executes all flows in correct order
   */
  def execute(): IngestionResult = {
    val batchId = metadataWriter.generateBatchId()
    val startTime = System.nanoTime()
    
    logger.info(s"Starting ingestion execution - batchId: $batchId, flows: ${flowConfigs.size}")
    
    val plan = buildExecutionPlan()
    val flowResults = mutable.ArrayBuffer[FlowResult]()
    val validatedFlows = mutable.Map[String, DataFrame]()
    
    try {
      // Execute each group in order
      plan.groups.zipWithIndex.foreach { case (group, groupIndex) =>
        val mode = if (group.parallel) "parallel" else "sequential"
        logger.info(s"Executing group ${groupIndex + 1}/${plan.groups.size} ($mode, ${group.flows.size} flows)")
        
        val groupResults = executeGroup(group, batchId, validatedFlows.toMap)
        
        // Process group results
        val shouldStop = processGroupResults(groupResults, flowResults, validatedFlows, batchId)
        if (shouldStop.isDefined) {
          return shouldStop.get
        }
      }
      
      // All flows completed successfully
      createSuccessResult(batchId, flowResults.toSeq, startTime)
      
    } catch {
      case e: Exception =>
        handleExecutionFailure(batchId, flowResults.toSeq, startTime, e)
    }
  }
  
  /**
   * Executes a group of flows (sequential or parallel)
   */
  private def executeGroup(
    group: ExecutionGroup,
    batchId: String,
    validatedFlows: Map[String, DataFrame]
  ): Seq[FlowResult] = {
    if (group.parallel) {
      groupExecutor.executeParallel(group, batchId, validatedFlows)
    } else {
      groupExecutor.executeSequential(group, batchId, validatedFlows)
    }
  }
  
  /**
   * Processes group results and checks for failures
   * Returns Some(IngestionResult) if execution should stop, None otherwise
   */
  private def processGroupResults(
    groupResults: Seq[FlowResult],
    flowResults: mutable.ArrayBuffer[FlowResult],
    validatedFlows: mutable.Map[String, DataFrame],
    batchId: String
  ): Option[IngestionResult] = {
    
    groupResults.foreach { result =>
      flowResults.append(result)
      
      if (!result.success) {
        // Flow failed completely
        logger.error(s"Flow ${result.flowName} failed: ${result.error.getOrElse("Unknown error")}")
        
        return Some(IngestionResult(
          batchId = batchId,
          flowResults = flowResults.toSeq,
          success = false,
          error = Some(s"Flow ${result.flowName} failed: ${result.error.getOrElse("Unknown error")}")
        ))
      } else if (groupExecutor.shouldStopExecution(result)) {
        // Flow succeeded but should stop execution (e.g., too many rejections)
        logger.warn(
          s"Stopping execution - flow ${result.flowName} " +
          f"rejection rate: ${result.rejectionRate}%.2f%%, rejected: ${result.rejectedRecords}"
        )
        
        return Some(IngestionResult(
          batchId = batchId,
          flowResults = flowResults.toSeq,
          success = false,
          error = Some(s"Flow ${result.flowName} exceeded rejection threshold or has validation errors")
        ))
      } else {
        // Flow succeeded, load validated data for next flows
        loadValidatedData(result, validatedFlows)
      }
    }
    
    None // Continue execution
  }
  
  /**
   * Loads validated data for a successful flow
   */
  private def loadValidatedData(
    result: FlowResult,
    validatedFlows: mutable.Map[String, DataFrame]
  ): Unit = {
    val validatedPath = flowConfigs.find(_.name == result.flowName)
      .flatMap(_.output.path)
      .getOrElse(s"${globalConfig.paths.validatedPath}/${result.flowName}")
    
    validatedFlows(result.flowName) = spark.read.parquet(validatedPath)
  }
  
  /**
   * Creates a successful ingestion result
   */
  private def createSuccessResult(
    batchId: String,
    flowResults: Seq[FlowResult],
    startTime: Long
  ): IngestionResult = {
    val executionTimeMs = (System.nanoTime() - startTime) / 1000000
    
    metadataWriter.writeBatchMetadata(
      batchId,
      flowResults,
      executionTimeMs,
      success = true
    )
    
    logBatchSummary(batchId, flowResults, executionTimeMs)
    
    IngestionResult(
      batchId = batchId,
      flowResults = flowResults,
      success = true
    )
  }
  
  /**
   * Handles execution failure
   */
  private def handleExecutionFailure(
    batchId: String,
    flowResults: Seq[FlowResult],
    startTime: Long,
    error: Exception
  ): IngestionResult = {
    val executionTimeMs = (System.nanoTime() - startTime) / 1000000
    logger.error(s"Ingestion execution failed after ${executionTimeMs}ms - batchId: $batchId", error)
    
    IngestionResult(
      batchId = batchId,
      flowResults = flowResults,
      success = false,
      error = Some(error.getMessage)
    )
  }
  
  /**
   * Logs batch summary
   */
  private def logBatchSummary(
    batchId: String,
    flowResults: Seq[FlowResult],
    executionTimeMs: Long
  ): Unit = {
    val totalInput = flowResults.map(_.inputRecords).sum
    val totalValid = flowResults.map(_.validRecords).sum
    val totalRejected = flowResults.map(_.rejectedRecords).sum
    val rejectionRate = if (totalInput > 0) (totalRejected.toDouble / totalInput * 100) else 0.0
    
    logger.info(
      f"Batch $batchId completed in ${executionTimeMs}ms - " +
      f"flows: ${flowResults.size}, input: $totalInput, valid: $totalValid, " +
      f"rejected: $totalRejected ($rejectionRate%.2f%%)"
    )
  }
}

/**
 * Execution plan containing groups of flows
 */
case class ExecutionPlan(
  groups: Seq[ExecutionGroup]
)

/**
 * Group of flows that can be executed together
 */
case class ExecutionGroup(
  flows: Seq[FlowConfig],
  parallel: Boolean
)

/**
 * Result of Ingestion execution
 */
case class IngestionResult(
  batchId: String,
  flowResults: Seq[FlowResult],
  success: Boolean,
  error: Option[String] = None
)
