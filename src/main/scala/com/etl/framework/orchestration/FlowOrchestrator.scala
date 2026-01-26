package com.etl.framework.orchestration

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig}
import com.etl.framework.logging.FrameworkLogger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.collection.mutable

/**
 * Coordinates execution of all flows respecting dependencies
 */
class FlowOrchestrator(
  globalConfig: GlobalConfig,
  flowConfigs: Seq[FlowConfig],
  domainsConfig: Option[DomainsConfig] = None
)(implicit spark: SparkSession) extends FrameworkLogger {
  
  /**
   * Builds execution plan based on FK dependencies
   */
  def buildExecutionPlan(): ExecutionPlan = {
    logOperationStart("BuildExecutionPlan", Map("flowCount" -> flowConfigs.size))
    
    // Build dependency graph from Foreign Keys
    val dependencyGraph = buildDependencyGraph()
    
    // Perform topological sort to determine execution order
    val sortedFlows = topologicalSort(dependencyGraph)
    
    // Group flows into execution groups for parallel execution
    val groups = groupFlowsForParallelExecution(sortedFlows, dependencyGraph)
    
    logInfo(s"Execution plan created with ${groups.size} groups", Map(
      "totalFlows" -> flowConfigs.size,
      "groups" -> groups.size
    ))
    
    ExecutionPlan(groups)
  }
  
  /**
   * Builds dependency graph from Foreign Key relationships
   * Returns a map: flow_name -> Set(dependent_flow_names)
   */
  private def buildDependencyGraph(): Map[String, Set[String]] = {
    val graph = mutable.Map[String, mutable.Set[String]]()
    
    // Initialize all flows in the graph
    flowConfigs.foreach { flow =>
      graph.getOrElseUpdate(flow.name, mutable.Set.empty)
    }
    
    // Add edges based on Foreign Key dependencies
    flowConfigs.foreach { flow =>
      flow.validation.foreignKeys.foreach { fk =>
        val referencedFlow = fk.references.flow
        
        // flow depends on referencedFlow
        // So referencedFlow must execute before flow
        graph.getOrElseUpdate(flow.name, mutable.Set.empty).add(referencedFlow)
      }
    }
    
    // Convert to immutable map
    graph.map { case (k, v) => k -> v.toSet }.toMap
  }
  
  /**
   * Performs topological sort on the dependency graph
   * Returns flows in execution order
   * Throws exception if circular dependency detected
   */
  private def topologicalSort(dependencyGraph: Map[String, Set[String]]): Seq[String] = {
    val sorted = mutable.ArrayBuffer[String]()
    val visited = mutable.Set[String]()
    val visiting = mutable.Set[String]()
    
    def visit(flowName: String): Unit = {
      if (visiting.contains(flowName)) {
        throw new CircularDependencyException(
          s"Circular dependency detected involving flow: $flowName"
        )
      }
      
      if (!visited.contains(flowName)) {
        visiting.add(flowName)
        
        // Visit all dependencies first
        dependencyGraph.getOrElse(flowName, Set.empty).foreach { dependency =>
          visit(dependency)
        }
        
        visiting.remove(flowName)
        visited.add(flowName)
        sorted.append(flowName)
      }
    }
    
    // Visit all flows
    dependencyGraph.keys.foreach(visit)
    
    sorted.toSeq
  }
  
  /**
   * Groups flows into execution groups for parallel execution
   * Flows in the same group have no dependencies on each other
   */
  private def groupFlowsForParallelExecution(
    sortedFlows: Seq[String],
    dependencyGraph: Map[String, Set[String]]
  ): Seq[ExecutionGroup] = {
    val groups = mutable.ArrayBuffer[ExecutionGroup]()
    val completed = mutable.Set[String]()
    val remaining = mutable.Queue(sortedFlows: _*)
    
    while (remaining.nonEmpty) {
      val currentGroup = mutable.ArrayBuffer[String]()
      val nextRemaining = mutable.Queue[String]()
      
      // Find all flows whose dependencies are satisfied
      while (remaining.nonEmpty) {
        val flow = remaining.dequeue()
        val dependencies = dependencyGraph.getOrElse(flow, Set.empty)
        
        if (dependencies.subsetOf(completed)) {
          // All dependencies satisfied, can execute in this group
          currentGroup.append(flow)
        } else {
          // Dependencies not satisfied, defer to next iteration
          nextRemaining.enqueue(flow)
        }
      }
      
      if (currentGroup.nonEmpty) {
        // Get FlowConfig objects for this group
        val flowConfigsInGroup = currentGroup.flatMap { flowName =>
          flowConfigs.find(_.name == flowName)
        }
        
        // Determine if parallel execution is enabled
        val parallel = globalConfig.performance.parallelFlows && currentGroup.size > 1
        
        groups.append(ExecutionGroup(flowConfigsInGroup.toSeq, parallel))
        
        // Mark flows in this group as completed AFTER the group is formed
        currentGroup.foreach(completed.add)
      }
      
      remaining.clear()
      remaining ++= nextRemaining
    }
    
    groups.toSeq
  }
  
  /**
   * Executes all flows in correct order
   */
  def execute(): IngestionResult = {
    val batchId = generateBatchId()
    val startTime = System.currentTimeMillis()
    
    logOperationStart("IngestionExecution", Map(
      "batchId" -> batchId,
      "flowCount" -> flowConfigs.size
    ))
    
    val plan = buildExecutionPlan()
    
    val flowResults = mutable.ArrayBuffer[FlowResult]()
    val validatedFlows = mutable.Map[String, DataFrame]()
    
    try {
      // Execute each group in order
      plan.groups.zipWithIndex.foreach { case (group, groupIndex) =>
        logInfo(s"Executing group ${groupIndex + 1}/${plan.groups.size}", Map(
          "flowCount" -> group.flows.size,
          "parallel" -> group.parallel
        ))
        
        val groupResults = if (group.parallel) {
          // Parallel execution
          executeGroupParallel(group, batchId, validatedFlows.toMap)
        } else {
          // Sequential execution
          executeGroupSequential(group, batchId, validatedFlows.toMap)
        }
        
        // Check for failures
        groupResults.foreach { result =>
          flowResults.append(result)
          
          if (!result.success) {
            // Flow failed completely
            logOperationFailure("FlowExecution", 
              new Exception(result.error.getOrElse("Unknown error")),
              Map("flowName" -> result.flowName, "batchId" -> batchId)
            )
            
            if (globalConfig.processing.rollbackOnFailure) {
              rollback(batchId, flowResults.toSeq)
            }
            
            return IngestionResult(
              batchId = batchId,
              flowResults = flowResults.toSeq,
              success = false,
              error = Some(s"Flow ${result.flowName} failed: ${result.error.getOrElse("Unknown error")}")
            )
          } else if (shouldStopExecution(result)) {
            // Flow succeeded but should stop execution (e.g., too many rejections)
            logWarning(s"Stopping execution due to validation errors in flow ${result.flowName}", Map(
              "rejectionRate" -> result.rejectionRate,
              "rejectedRecords" -> result.rejectedRecords
            ))
            
            if (globalConfig.processing.rollbackOnFailure) {
              rollback(batchId, flowResults.toSeq)
            }
            
            return IngestionResult(
              batchId = batchId,
              flowResults = flowResults.toSeq,
              success = false,
              error = Some(s"Flow ${result.flowName} exceeded rejection threshold or has validation errors")
            )
          } else {
            // Flow succeeded, load validated data for next flows
            val validatedPath = flowConfigs.find(_.name == result.flowName)
              .flatMap(_.output.path)
              .getOrElse(s"${globalConfig.paths.validatedPath}/${result.flowName}")
            
            validatedFlows(result.flowName) = spark.read.parquet(validatedPath)
          }
        }
      }
      
      // All flows completed successfully
      val executionTimeMs = System.currentTimeMillis() - startTime
      writeBatchMetadata(batchId, flowResults.toSeq, executionTimeMs, success = true)
      
      // Log batch summary
      val totalInput = flowResults.map(_.inputRecords).sum
      val totalValid = flowResults.map(_.validRecords).sum
      val totalRejected = flowResults.map(_.rejectedRecords).sum
      
      logBatchSummary(
        batchId,
        flowResults.size,
        totalInput,
        totalValid,
        totalRejected,
        executionTimeMs,
        success = true
      )
      
      IngestionResult(
        batchId = batchId,
        flowResults = flowResults.toSeq,
        success = true
      )
      
    } catch {
      case e: Exception =>
        logOperationFailure("IngestionExecution", e, Map("batchId" -> batchId))
        
        if (globalConfig.processing.rollbackOnFailure) {
          rollback(batchId, flowResults.toSeq)
        }
        
        IngestionResult(
          batchId = batchId,
          flowResults = flowResults.toSeq,
          success = false,
          error = Some(e.getMessage)
        )
    }
  }
  
  /**
   * Executes a group of flows sequentially
   * Returns early if a flow fails and rollbackOnFailure is enabled
   */
  private def executeGroupSequential(
    group: ExecutionGroup,
    batchId: String,
    validatedFlows: Map[String, DataFrame]
  ): Seq[FlowResult] = {
    val results = mutable.ArrayBuffer[FlowResult]()
    
    for (flowConfig <- group.flows) {
      val result = executeFlow(flowConfig, batchId, validatedFlows)
      results.append(result)
      
      // Check if we should stop execution immediately
      // Only stop if rollbackOnFailure is enabled, otherwise continue with remaining flows
      if (globalConfig.processing.rollbackOnFailure) {
        if (!result.success || shouldStopExecution(result)) {
          // Stop executing remaining flows in this group
          return results.toSeq
        }
      }
    }
    
    results.toSeq
  }
  
  /**
   * Executes a group of flows in parallel
   */
  private def executeGroupParallel(
    group: ExecutionGroup,
    batchId: String,
    validatedFlows: Map[String, DataFrame]
  ): Seq[FlowResult] = {
    import scala.concurrent._
    import scala.concurrent.duration._
    import ExecutionContext.Implicits.global
    
    val futures = group.flows.map { flowConfig =>
      Future {
        executeFlow(flowConfig, batchId, validatedFlows)
      }
    }
    
    val allResults = Future.sequence(futures)
    Await.result(allResults, Duration.Inf)
  }
  
  /**
   * Executes a single flow
   */
  private def executeFlow(
    flowConfig: FlowConfig,
    batchId: String,
    validatedFlows: Map[String, DataFrame]
  ): FlowResult = {
    logDebug(s"Starting flow execution", Map("flowName" -> flowConfig.name, "batchId" -> batchId))
    
    val executor = new FlowExecutor(flowConfig, globalConfig, validatedFlows, domainsConfig)
    executor.execute(batchId)
  }
  
  /**
   * Determines if execution should stop based on result
   */
  private def shouldStopExecution(result: FlowResult): Boolean = {
    if (!result.success) {
      // Flow failed completely
      return true
    }
    
    // Check if rejection rate exceeds threshold
    if (globalConfig.processing.maxRejectionRate > 0 &&
        result.rejectionRate > globalConfig.processing.maxRejectionRate) {
      logValidationWarning(
        result.flowName,
        "RejectionRateThreshold",
        result.rejectedRecords,
        f"Rate ${result.rejectionRate}%.2f%% exceeds threshold ${globalConfig.processing.maxRejectionRate}%.2f%%"
      )
      return globalConfig.processing.failOnValidationError
    }
    
    // Check if there are any rejected records and fail_on_validation_error is true
    if (globalConfig.processing.failOnValidationError && result.rejectedRecords > 0) {
      logValidationWarning(
        result.flowName,
        "ValidationError",
        result.rejectedRecords,
        "failOnValidationError is enabled"
      )
      return true
    }
    
    false
  }
  
  /**
   * Performs rollback of completed flows
   */
  private def rollback(batchId: String, results: Seq[FlowResult]): Unit = {
    val flowsToRollback = results.filter(_.success).map(_.flowName)
    logRollback(batchId, flowsToRollback, "Flow failure or validation error")
    
    results.filter(_.success).foreach { result =>
      try {
        val flowConfig = flowConfigs.find(_.name == result.flowName).get
        
        // Delete validated output
        val validatedPath = flowConfig.output.path.getOrElse(
          s"${globalConfig.paths.validatedPath}/${result.flowName}"
        )
        deleteDirectory(validatedPath)
        logDebug(s"Deleted validated data", Map("flowName" -> result.flowName, "path" -> validatedPath))
        
        // Delete rejected output
        val rejectedPath = flowConfig.output.rejectedPath.getOrElse(
          s"${globalConfig.paths.rejectedPath}/${result.flowName}"
        )
        deleteDirectory(rejectedPath)
        logDebug(s"Deleted rejected data", Map("flowName" -> result.flowName, "path" -> rejectedPath))
        
        // For delta mode, we would restore previous state here
        // This is a simplified implementation
        
      } catch {
        case e: Exception =>
          logError(s"Error during rollback of flow ${result.flowName}", Some(e))
      }
    }
    
    // Mark batch as failed in metadata
    writeBatchMetadata(batchId, results, 0, success = false, rolled_back = true)
  }
  
  /**
   * Deletes a directory recursively
   */
  private def deleteDirectory(path: String): Unit = {
    import java.nio.file.{Files, Paths}
    import scala.collection.JavaConverters._
    
    val dirPath = Paths.get(path)
    if (Files.exists(dirPath)) {
      Files.walk(dirPath)
        .iterator()
        .asScala
        .toSeq
        .reverse
        .foreach(p => Files.deleteIfExists(p))
    }
  }
  
  /**
   * Generates a unique batch ID
   */
  private def generateBatchId(): String = {
    val format = globalConfig.processing.batchIdFormat
    val timestamp = Instant.now()
    
    format match {
      case "timestamp" =>
        timestamp.toEpochMilli.toString
      
      case "datetime" =>
        DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
          .withZone(java.time.ZoneId.systemDefault())
          .format(timestamp)
      
      case _ =>
        // Default to datetime format
        DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
          .withZone(java.time.ZoneId.systemDefault())
          .format(timestamp)
    }
  }
  
  /**
   * Writes batch metadata
   */
  private def writeBatchMetadata(
    batchId: String,
    flowResults: Seq[FlowResult],
    executionTimeMs: Long,
    success: Boolean,
    rolled_back: Boolean = false
  ): Unit = {
    val metadataPath = s"${globalConfig.paths.metadataPath}/$batchId/summary.json"
    
    val totalInput = flowResults.map(_.inputRecords).sum
    val totalValid = flowResults.map(_.validRecords).sum
    val totalRejected = flowResults.map(_.rejectedRecords).sum
    val overallRejectionRate = if (totalInput > 0) totalRejected.toDouble / totalInput else 0.0
    
    val metadata = Map(
      "batch_id" -> batchId,
      "execution_start" -> Instant.now().toString,
      "execution_time_ms" -> executionTimeMs,
      "success" -> success,
      "rolled_back" -> rolled_back,
      "flows_processed" -> flowResults.size,
      "total_input_records" -> totalInput,
      "total_valid_records" -> totalValid,
      "total_rejected_records" -> totalRejected,
      "overall_rejection_rate" -> overallRejectionRate,
      "flows" -> flowResults.map { result =>
        Map(
          "flow_name" -> result.flowName,
          "success" -> result.success,
          "load_mode" -> flowConfigs.find(_.name == result.flowName).map(_.loadMode.`type`).getOrElse("unknown"),
          "input_records" -> result.inputRecords,
          "merged_records" -> result.mergedRecords,
          "valid_records" -> result.validRecords,
          "rejected_records" -> result.rejectedRecords,
          "rejection_rate" -> result.rejectionRate,
          "execution_time_ms" -> result.executionTimeMs,
          "rejection_reasons" -> result.rejectionReasons,
          "error" -> result.error.getOrElse("")
        )
      }
    )
    
    // Convert to JSON and write
    import org.json4s._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.write
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    
    val jsonString = write(metadata)
    
    // Write to file
    import java.nio.file.{Files, Paths, StandardOpenOption}
    val path = Paths.get(metadataPath)
    Files.createDirectories(path.getParent)
    Files.write(path, jsonString.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    
    // Create "latest" symlink
    createLatestSymlink(batchId)
    
    logDebug(s"Batch metadata written", Map("batchId" -> batchId, "path" -> metadataPath))
  }
  
  /**
   * Creates a "latest" symlink pointing to the current batch
   */
  private def createLatestSymlink(batchId: String): Unit = {
    import java.nio.file.{Files, Paths}
    
    val latestPath = Paths.get(s"${globalConfig.paths.metadataPath}/latest")
    val targetPath = Paths.get(s"${globalConfig.paths.metadataPath}/$batchId")
    
    try {
      // Delete existing symlink if it exists
      if (Files.exists(latestPath)) {
        Files.delete(latestPath)
      }
      
      // Create new symlink
      Files.createSymbolicLink(latestPath, targetPath)
      logDebug(s"Created 'latest' symlink", Map("batchId" -> batchId))
    } catch {
      case e: Exception =>
        logWarning(s"Could not create 'latest' symlink: ${e.getMessage}")
    }
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

/**
 * Exception thrown when circular dependency is detected
 */
class CircularDependencyException(message: String) extends Exception(message)
