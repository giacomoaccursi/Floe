package com.etl.framework.aggregation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import java.util.concurrent.Executors

/**
 * Executes DAG execution plans
 */
class DAGExecutor(nodeProcessor: DAGNodeProcessor) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val MaxParallelTimeout: FiniteDuration = 2.hours

  // Bounded thread pool: avoids saturating the driver with unbounded concurrent
  // metadata resolutions and schema inferences when many DAG nodes run in parallel
  private val parallelEc: ExecutionContext = ExecutionContext.fromExecutorService(
    Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors() * 2)
  )
  
  /**
   * Executes DAG nodes according to the execution plan
   */
  def execute(plan: DAGExecutionPlan): DataFrame = {
    logger.info("Executing DAG plan")
    val nodeResults = mutable.Map[String, DataFrame]()
    
    plan.groups.foreach { group =>
      logger.info(s"Executing DAG group with ${group.nodes.size} nodes (parallel=${group.parallel})")
      
      val groupResults = if (group.parallel) {
        executeGroupParallel(group, nodeResults.toMap)
      } else {
        executeGroupSequential(group, nodeResults.toMap)
      }
      
      groupResults.foreach { case (nodeId, df) =>
        nodeResults(nodeId) = df
      }
    }
    
    val rootResult = nodeResults.getOrElse(
      plan.rootNode,
      throw new IllegalStateException(
        s"DAG execution error: root node '${plan.rootNode}' was not produced. " +
        s"Available nodes: ${nodeResults.keys.mkString(", ")}"
      )
    )
    logger.info(s"DAG execution completed, returning root node: ${plan.rootNode}")
    rootResult
  }
  
  /**
   * Executes a group of nodes sequentially
   */
  private def executeGroupSequential(
    group: DAGExecutionGroup,
    nodeResults: Map[String, DataFrame]
  ): Map[String, DataFrame] = {
    val results = mutable.Map[String, DataFrame]()
    
    group.nodes.foreach { node =>
      val result = nodeProcessor.executeNode(node, nodeResults ++ results)
      results(node.id) = result
    }
    
    results.toMap
  }
  
  /**
   * Executes a group of nodes in parallel
   */
  private def executeGroupParallel(
    group: DAGExecutionGroup,
    nodeResults: Map[String, DataFrame]
  ): Map[String, DataFrame] = {
    val futures = group.nodes.map { node =>
      Future {
        node.id -> nodeProcessor.executeNode(node, nodeResults)
      }(parallelEc)
    }

    val allResults = Future.sequence(futures)(implicitly, parallelEc)
    Await.result(allResults, MaxParallelTimeout).toMap
  }
}
