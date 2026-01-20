package com.etl.framework.aggregation

import com.etl.framework.config.{AggregationConfig, DAGNode, GlobalConfig, JoinConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Coordinates DAG-based aggregation
 */
class DAGOrchestrator(
  dagConfig: AggregationConfig,
  globalConfig: GlobalConfig,
  autoDiscoverAdditionalTables: Boolean = false
)(implicit spark: SparkSession) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Builds execution plan from DAG configuration
   */
  def buildExecutionPlan(): DAGExecutionPlan = {
    logger.info("Building DAG execution plan")
    
    // 1. Load configured nodes
    val configuredNodes = dagConfig.nodes
    logger.info(s"Loaded ${configuredNodes.size} configured DAG nodes")
    
    // 2. If auto-discovery, load additional tables
    val additionalNodes = if (autoDiscoverAdditionalTables) {
      logger.info("Auto-discovery enabled, discovering additional tables")
      discoverAdditionalTables()
    } else {
      Seq.empty
    }
    logger.info(s"Discovered ${additionalNodes.size} additional table nodes")
    
    // 3. Merge nodes
    val allNodes = configuredNodes ++ additionalNodes
    logger.info(s"Total DAG nodes: ${allNodes.size}")
    
    // 4. Build DAG and determine execution order
    buildDAG(allNodes)
  }
  
  /**
   * Executes the DAG
   */
  def execute(): DataFrame = {
    logger.info("Starting DAG execution")
    val plan = buildExecutionPlan()
    executeDAG(plan)
  }
  
  /**
   * Discovers additional tables from metadata
   */
  private def discoverAdditionalTables(): Seq[DAGNode] = {
    val metadataPath = s"${globalConfig.paths.metadataPath}/latest"
    logger.info(s"Reading additional tables metadata from: $metadataPath")
    
    try {
      val additionalTables = loadAdditionalTablesMetadata(metadataPath)
      logger.info(s"Found ${additionalTables.size} additional tables with DAG metadata")
      
      additionalTables.map { table =>
        val node = DAGNode(
          id = s"${table.tableName}_node",
          description = table.dagMetadata.description.getOrElse(s"Auto-discovered table: ${table.tableName}"),
          sourceFlow = table.tableName,
          sourcePath = table.path,
          dependencies = inferDependencies(table),
          join = inferJoinConfig(table),
          select = Seq.empty,
          filters = Seq.empty
        )
        logger.info(s"Created DAG node for additional table: ${table.tableName}")
        node
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Could not discover additional tables: ${e.getMessage}")
        Seq.empty
    }
  }
  
  /**
   * Loads additional tables metadata from the metadata directory
   */
  private def loadAdditionalTablesMetadata(metadataPath: String): Seq[AdditionalTableInfo] = {
    import java.nio.file.{Files, Paths}
    import scala.collection.JavaConverters._
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    
    val additionalTablesDir = Paths.get(s"$metadataPath/additional_tables")
    
    if (!Files.exists(additionalTablesDir)) {
      logger.info("No additional tables directory found")
      return Seq.empty
    }
    
    implicit val formats: Formats = DefaultFormats
    
    Files.list(additionalTablesDir)
      .iterator()
      .asScala
      .filter(p => p.toString.endsWith(".json"))
      .flatMap { metadataFile =>
        try {
          val jsonContent = new String(Files.readAllBytes(metadataFile))
          val json = parse(jsonContent)
          
          val tableName = (json \ "table_name").extract[String]
          val path = (json \ "path").extract[String]
          
          // Check if dagMetadata exists
          val dagMetadataOpt = (json \ "dag_metadata").toOption.map { dagJson =>
            AdditionalTableMetadata(
              primaryKey = (dagJson \ "primary_key").extract[Seq[String]],
              joinKeys = (dagJson \ "join_keys").extract[Map[String, Seq[String]]],
              description = (dagJson \ "description").extractOpt[String],
              partitionBy = (dagJson \ "partition_by").extractOpt[Seq[String]].getOrElse(Seq.empty)
            )
          }
          
          dagMetadataOpt.map { dagMetadata =>
            AdditionalTableInfo(tableName, path, dagMetadata)
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Could not parse metadata file ${metadataFile}: ${e.getMessage}")
            None
        }
      }
      .toSeq
  }
  
  /**
   * Infers dependencies from join keys
   */
  private def inferDependencies(table: AdditionalTableInfo): Seq[String] = {
    // Dependencies are the flows that this table joins with
    table.dagMetadata.joinKeys.keys.map(flow => s"${flow}_node").toSeq
  }
  
  /**
   * Infers join configuration from metadata
   */
  private def inferJoinConfig(table: AdditionalTableInfo): Option[JoinConfig] = {
    // Use first join key as parent
    table.dagMetadata.joinKeys.headOption.map { case (parentFlow, keys) =>
      JoinConfig(
        `type` = "left_outer",
        parent = s"${parentFlow}_node",
        on = keys.map(key => com.etl.framework.config.JoinCondition(left = key, right = key)),
        strategy = "nest",
        nestAs = Some(table.tableName),
        aggregations = Seq.empty
      )
    }
  }
  
  /**
   * Builds DAG from nodes and determines execution order
   */
  private def buildDAG(nodes: Seq[DAGNode]): DAGExecutionPlan = {
    logger.info("Building dependency graph for DAG nodes")
    
    // Build dependency graph
    val dependencyGraph = buildDependencyGraph(nodes)
    
    // Perform topological sort to determine execution order
    val sortedNodeIds = topologicalSort(dependencyGraph, nodes)
    
    // Group nodes into execution groups for parallel execution
    val groups = groupNodesForParallelExecution(sortedNodeIds, dependencyGraph, nodes)
    
    // Find root node (node with no dependents)
    val rootNode = findRootNode(nodes, dependencyGraph)
    
    DAGExecutionPlan(groups, rootNode)
  }
  
  /**
   * Builds dependency graph from DAG nodes
   * Returns a map: node_id -> Set(dependency_node_ids)
   */
  private def buildDependencyGraph(nodes: Seq[DAGNode]): Map[String, Set[String]] = {
    val graph = mutable.Map[String, mutable.Set[String]]()
    
    // Initialize all nodes in the graph
    nodes.foreach { node =>
      graph.getOrElseUpdate(node.id, mutable.Set.empty)
    }
    
    // Add edges based on dependencies
    nodes.foreach { node =>
      node.dependencies.foreach { dep =>
        graph.getOrElseUpdate(node.id, mutable.Set.empty).add(dep)
      }
    }
    
    // Convert to immutable map
    graph.map { case (k, v) => k -> v.toSet }.toMap
  }
  
  /**
   * Performs topological sort on the dependency graph
   * Returns node IDs in execution order
   * Throws exception if circular dependency detected
   */
  private def topologicalSort(
    dependencyGraph: Map[String, Set[String]],
    nodes: Seq[DAGNode]
  ): Seq[String] = {
    val sorted = mutable.ArrayBuffer[String]()
    val visited = mutable.Set[String]()
    val visiting = mutable.Set[String]()
    
    def visit(nodeId: String): Unit = {
      if (visiting.contains(nodeId)) {
        throw new CircularDependencyException(
          s"Circular dependency detected in DAG involving node: $nodeId"
        )
      }
      
      if (!visited.contains(nodeId)) {
        visiting.add(nodeId)
        
        // Visit all dependencies first
        dependencyGraph.getOrElse(nodeId, Set.empty).foreach { dependency =>
          visit(dependency)
        }
        
        visiting.remove(nodeId)
        visited.add(nodeId)
        sorted.append(nodeId)
      }
    }
    
    // Visit all nodes
    dependencyGraph.keys.foreach(visit)
    
    logger.info(s"Topological sort completed: ${sorted.mkString(" -> ")}")
    sorted.toSeq
  }
  
  /**
   * Groups nodes into execution groups for parallel execution
   * Nodes in the same group have no dependencies on each other
   */
  private def groupNodesForParallelExecution(
    sortedNodeIds: Seq[String],
    dependencyGraph: Map[String, Set[String]],
    nodes: Seq[DAGNode]
  ): Seq[DAGExecutionGroup] = {
    val groups = mutable.ArrayBuffer[DAGExecutionGroup]()
    val completed = mutable.Set[String]()
    val remaining = mutable.Queue(sortedNodeIds: _*)
    
    while (remaining.nonEmpty) {
      val currentGroup = mutable.ArrayBuffer[String]()
      val nextRemaining = mutable.Queue[String]()
      
      // Find all nodes whose dependencies are satisfied
      while (remaining.nonEmpty) {
        val nodeId = remaining.dequeue()
        val dependencies = dependencyGraph.getOrElse(nodeId, Set.empty)
        
        if (dependencies.subsetOf(completed)) {
          // All dependencies satisfied, can execute in this group
          currentGroup.append(nodeId)
        } else {
          // Dependencies not satisfied, defer to next iteration
          nextRemaining.enqueue(nodeId)
        }
      }
      
      if (currentGroup.nonEmpty) {
        // Get DAGNode objects for this group
        val nodesInGroup = currentGroup.flatMap { nodeId =>
          nodes.find(_.id == nodeId)
        }
        
        // Determine if parallel execution is enabled
        val parallel = globalConfig.performance.parallelNodes && currentGroup.size > 1
        
        groups.append(DAGExecutionGroup(nodesInGroup.toSeq, parallel))
        logger.info(s"Created execution group with ${nodesInGroup.size} nodes (parallel=$parallel)")
        
        // Mark nodes in this group as completed
        currentGroup.foreach(completed.add)
      }
      
      remaining.clear()
      remaining ++= nextRemaining
    }
    
    groups.toSeq
  }
  
  /**
   * Finds the root node (node with no dependents)
   * This is the final output node of the DAG
   */
  private def findRootNode(nodes: Seq[DAGNode], dependencyGraph: Map[String, Set[String]]): String = {
    // Find nodes that are not dependencies of any other node
    val allDependencies = dependencyGraph.values.flatten.toSet
    val rootNodes = nodes.map(_.id).filterNot(allDependencies.contains)
    
    if (rootNodes.isEmpty) {
      throw new IllegalStateException("No root node found in DAG - all nodes are dependencies of others")
    }
    
    if (rootNodes.size > 1) {
      logger.warn(s"Multiple root nodes found: ${rootNodes.mkString(", ")}. Using first one: ${rootNodes.head}")
    }
    
    rootNodes.head
  }
  
  /**
   * Executes DAG nodes in order
   */
  private def executeDAG(plan: DAGExecutionPlan): DataFrame = {
    logger.info("Executing DAG plan")
    val nodeResults = mutable.Map[String, DataFrame]()
    
    plan.groups.foreach { group =>
      logger.info(s"Executing DAG group with ${group.nodes.size} nodes (parallel=${group.parallel})")
      
      val groupResults = if (group.parallel) {
        // Parallel execution
        executeGroupParallel(group, nodeResults.toMap)
      } else {
        // Sequential execution
        executeGroupSequential(group, nodeResults.toMap)
      }
      
      // Store results
      groupResults.foreach { case (nodeId, df) =>
        nodeResults(nodeId) = df
      }
    }
    
    // Return root node result
    val rootResult = nodeResults(plan.rootNode)
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
      val result = executeNode(node, nodeResults ++ results)
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
    import scala.concurrent._
    import scala.concurrent.duration._
    import ExecutionContext.Implicits.global
    
    val futures = group.nodes.map { node =>
      Future {
        node.id -> executeNode(node, nodeResults)
      }
    }
    
    val allResults = Future.sequence(futures)
    Await.result(allResults, Duration.Inf).toMap
  }
  
  /**
   * Executes a single DAG node
   */
  private def executeNode(node: DAGNode, nodeResults: Map[String, DataFrame]): DataFrame = {
    logger.info(s"Executing DAG node: ${node.id}")
    
    // 1. Load source data
    val sourceData = spark.read.parquet(node.sourcePath)
    logger.debug(s"Loaded source data from: ${node.sourcePath}")
    
    // 2. Apply filters
    val filtered = applyFilters(sourceData, node.filters)
    
    // 3. Apply select
    val selected = applySelect(filtered, node.select)
    
    // 4. Apply join if configured
    val result = node.join match {
      case Some(joinConfig) =>
        val parentData = nodeResults(joinConfig.parent)
        applyJoin(parentData, selected, joinConfig)
      case None =>
        selected
    }
    
    logger.info(s"Node ${node.id} execution completed with ${result.count()} records")
    result
  }
  
  /**
   * Applies filters to a DataFrame
   */
  private def applyFilters(data: DataFrame, filters: Seq[String]): DataFrame = {
    if (filters.isEmpty) {
      data
    } else {
      filters.foldLeft(data) { (df, filter) =>
        logger.debug(s"Applying filter: $filter")
        df.filter(filter)
      }
    }
  }
  
  /**
   * Applies column selection to a DataFrame
   */
  private def applySelect(data: DataFrame, columns: Seq[String]): DataFrame = {
    if (columns.isEmpty) {
      data
    } else {
      logger.debug(s"Selecting columns: ${columns.mkString(", ")}")
      data.select(columns.map(col): _*)
    }
  }
  
  /**
   * Applies join based on strategy
   */
  private def applyJoin(
    parent: DataFrame,
    child: DataFrame,
    joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(s"Applying ${joinConfig.strategy} join with type ${joinConfig.`type`}")
    
    joinConfig.strategy match {
      case "nest" =>
        applyNestJoin(parent, child, joinConfig)
      case "flatten" =>
        applyFlattenJoin(parent, child, joinConfig)
      case "aggregate" =>
        applyAggregateJoin(parent, child, joinConfig)
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported join strategy: ${joinConfig.strategy}")
    }
  }
  
  /**
   * Applies nest join strategy - creates nested array with related records
   */
  private def applyNestJoin(
    parent: DataFrame,
    child: DataFrame,
    joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(s"Applying nest join strategy, nesting as: ${joinConfig.nestAs.getOrElse("nested_records")}")
    
    val nestFieldName = joinConfig.nestAs.getOrElse("nested_records")
    
    // Get join keys from child for grouping
    val childJoinKeys = joinConfig.on.map(_.right)
    
    // Create a struct of all child columns
    val childStruct = struct(child.columns.map(child(_)): _*)
    
    // Group child records by join keys and collect into array
    val groupedChild = child
      .groupBy(childJoinKeys.map(child(_)): _*)
      .agg(collect_list(childStruct).as(nestFieldName))
    
    // Join parent with grouped child
    val joined = parent.join(
      groupedChild,
      joinConfig.on.map { cond =>
        parent(cond.left) === groupedChild(cond.right)
      }.reduce(_ && _),
      joinConfig.`type`
    )
    
    // Drop duplicate join key columns from child and coalesce null arrays to empty arrays
    val columnsToKeep = parent.columns.map(joined(_)) :+ 
      coalesce(joined(nestFieldName), array().cast(joined.schema(nestFieldName).dataType)).as(nestFieldName)
    
    joined.select(columnsToKeep: _*)
  }
  
  /**
   * Applies flatten join strategy - flattens child fields into parent record
   */
  private def applyFlattenJoin(
    parent: DataFrame,
    child: DataFrame,
    joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(s"Applying flatten join strategy with type ${joinConfig.`type`}")
    
    // Get parent and child column names
    val parentColumns = parent.columns.toSet
    val childColumns = child.columns.toSet
    
    // Get join key columns from child side
    val childJoinKeys = joinConfig.on.map(_.right).toSet
    
    // Identify child columns that need to be renamed (excluding join keys)
    val childDataColumns = childColumns -- childJoinKeys
    
    // Rename child columns that conflict with parent columns by prefixing with "child_"
    val renamedChild = childDataColumns.foldLeft(child) { (df, childCol) =>
      if (parentColumns.contains(childCol)) {
        // Conflict: rename with "child_" prefix
        df.withColumnRenamed(childCol, s"child_$childCol")
      } else {
        // No conflict: keep original name
        df
      }
    }
    
    // Perform the join
    val joined = parent.join(
      renamedChild,
      joinConfig.on.map { cond =>
        parent(cond.left) === renamedChild(cond.right)
      }.reduce(_ && _),
      joinConfig.`type`
    )
    
    // Get final column list (parent columns + renamed child data columns, excluding duplicate join keys)
    val finalChildColumns = childDataColumns.map { childCol =>
      if (parentColumns.contains(childCol)) s"child_$childCol" else childCol
    }
    
    val allColumns = parentColumns ++ finalChildColumns
    joined.select(allColumns.toSeq.map(col): _*)
  }
  
  /**
   * Applies aggregate join strategy - aggregates child records with functions
   */
  private def applyAggregateJoin(
    parent: DataFrame,
    child: DataFrame,
    joinConfig: JoinConfig
  ): DataFrame = {
    logger.info(s"Applying aggregate join strategy with ${joinConfig.aggregations.size} aggregations")
    
    // Get join keys from child for grouping
    val childJoinKeys = joinConfig.on.map(_.right)
    
    // Build aggregation expressions
    val aggExprs = joinConfig.aggregations.map { aggSpec =>
      val aggFunc = aggSpec.function.toLowerCase match {
        case "sum" => sum(child(aggSpec.column))
        case "count" => count(child(aggSpec.column))
        case "avg" => avg(child(aggSpec.column))
        case "min" => min(child(aggSpec.column))
        case "max" => max(child(aggSpec.column))
        case other => throw new UnsupportedOperationException(s"Unsupported aggregation function: $other")
      }
      aggFunc.as(aggSpec.alias)
    }
    
    // Group child records by join keys and apply aggregations
    val aggregatedChild = child
      .groupBy(childJoinKeys.map(child(_)): _*)
      .agg(aggExprs.head, aggExprs.tail: _*)
    
    // Join parent with aggregated child
    val result = parent.join(
      aggregatedChild,
      joinConfig.on.map { cond =>
        parent(cond.left) === aggregatedChild(cond.right)
      }.reduce(_ && _),
      joinConfig.`type`
    )
    
    // Drop duplicate join key columns from child
    val columnsToKeep = parent.columns ++ joinConfig.aggregations.map(_.alias)
    result.select(columnsToKeep.map(col): _*)
  }
}

/**
 * DAG execution plan
 */
case class DAGExecutionPlan(
  groups: Seq[DAGExecutionGroup],
  rootNode: String
)

/**
 * Group of DAG nodes that can be executed together
 */
case class DAGExecutionGroup(
  nodes: Seq[DAGNode],
  parallel: Boolean
)

/**
 * Information about an additional table
 */
case class AdditionalTableInfo(
  tableName: String,
  path: String,
  dagMetadata: AdditionalTableMetadata
)

/**
 * Metadata for integrating additional tables into DAG
 */
case class AdditionalTableMetadata(
  primaryKey: Seq[String],
  joinKeys: Map[String, Seq[String]] = Map.empty,
  description: Option[String] = None,
  partitionBy: Seq[String] = Seq.empty
)

/**
 * Exception thrown when circular dependency is detected in DAG
 */
class CircularDependencyException(message: String) extends Exception(message)
