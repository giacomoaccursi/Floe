package com.etl.framework.aggregation

import com.etl.framework.config._
import com.etl.framework.TestConfig
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters
import org.apache.spark.sql.SparkSession

/**
 * Property-based tests for DAG node execution order
 * Feature: spark-etl-framework, Property 21: DAG Node Execution Order
 * Validates: Requirements 14.2, 14.3
 * 
 * Test configuration: Uses standardTestCount (20 cases)
 */
object DAGNodeExecutionOrderProperties extends Properties("DAGNodeExecutionOrder") {
  
  // Configure test parameters
  override def overrideParameters(p: Parameters): Parameters = TestConfig.standardParams
  
  // Create a minimal Spark session for testing
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("DAGNodeExecutionOrderPropertiesTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  
  // Generator for node IDs
  val nodeIdGen: Gen[String] = for {
    prefix <- Gen.oneOf("node", "table", "agg", "join")
    suffix <- Gen.choose(1, 100)
  } yield s"${prefix}_$suffix"
  
  // Generator for a DAG node with no dependencies
  def nodeWithNoDependenciesGen(nodeId: String, sourceFlow: String): Gen[DAGNode] = for {
    description <- Gen.alphaNumStr
  } yield DAGNode(
    id = nodeId,
    description = description,
    sourceFlow = sourceFlow,
    sourcePath = s"/data/validated/$sourceFlow",
    dependencies = Seq.empty,  // No dependencies
    join = None,
    select = Seq.empty,
    filters = Seq.empty
  )
  
  // Generator for a DAG node with dependencies on other nodes
  def nodeWithDependenciesGen(
    nodeId: String,
    sourceFlow: String,
    referencedNodes: Seq[String]
  ): Gen[DAGNode] = for {
    description <- Gen.alphaNumStr
    depCount <- Gen.choose(1, Math.min(3, referencedNodes.size))
    selectedDeps <- Gen.pick(depCount, referencedNodes)
    // Optionally add a join configuration
    hasJoin <- Gen.oneOf(true, false)
    joinConfig <- if (hasJoin && selectedDeps.nonEmpty) {
      Gen.const(Some(JoinConfig(
        `type` = "left_outer",
        parent = selectedDeps.head,
        on = Seq(JoinCondition("id", "id")),
        strategy = "nest",
        nestAs = Some(s"${nodeId}_nested"),
        aggregations = Seq.empty
      )))
    } else {
      Gen.const(None)
    }
  } yield DAGNode(
    id = nodeId,
    description = description,
    sourceFlow = sourceFlow,
    sourcePath = s"/data/validated/$sourceFlow",
    dependencies = selectedDeps.toSeq,
    join = joinConfig,
    select = Seq.empty,
    filters = Seq.empty
  )
  
  // Generator for a valid DAG of nodes (no circular dependencies)
  val validNodeDAGGen: Gen[Seq[DAGNode]] = for {
    // Generate 3-10 nodes
    nodeCount <- Gen.choose(3, 10)
    nodeIds <- Gen.listOfN(nodeCount, nodeIdGen).map(_.distinct)
    sourceFlows <- Gen.listOfN(nodeCount, Gen.alphaNumStr.suchThat(_.nonEmpty))
    
    // Create nodes with dependencies forming a DAG
    // First node has no dependencies
    firstNode <- nodeWithNoDependenciesGen(nodeIds.head, sourceFlows.head)
    
    // Subsequent nodes can depend on previous nodes
    restNodes <- nodeIds.tail.zip(sourceFlows.tail).foldLeft(Gen.const(Seq.empty[DAGNode])) { 
      case (genAcc, (nodeId, sourceFlow)) =>
        for {
          acc <- genAcc
          // Can reference any previously created node
          availableRefs = firstNode.id +: acc.map(_.id)
          // Randomly decide if this node has dependencies
          hasDeps <- Gen.oneOf(true, false)
          node <- if (hasDeps && availableRefs.nonEmpty) {
            nodeWithDependenciesGen(nodeId, sourceFlow, availableRefs)
          } else {
            nodeWithNoDependenciesGen(nodeId, sourceFlow)
          }
        } yield acc :+ node
    }
  } yield firstNode +: restNodes
  
  // Generator for AggregationConfig
  def aggregationConfigGen(nodes: Seq[DAGNode]): Gen[AggregationConfig] = Gen.const(
    AggregationConfig(
      name = "test_aggregation",
      description = "Test DAG aggregation",
      version = "1.0",
      batchModel = ModelConfig(
        `class` = "com.example.BatchModel",
        mappingFile = None,
        mapperClass = None
      ),
      finalModel = ModelConfig(
        `class` = "com.example.FinalModel",
        mappingFile = None,
        mapperClass = None
      ),
      output = DAGOutputConfig(
        batch = OutputConfig(
          path = Some("/data/model/batch"),
          rejectedPath = None,
          format = "parquet",
          partitionBy = Seq.empty,
          compression = "snappy",
          options = Map.empty
        ),
        `final` = OutputConfig(
          path = Some("/data/model/final"),
          rejectedPath = None,
          format = "parquet",
          partitionBy = Seq.empty,
          compression = "snappy",
          options = Map.empty
        )
      ),
      nodes = nodes
    )
  )
  
  // Generator for GlobalConfig
  val globalConfigGen: Gen[GlobalConfig] = Gen.const(
    GlobalConfig(
      spark = SparkConfig(
        appName = "test",
        master = "local[*]",
        config = Map.empty
      ),
      paths = PathsConfig(
        inputBase = "/data/input",
        outputBase = "/data/output",
        validatedPath = "/data/validated",
        rejectedPath = "/data/rejected",
        metadataPath = "/data/metadata",
        modelPath = "/data/model",
        stagingPath = "/data/staging",
        checkpointPath = "/data/checkpoint"
      ),
      processing = ProcessingConfig(
        batchIdFormat = "yyyyMMdd_HHmmss",
        executionMode = "batch",
        failOnValidationError = false,
        maxRejectionRate = 0.1,
        rollbackOnFailure = false,
        checkpointEnabled = false,
        checkpointInterval = "5m"
      ),
      performance = PerformanceConfig(
        parallelFlows = false,
        parallelNodes = false,
        broadcastThreshold = 10485760L,
        cacheValidated = false,
        shufflePartitions = 200
      ),
      monitoring = MonitoringConfig(
        enabled = false,
        metricsExporter = None,
        metricsEndpoint = None,
        logLevel = "INFO"
      ),
      security = SecurityConfig(
        encryptionEnabled = false,
        kmsKeyId = None,
        authenticationEnabled = false
      )
    )
  )
  
  /**
   * Property 21: DAG Node Execution Order
   * For any DAG with node dependencies, the execution order should ensure that
   * all dependencies of a node are executed before that node.
   */
  property("dag_execution_order_respects_dependencies") = forAll(validNodeDAGGen, globalConfigGen) {
    (nodes, globalConfig) =>
      val dagConfig = aggregationConfigGen(nodes).sample.get
      val orchestrator = new DAGOrchestrator(dagConfig, globalConfig, autoDiscoverAdditionalTables = false)
      
      try {
        val plan = orchestrator.buildExecutionPlan()
        
        // Flatten all nodes from all groups in execution order
        val executionOrder = plan.groups.flatMap(_.nodes.map(_.id))
        
        // Create a map of node ID to its position in execution order
        val executionPosition = executionOrder.zipWithIndex.toMap
        
        // Verify that for each node, all its dependencies appear before it
        val dependenciesRespected = nodes.forall { node =>
          val nodePosition = executionPosition.getOrElse(node.id, -1)
          
          // Check all dependencies
          node.dependencies.forall { dep =>
            val depPosition = executionPosition.getOrElse(dep, Int.MaxValue)
            depPosition < nodePosition
          }
        }
        
        // Verify all nodes are in the execution order
        val allNodesIncluded = nodes.forall(node => executionPosition.contains(node.id))
        
        dependenciesRespected && allNodesIncluded
        
      } catch {
        case _: CircularDependencyException =>
          // Should not happen with valid DAG
          false
        case e: Exception =>
          println(s"Unexpected exception: ${e.getMessage}")
          e.printStackTrace()
          false
      }
  }
  
  /**
   * Property 21: DAG Dependency Graph Contains All Edges
   * For any set of DAG nodes with dependencies, the dependency graph should contain
   * all dependency relationships.
   */
  property("dag_dependency_graph_contains_all_edges") = forAll(validNodeDAGGen, globalConfigGen) {
    (nodes, globalConfig) =>
      val dagConfig = aggregationConfigGen(nodes).sample.get
      val orchestrator = new DAGOrchestrator(dagConfig, globalConfig, autoDiscoverAdditionalTables = false)
      
      // Use reflection to access private buildDependencyGraph method
      val method = orchestrator.getClass.getDeclaredMethod("buildDependencyGraph", classOf[Seq[DAGNode]])
      method.setAccessible(true)
      val dependencyGraph = method.invoke(orchestrator, nodes).asInstanceOf[Map[String, Set[String]]]
      
      // Verify that all dependencies are in the graph
      val allDepsRepresented = nodes.forall { node =>
        val expectedDependencies = node.dependencies.toSet
        val actualDependencies = dependencyGraph.getOrElse(node.id, Set.empty)
        
        // All dependencies should be in the dependency set
        expectedDependencies.subsetOf(actualDependencies)
      }
      
      // Verify that all nodes are in the graph
      val allNodesInGraph = nodes.forall(node => dependencyGraph.contains(node.id))
      
      allDepsRepresented && allNodesInGraph
  }
  
  /**
   * Property 21: Topological Sort Detects Circular Dependencies in DAG
   * For any DAG nodes with circular dependencies, topological sort should throw CircularDependencyException
   */
  property("dag_topological_sort_detects_circular_dependencies") = {
    // Create nodes with circular dependency: A -> B -> C -> A
    val nodeA = DAGNode(
      id = "node_a",
      description = "Node A",
      sourceFlow = "flow_a",
      sourcePath = "/data/validated/flow_a",
      dependencies = Seq("node_c"),  // Depends on C
      join = None,
      select = Seq.empty,
      filters = Seq.empty
    )
    
    val nodeB = DAGNode(
      id = "node_b",
      description = "Node B",
      sourceFlow = "flow_b",
      sourcePath = "/data/validated/flow_b",
      dependencies = Seq("node_a"),  // Depends on A
      join = None,
      select = Seq.empty,
      filters = Seq.empty
    )
    
    val nodeC = DAGNode(
      id = "node_c",
      description = "Node C",
      sourceFlow = "flow_c",
      sourcePath = "/data/validated/flow_c",
      dependencies = Seq("node_b"),  // Depends on B
      join = None,
      select = Seq.empty,
      filters = Seq.empty
    )
    
    val dagConfig = AggregationConfig(
      name = "circular_test",
      description = "Test circular dependencies",
      version = "1.0",
      batchModel = ModelConfig("com.example.BatchModel", None, None),
      finalModel = ModelConfig("com.example.FinalModel", None, None),
      output = DAGOutputConfig(
        batch = OutputConfig(Some("/data/model/batch"), None, "parquet", Seq.empty, "snappy", Map.empty),
        `final` = OutputConfig(Some("/data/model/final"), None, "parquet", Seq.empty, "snappy", Map.empty)
      ),
      nodes = Seq(nodeA, nodeB, nodeC)
    )
    
    val globalConfig = GlobalConfig(
      spark = SparkConfig("test", "local[*]", Map.empty),
      paths = PathsConfig("/data/input", "/data/output", "/data/validated", "/data/rejected",
        "/data/metadata", "/data/model", "/data/staging", "/data/checkpoint"),
      processing = ProcessingConfig("yyyyMMdd_HHmmss", "batch", false, 0.1, false, false, "5m"),
      performance = PerformanceConfig(false, false, 10485760L, false, 200),
      monitoring = MonitoringConfig(false, None, None, "INFO"),
      security = SecurityConfig(false, None, false)
    )
    
    val orchestrator = new DAGOrchestrator(dagConfig, globalConfig, autoDiscoverAdditionalTables = false)
    
    try {
      orchestrator.buildExecutionPlan()
      // Should have thrown CircularDependencyException
      false
    } catch {
      case _: CircularDependencyException =>
        // Expected
        true
      case e: Exception =>
        println(s"Unexpected exception: ${e.getMessage}")
        false
    }
  }
  
  /**
   * Property 21: Empty Node List Produces Empty Execution Plan
   */
  property("empty_nodes_produces_empty_plan") = forAll(globalConfigGen) { globalConfig =>
    val dagConfig = AggregationConfig(
      name = "empty_test",
      description = "Test empty nodes",
      version = "1.0",
      batchModel = ModelConfig("com.example.BatchModel", None, None),
      finalModel = ModelConfig("com.example.FinalModel", None, None),
      output = DAGOutputConfig(
        batch = OutputConfig(Some("/data/model/batch"), None, "parquet", Seq.empty, "snappy", Map.empty),
        `final` = OutputConfig(Some("/data/model/final"), None, "parquet", Seq.empty, "snappy", Map.empty)
      ),
      nodes = Seq.empty
    )
    
    val orchestrator = new DAGOrchestrator(dagConfig, globalConfig, autoDiscoverAdditionalTables = false)
    
    try {
      val plan = orchestrator.buildExecutionPlan()
      plan.groups.isEmpty
    } catch {
      case _: IllegalStateException =>
        // No root node found - this is acceptable for empty DAG
        true
      case e: Exception =>
        println(s"Unexpected exception: ${e.getMessage}")
        false
    }
  }
  
  /**
   * Property 21: Single Node With No Dependencies Produces Single Group
   */
  property("single_node_produces_single_group") = forAll(nodeIdGen, globalConfigGen) {
    (nodeId, globalConfig) =>
      val node = DAGNode(
        id = nodeId,
        description = "Test node",
        sourceFlow = "test_flow",
        sourcePath = "/data/validated/test_flow",
        dependencies = Seq.empty,
        join = None,
        select = Seq.empty,
        filters = Seq.empty
      )
      
      val dagConfig = AggregationConfig(
        name = "single_node_test",
        description = "Test single node",
        version = "1.0",
        batchModel = ModelConfig("com.example.BatchModel", None, None),
        finalModel = ModelConfig("com.example.FinalModel", None, None),
        output = DAGOutputConfig(
          batch = OutputConfig(Some("/data/model/batch"), None, "parquet", Seq.empty, "snappy", Map.empty),
          `final` = OutputConfig(Some("/data/model/final"), None, "parquet", Seq.empty, "snappy", Map.empty)
        ),
        nodes = Seq(node)
      )
      
      val orchestrator = new DAGOrchestrator(dagConfig, globalConfig, autoDiscoverAdditionalTables = false)
      val plan = orchestrator.buildExecutionPlan()
      
      plan.groups.size == 1 &&
      plan.groups.head.nodes.size == 1 &&
      plan.groups.head.nodes.head.id == nodeId
  }
}
