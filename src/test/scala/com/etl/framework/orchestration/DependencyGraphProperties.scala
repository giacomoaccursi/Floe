package com.etl.framework.orchestration

import com.etl.framework.config._
import com.etl.framework.TestConfig
import com.etl.framework.orchestration.planning.CircularDependencyException
import org.scalacheck.{Arbitrary, Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters
import org.apache.spark.sql.SparkSession

/**
 * Property-based tests for dependency graph construction and topological sort
 * Feature: spark-etl-framework, Property 4: Dependency Graph Validity
 * Feature: spark-etl-framework, Property 5: Topological Sort Correctness
 * Validates: Requirements 3.1, 3.2
 * 
 * Test configuration: Uses standardTestCount (20 cases)
 */
object DependencyGraphProperties extends Properties("DependencyGraph") {
  
  // Configure test parameters
  override def overrideParameters(p: Parameters): Parameters = TestConfig.standardParams
  
  // Create a minimal Spark session for testing
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("DependencyGraphPropertiesTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  
  // Generator for flow names
  val flowNameGen: Gen[String] = for {
    prefix <- Gen.oneOf("customers", "orders", "products", "transactions", "users", "accounts")
    suffix <- Gen.choose(1, 100)
  } yield s"${prefix}_$suffix"
  
  // Generator for column names
  val columnNameGen: Gen[String] = for {
    name <- Gen.oneOf("id", "customer_id", "order_id", "product_id", "user_id", "account_id")
  } yield name
  
  // Generator for a flow with no dependencies
  def flowWithNoDependenciesGen(flowName: String): Gen[FlowConfig] = for {
    description <- Gen.alphaNumStr
    version <- Gen.oneOf("1.0", "1.1", "2.0")
    owner <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    pkColumn <- columnNameGen
  } yield FlowConfig(
    name = flowName,
    description = description,
    version = version,
    owner = owner,
    source = SourceConfig(
      `type` = "file",
      path = s"/data/input/$flowName",
      format = "csv",
      options = Map.empty,
      filePattern = None
    ),
    schema = SchemaConfig(
      enforceSchema = true,
      allowExtraColumns = false,
      columns = Seq(
        ColumnConfig(pkColumn, "string", nullable = false, None, "Primary key")
      )
    ),
    loadMode = LoadModeConfig(
      `type` = "full",
      keyColumns = Seq.empty
    ),
    validation = ValidationConfig(
      primaryKey = Seq(pkColumn),
      foreignKeys = Seq.empty,  // No dependencies
      rules = Seq.empty
    ),
    output = OutputConfig(
      path = None,
      rejectedPath = None,
      format = "parquet",
      partitionBy = Seq.empty,
      compression = "snappy",
      options = Map.empty
    )
  )
  
  // Generator for a flow with dependencies on other flows
  def flowWithDependenciesGen(flowName: String, referencedFlows: Seq[String]): Gen[FlowConfig] = for {
    description <- Gen.alphaNumStr
    version <- Gen.oneOf("1.0", "1.1", "2.0")
    owner <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    pkColumn <- columnNameGen
    fkCount <- Gen.choose(1, Math.min(3, referencedFlows.size))
    selectedRefs <- Gen.pick(fkCount, referencedFlows)
    foreignKeys = selectedRefs.zipWithIndex.map { case (refFlow, idx) =>
      ForeignKeyConfig(
        name = s"fk_$idx",
        column = s"${refFlow}_id",
        references = ReferenceConfig(
          flow = refFlow,
          column = "id"
        )
      )
    }
  } yield FlowConfig(
    name = flowName,
    description = description,
    version = version,
    owner = owner,
    source = SourceConfig(
      `type` = "file",
      path = s"/data/input/$flowName",
      format = "csv",
      options = Map.empty,
      filePattern = None
    ),
    schema = SchemaConfig(
      enforceSchema = true,
      allowExtraColumns = false,
      columns = ColumnConfig(pkColumn, "string", nullable = false, None, "Primary key") +:
        foreignKeys.map(fk => ColumnConfig(fk.column, "string", nullable = false, None, "Foreign key"))
    ),
    loadMode = LoadModeConfig(
      `type` = "full",
      keyColumns = Seq.empty
    ),
    validation = ValidationConfig(
      primaryKey = Seq(pkColumn),
      foreignKeys = foreignKeys.toSeq,
      rules = Seq.empty
    ),
    output = OutputConfig(
      path = None,
      rejectedPath = None,
      format = "parquet",
      partitionBy = Seq.empty,
      compression = "snappy",
      options = Map.empty
    )
  )
  
  // Generator for a valid DAG of flows (no circular dependencies)
  val validFlowDAGGen: Gen[Seq[FlowConfig]] = for {
    // Generate 3-10 flows
    flowCount <- Gen.choose(3, 10)
    flowNames <- Gen.listOfN(flowCount, flowNameGen).map(_.distinct)
    
    // Create flows with dependencies forming a DAG
    // First flow has no dependencies
    firstFlow <- flowWithNoDependenciesGen(flowNames.head)
    
    // Subsequent flows can depend on previous flows
    restFlows <- flowNames.tail.foldLeft(Gen.const(Seq.empty[FlowConfig])) { (genAcc, flowName) =>
      for {
        acc <- genAcc
        // Can reference any previously created flow
        availableRefs = firstFlow.name +: acc.map(_.name)
        // Randomly decide if this flow has dependencies
        hasDeps <- Gen.oneOf(true, false)
        flow <- if (hasDeps && availableRefs.nonEmpty) {
          flowWithDependenciesGen(flowName, availableRefs)
        } else {
          flowWithNoDependenciesGen(flowName)
        }
      } yield acc :+ flow
    }
  } yield firstFlow +: restFlows
  
  // Generator for GlobalConfig
  val globalConfigGen: Gen[GlobalConfig] = Gen.const(
    GlobalConfig(
      paths = PathsConfig(
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
   * Property 4: Dependency Graph Validity
   * For any set of flows with Foreign Key relationships, the Flow_Orchestrator should build
   * a dependency graph where every FK dependency is represented as an edge.
   */
  property("dependency_graph_contains_all_fk_edges") = forAll(validFlowDAGGen, globalConfigGen) { 
    (flows, globalConfig) =>
      // Use DependencyGraphBuilder directly
      val builder = new com.etl.framework.orchestration.planning.DependencyGraphBuilder(flows)
      val dependencyGraph = builder.buildGraph()
      
      // Verify that all FK relationships are in the graph
      val allFKsRepresented = flows.forall { flow =>
        val expectedDependencies = flow.validation.foreignKeys.map(_.references.flow).toSet
        val actualDependencies = dependencyGraph.getOrElse(flow.name, Set.empty)
        
        // All FK references should be in the dependency set
        expectedDependencies.subsetOf(actualDependencies)
      }
      
      // Verify that all flows are in the graph
      val allFlowsInGraph = flows.forall(flow => dependencyGraph.contains(flow.name))
      
      allFKsRepresented && allFlowsInGraph
  }
  
  /**
   * Property 5: Topological Sort Correctness
   * For any valid dependency graph, the topological sort should produce an execution order
   * where all dependencies of a flow are executed before that flow.
   */
  property("topological_sort_respects_dependencies") = forAll(validFlowDAGGen, globalConfigGen) {
    (flows, globalConfig) =>
      val orchestrator = new FlowOrchestrator(globalConfig, flows)
      
      try {
        val plan = orchestrator.buildExecutionPlan()
        
        // Flatten all flows from all groups in execution order
        val executionOrder = plan.groups.flatMap(_.flows.map(_.name))
        
        // Create a map of flow name to its position in execution order
        val executionPosition = executionOrder.zipWithIndex.toMap
        
        // Verify that for each flow, all its dependencies appear before it
        val dependenciesRespected = flows.forall { flow =>
          val flowPosition = executionPosition.getOrElse(flow.name, -1)
          
          // Check all FK dependencies
          flow.validation.foreignKeys.forall { fk =>
            val depPosition = executionPosition.getOrElse(fk.references.flow, Int.MaxValue)
            depPosition < flowPosition
          }
        }
        
        // Verify all flows are in the execution order
        val allFlowsIncluded = flows.forall(flow => executionPosition.contains(flow.name))
        
        dependenciesRespected && allFlowsIncluded
        
      } catch {
        case _: CircularDependencyException =>
          // Should not happen with valid DAG
          false
        case e: Exception =>
          println(s"Unexpected exception: ${e.getMessage}")
          false
      }
  }
  
  /**
   * Property 5: Topological Sort Detects Circular Dependencies
   * For any flows with circular dependencies, topological sort should throw CircularDependencyException
   */
  property("topological_sort_detects_circular_dependencies") = {
    // Create flows with circular dependency: A -> B -> C -> A
    val flowA = FlowConfig(
      name = "flow_a",
      description = "Flow A",
      version = "1.0",
      owner = "test",
      source = SourceConfig("file", "/data/input/a", "csv", Map.empty, None),
      schema = SchemaConfig(true, false, Seq(
        ColumnConfig("id", "string", false, None, "PK"),
        ColumnConfig("c_id", "string", false, None, "FK to C")
      )),
      loadMode = LoadModeConfig("full", Seq.empty),
      validation = ValidationConfig(
        primaryKey = Seq("id"),
        foreignKeys = Seq(
          ForeignKeyConfig("fk_c", "c_id", ReferenceConfig("flow_c", "id"))
        ),
        rules = Seq.empty
      ),
      output = OutputConfig(None, None, "parquet", Seq.empty, "snappy", Map.empty)
    )
    
    val flowB = FlowConfig(
      name = "flow_b",
      description = "Flow B",
      version = "1.0",
      owner = "test",
      source = SourceConfig("file", "/data/input/b", "csv", Map.empty, None),
      schema = SchemaConfig(true, false, Seq(
        ColumnConfig("id", "string", false, None, "PK"),
        ColumnConfig("a_id", "string", false, None, "FK to A")
      )),
      loadMode = LoadModeConfig("full", Seq.empty),
      validation = ValidationConfig(
        primaryKey = Seq("id"),
        foreignKeys = Seq(
          ForeignKeyConfig("fk_a", "a_id", ReferenceConfig("flow_a", "id"))
        ),
        rules = Seq.empty
      ),
      output = OutputConfig(None, None, "parquet", Seq.empty, "snappy", Map.empty)
    )
    
    val flowC = FlowConfig(
      name = "flow_c",
      description = "Flow C",
      version = "1.0",
      owner = "test",
      source = SourceConfig("file", "/data/input/c", "csv", Map.empty, None),
      schema = SchemaConfig(true, false, Seq(
        ColumnConfig("id", "string", false, None, "PK"),
        ColumnConfig("b_id", "string", false, None, "FK to B")
      )),
      loadMode = LoadModeConfig("full", Seq.empty),
      validation = ValidationConfig(
        primaryKey = Seq("id"),
        foreignKeys = Seq(
          ForeignKeyConfig("fk_b", "b_id", ReferenceConfig("flow_b", "id"))
        ),
        rules = Seq.empty
      ),
      output = OutputConfig(None, None, "parquet", Seq.empty, "snappy", Map.empty)
    )
    
    val globalConfig = GlobalConfig(
      paths = PathsConfig("/data/validated", "/data/rejected",
        "/data/metadata", "/data/model", "/data/staging", "/data/checkpoint"),
      processing = ProcessingConfig("yyyyMMdd_HHmmss", "batch", false, 0.1, false, "5m"),
      performance = PerformanceConfig(false, false, 10485760L, false, 200),
      monitoring = MonitoringConfig(false, None, None, "INFO"),
      security = SecurityConfig(false, None, false)
    )
    
    val orchestrator = new FlowOrchestrator(globalConfig, Seq(flowA, flowB, flowC))
    
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
   * Property 5: Empty Flow List Produces Empty Execution Plan
   */
  property("empty_flows_produces_empty_plan") = forAll(globalConfigGen) { globalConfig =>
    val orchestrator = new FlowOrchestrator(globalConfig, Seq.empty)
    val plan = orchestrator.buildExecutionPlan()
    
    plan.groups.isEmpty
  }
  
  /**
   * Property 5: Single Flow With No Dependencies Produces Single Group
   */
  property("single_flow_produces_single_group") = forAll(flowNameGen, globalConfigGen) { 
    (flowName, globalConfig) =>
      val flow = flowWithNoDependenciesGen(flowName).sample.getOrElse(
        // Fallback flow if generation fails
        FlowConfig(
          name = flowName,
          description = "Test flow",
          version = "1.0",
          owner = "test",
          source = SourceConfig("file", s"/data/input/$flowName", "csv", Map.empty, None),
          schema = SchemaConfig(true, false, Seq(ColumnConfig("id", "string", false, None, "PK"))),
          loadMode = LoadModeConfig("full", Seq.empty),
          validation = ValidationConfig(Seq("id"), Seq.empty, Seq.empty),
          output = OutputConfig(None, None, "parquet", Seq.empty, "snappy", Map.empty)
        )
      )
      
      val orchestrator = new FlowOrchestrator(globalConfig, Seq(flow))
      val plan = orchestrator.buildExecutionPlan()
      
      plan.groups.size == 1 && 
      plan.groups.head.flows.size == 1 &&
      plan.groups.head.flows.head.name == flowName
  }
}

