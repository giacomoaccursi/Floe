package com.etl.framework.pipeline

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import com.etl.framework.core.FlowTransformation.FlowTransformation
import com.etl.framework.orchestration.{FlowOrchestrator, IngestionResult}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Fluent API builder for Ingestion pipeline (Extract, Load & Validation)
 */
class IngestionPipeline private(
                                 globalConfig: GlobalConfig,
                                 flowConfigs: Seq[FlowConfig],
                                 flowTransformations: Map[String, FlowTransformations]
                               )(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Executes Ingestion pipeline
   * Returns IngestionResult with batch ID and flow results
   */
  def execute(): IngestionResult = {
    logger.info("Executing Ingestion pipeline")

    // Apply transformations to flow configs
    val enrichedFlowConfigs = flowConfigs.map { flowConfig =>
      flowTransformations.get(flowConfig.name) match {
        case Some(transformations) =>
          flowConfig.copy(
            preValidationTransformation = transformations.preValidation,
            postValidationTransformation = transformations.postValidation
          )
        case None =>
          flowConfig
      }
    }

    // Create and execute orchestrator
    val orchestrator = new FlowOrchestrator(globalConfig, enrichedFlowConfigs)
    orchestrator.execute()
  }

  /**
   * Returns the global configuration
   */
  def getGlobalConfig: GlobalConfig = globalConfig

  /**
   * Returns the flow configurations
   */
  def getFlowConfigs: Seq[FlowConfig] = flowConfigs
}


/**
 * Container for flow transformations
 */
private case class FlowTransformations(
                                        preValidation: Option[FlowTransformation],
                                        postValidation: Option[FlowTransformation]
                                      )
