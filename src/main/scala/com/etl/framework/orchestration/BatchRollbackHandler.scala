package com.etl.framework.orchestration

import com.etl.framework.config.{FlowConfig, GlobalConfig}
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

/**
 * Handles rollback of batch execution
 */
class BatchRollbackHandler(
  globalConfig: GlobalConfig,
  flowConfigs: Seq[FlowConfig]
) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Performs rollback of completed flows
   */
  def rollback(batchId: String, results: Seq[FlowResult]): Unit = {
    val flowsToRollback = results.filter(_.success).map(_.flowName)
    logger.warn(s"Rolling back batch $batchId - flows: [${flowsToRollback.mkString(", ")}]")
    
    results.filter(_.success).foreach { result =>
      try {
        val flowConfig = flowConfigs.find(_.name == result.flowName).get
        
        // Delete validated output
        val validatedPath = flowConfig.output.path.getOrElse(
          s"${globalConfig.paths.validatedPath}/${result.flowName}"
        )
        deleteDirectory(validatedPath)
        logger.debug(s"Deleted validated data for ${result.flowName}: $validatedPath")
        
        // Delete rejected output
        val rejectedPath = flowConfig.output.rejectedPath.getOrElse(
          s"${globalConfig.paths.rejectedPath}/${result.flowName}"
        )
        deleteDirectory(rejectedPath)
        logger.debug(s"Deleted rejected data for ${result.flowName}: $rejectedPath")
        
        // For delta mode, we would restore previous state here
        // This is a simplified implementation
        
      } catch {
        case e: Exception =>
          logger.error(s"Error during rollback of flow ${result.flowName}: ${e.getMessage}", e)
      }
    }
  }
  
  /**
   * Deletes a directory recursively
   */
  private def deleteDirectory(path: String): Unit = {
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
}
