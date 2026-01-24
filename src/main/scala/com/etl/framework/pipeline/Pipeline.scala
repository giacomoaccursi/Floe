package com.etl.framework.pipeline

import com.etl.framework.orchestration.IngestionResult
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Flexible ETL pipeline that allows executing any combination of phases
 */
class Pipeline[B: Encoder, F: Encoder] private(
  phase1: Option[IngestionPipeline],
  phase2: Option[TransformationPipeline[B]],
  phase3: Option[ExportPipeline[B, F]]
)(implicit spark: SparkSession) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Executes Ingestion phase independently
   * Returns None if Ingestion is not configured
   */
  def executeIngestion(): Option[IngestionResult] = {
    phase1.map { p1 =>
      logger.info("Executing Ingestion phase")
      p1.execute()
    }
  }
  
  /**
   * Executes Transformation phase independently
   * Returns None if Transformation is not configured
   */
  def executeTransformation(): Option[Dataset[B]] = {
    phase2.map { p2 =>
      logger.info("Executing Transformation phase")
      p2.execute()
    }
  }
  
  /**
   * Executes Export phase independently
   * 
   * @param batchModel Optional batch model to use as input. If None, Export will load from configured path
   * @return None if Export is not configured
   */
  def executeExport(batchModel: Option[Dataset[B]] = None): Option[Dataset[F]] = {
    phase3.map { p3 =>
      logger.info("Executing Export phase")
      batchModel match {
        case Some(bm) =>
          logger.info("Using provided batch model as input")
          val mapper = p3.getMapper
          mapper.map(bm)
        case None =>
          logger.info("Loading batch model from configured path")
          p3.execute()
      }
    }
  }
  
  /**
   * Executes all configured phases sequentially with automatic data flow
   * Stops on first failure if stopOnFailure is true
   * 
   * @param stopOnFailure If true, stops execution on first phase failure (default: true)
   * @return PipelineResult containing results from all executed phases
   */
  def executeAll(stopOnFailure: Boolean = true): PipelineResult[B, F] = {
    logger.info("=" * 80)
    logger.info("Starting pipeline execution (all configured phases)")
    logger.info("=" * 80)
    
    val startTime = System.currentTimeMillis()
    
    try {
      // Phase 1: Ingestion
      val ingestionResult = phase1.flatMap { p1 =>
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 1: Ingestion & Validation")
        logger.info("=" * 80)
        val result = p1.execute()
        
        if (!result.success && stopOnFailure) {
          logger.error("Ingestion failed, stopping pipeline execution")
          None  // Signal to stop
        } else {
          logger.info(s"Ingestion completed. Success: ${result.success}, Batch ID: ${result.batchId}")
          Some(result)
        }
      }
      
      // Check if we should stop after Ingestion
      val shouldContinue = phase1.isEmpty || ingestionResult.isDefined || !stopOnFailure
      
      if (!shouldContinue) {
        // Ingestion failed and stopOnFailure is true
        val failedResult = phase1.map(_.execute()).get
        return PipelineResult(
          ingestionResult = Some(failedResult),
          batchModel = None,
          finalModel = None,
          success = false,
          error = Some(s"Ingestion failed: ${failedResult.error.getOrElse("Unknown error")}"),
          executionTimeMs = System.currentTimeMillis() - startTime
        )
      }
      
      // Phase 2: Transformation
      val batchModel = phase2.map { p2 =>
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 2: Transformation")
        logger.info("=" * 80)
        val model = p2.execute()
        logger.info(s"Transformation completed. Records: ${model.count()}")
        model
      }
      
      // Phase 3: Export
      val finalModel = phase3.map { p3 =>
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 3: Export")
        logger.info("=" * 80)
        
        val modelToExport = batchModel match {
          case Some(bm) =>
            logger.info("Using batch model from Transformation phase")
            val mapper = p3.getMapper
            mapper.map(bm)
          case None =>
            logger.info("Loading batch model from configured path")
            p3.execute()
        }
        
        logger.info(s"Export completed. Records: ${modelToExport.count()}")
        modelToExport
      }
      
      val executionTimeMs = System.currentTimeMillis() - startTime
      val success = ingestionResult.forall(_.success)
      
      logger.info("\n" + "=" * 80)
      logger.info(s"Pipeline execution completed. Success: $success")
      logger.info(s"Total execution time: ${executionTimeMs}ms (${executionTimeMs / 1000.0}s)")
      logger.info("=" * 80)
      
      PipelineResult(
        ingestionResult = ingestionResult,
        batchModel = batchModel,
        finalModel = finalModel,
        success = success,
        error = if (success) None else Some("One or more phases failed"),
        executionTimeMs = executionTimeMs
      )
      
    } catch {
      case e: Exception =>
        val executionTimeMs = System.currentTimeMillis() - startTime
        logger.error("Pipeline execution failed with exception", e)
        
        PipelineResult(
          ingestionResult = None,
          batchModel = None,
          finalModel = None,
          success = false,
          error = Some(e.getMessage),
          executionTimeMs = executionTimeMs
        )
    }
  }
  
  /**
   * Executes specific phases in custom order
   * Provides maximum flexibility for custom execution flows
   * 
   * Example:
   * {{{
   * // Execute only Ingestion and Export, skipping Transformation
   * pipeline.executePhases(
   *   ingestion = true,
   *   transformation = false,
   *   export = true
   * )
   * }}}
   * 
   * @param ingestion Execute Ingestion phase
   * @param transformation Execute Transformation phase
   * @param export Execute Export phase
   * @param batchModelForExport Optional batch model to pass to Export
   * @return PipelineResult containing results from executed phases
   */
  def executePhases(
    ingestion: Boolean = true,
    transformation: Boolean = true,
    export: Boolean = true,
    batchModelForExport: Option[Dataset[B]] = None
  ): PipelineResult[B, F] = {
    logger.info("=" * 80)
    logger.info("Starting custom pipeline execution")
    logger.info(s"Phases to execute: Ingestion=$ingestion, Transformation=$transformation, Export=$export")
    logger.info("=" * 80)
    
    val startTime = System.currentTimeMillis()
    
    try {
      val ingestionResult = if (ingestion) executeIngestion() else None
      val batchModel = if (transformation) executeTransformation() else None
      val finalModel = if (export) {
        val modelToUse = batchModelForExport.orElse(batchModel)
        executeExport(modelToUse)
      } else None
      
      val executionTimeMs = System.currentTimeMillis() - startTime
      val success = ingestionResult.forall(_.success)
      
      logger.info("\n" + "=" * 80)
      logger.info(s"Custom pipeline execution completed. Success: $success")
      logger.info(s"Total execution time: ${executionTimeMs}ms")
      logger.info("=" * 80)
      
      PipelineResult(
        ingestionResult = ingestionResult,
        batchModel = batchModel,
        finalModel = finalModel,
        success = success,
        error = None,
        executionTimeMs = executionTimeMs
      )
      
    } catch {
      case e: Exception =>
        val executionTimeMs = System.currentTimeMillis() - startTime
        logger.error("Custom pipeline execution failed", e)
        
        PipelineResult(
          ingestionResult = None,
          batchModel = None,
          finalModel = None,
          success = false,
          error = Some(e.getMessage),
          executionTimeMs = executionTimeMs
        )
    }
  }

  /**
   * Returns the Ingestion pipeline
   */
  def getIngestion: Option[IngestionPipeline] = phase1
  
  /**
   * Returns the Transformation pipeline
   */
  def getTransformation: Option[TransformationPipeline[B]] = phase2
  
  /**
   * Returns the Export pipeline
   */
  def getExport: Option[ExportPipeline[B, F]] = phase3
}

/**
 * Builder for complete Pipeline
 */
class PipelineBuilder[B: Encoder, F: Encoder](implicit spark: SparkSession) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  private var phase1Opt: Option[IngestionPipeline] = None
  private var phase2Opt: Option[TransformationPipeline[B]] = None
  private var phase3Opt: Option[ExportPipeline[B, F]] = None
  
  /**
   * Sets the Ingestion pipeline
   * 
   * @param pipeline Ingestion pipeline
   * @return This builder for chaining
   */
  def withIngestion(pipeline: IngestionPipeline): PipelineBuilder[B, F] = {
    logger.info("Setting Ingestion pipeline")
    this.phase1Opt = Some(pipeline)
    this
  }
  
  /**
   * Configures Ingestion using a builder function
   * 
   * @param builderFn Function that configures IngestionPipelineBuilder
   * @return This builder for chaining
   */
  def withIngestion(builderFn: IngestionPipelineBuilder => IngestionPipeline): PipelineBuilder[B, F] = {
    logger.info("Configuring Ingestion pipeline")
    val phase1Builder = IngestionPipeline.builder()
    this.phase1Opt = Some(builderFn(phase1Builder))
    this
  }
  
  /**
   * Sets the Transformation pipeline
   * 
   * @param pipeline Transformation pipeline
   * @return This builder for chaining
   */
  def withTransformation(pipeline: TransformationPipeline[B]): PipelineBuilder[B, F] = {
    logger.info("Setting Transformation pipeline")
    this.phase2Opt = Some(pipeline)
    this
  }
  
  /**
   * Configures Transformation using a builder function
   * 
   * @param builderFn Function that configures TransformationPipelineBuilder
   * @return This builder for chaining
   */
  def withTransformation(builderFn: TransformationPipelineBuilder[B] => TransformationPipeline[B]): PipelineBuilder[B, F] = {
    logger.info("Configuring Transformation pipeline")
    val phase2Builder = TransformationPipeline.builder[B]()
    this.phase2Opt = Some(builderFn(phase2Builder))
    this
  }
  
  /**
   * Sets the Export pipeline
   * 
   * @param pipeline Export pipeline
   * @return This builder for chaining
   */
  def withExport(pipeline: ExportPipeline[B, F]): PipelineBuilder[B, F] = {
    logger.info("Setting Export pipeline")
    this.phase3Opt = Some(pipeline)
    this
  }
  
  /**
   * Configures Export using a builder function
   * 
   * @param builderFn Function that configures ExportPipelineBuilder
   * @return This builder for chaining
   */
  def withExport(builderFn: ExportPipelineBuilder[B, F] => ExportPipeline[B, F]): PipelineBuilder[B, F] = {
    logger.info("Configuring Export pipeline")
    val phase3Builder = ExportPipeline.builder[B, F]()
    this.phase3Opt = Some(builderFn(phase3Builder))
    this
  }
  
  /**
   * Builds the complete Pipeline
   * At least one phase must be configured
   * 
   * @return Configured Pipeline
   * @throws IllegalStateException if no phases are configured
   */
  def build(): Pipeline[B, F] = {
    logger.info("Building complete Pipeline")
    
    if (phase1Opt.isEmpty && phase2Opt.isEmpty && phase3Opt.isEmpty) {
      throw new IllegalStateException(
        "At least one phase must be configured. Use withIngestion(), withTransformation(), or withExport()"
      )
    }
    
    val configuredPhases = Seq(
      phase1Opt.map(_ => "Ingestion"),
      phase2Opt.map(_ => "Transformation"),
      phase3Opt.map(_ => "Export")
    ).flatten
    
    logger.info(s"Pipeline built with: ${configuredPhases.mkString(", ")}")
    Pipeline.create[B, F](phase1Opt, phase2Opt, phase3Opt)
  }
}

/**
 * Companion object for Pipeline
 */
object Pipeline {
  
  /**
   * Internal factory method to create Pipeline instances
   */
  private[pipeline] def create[B: Encoder, F: Encoder](
    phase1: Option[IngestionPipeline],
    phase2: Option[TransformationPipeline[B]],
    phase3: Option[ExportPipeline[B, F]]
  )(implicit spark: SparkSession): Pipeline[B, F] = {
    new Pipeline[B, F](phase1, phase2, phase3)
  }
  
  /**
   * Creates a new Pipeline builder
   * 
   * @tparam B Batch Model type
   * @tparam F Final Model type
   * @param spark Implicit SparkSession
   * @return New PipelineBuilder
   */
  def builder[B: Encoder, F: Encoder]()(implicit spark: SparkSession): PipelineBuilder[B, F] = {
    new PipelineBuilder[B, F]()
  }
  
  /**
   * Creates a complete Pipeline with all three phases
   * Convenience method for full end-to-end pipelines
   * 
   * @param phase1 Ingestion pipeline
   * @param phase2 Transformation pipeline
   * @param phase3 Export pipeline
   * @param spark Implicit SparkSession
   * @tparam B Batch Model type
   * @tparam F Final Model type
   * @return Pipeline with all three phases configured
   */
  def complete[B: Encoder, F: Encoder](
    phase1: IngestionPipeline,
    phase2: TransformationPipeline[B],
    phase3: ExportPipeline[B, F]
  )(implicit spark: SparkSession): Pipeline[B, F] = {
    new Pipeline[B, F](Some(phase1), Some(phase2), Some(phase3))
  }
}

/**
 * Result of complete pipeline execution
 */
case class PipelineResult[B, F](
  ingestionResult: Option[IngestionResult],
  batchModel: Option[Dataset[B]],
  finalModel: Option[Dataset[F]],
  success: Boolean,
  error: Option[String],
  executionTimeMs: Long
) {
  
  /**
   * Returns the batch ID from Ingestion if available
   */
  def getBatchId: Option[String] = ingestionResult.map(_.batchId)
  
  /**
   * Returns total input records from Ingestion if available
   */
  def getTotalInputRecords: Option[Long] = {
    ingestionResult.map(_.flowResults.map(_.inputRecords).sum)
  }
  
  /**
   * Returns total valid records from Ingestion if available
   */
  def getTotalValidRecords: Option[Long] = {
    ingestionResult.map(_.flowResults.map(_.validRecords).sum)
  }
  
  /**
   * Returns total rejected records from Ingestion if available
   */
  def getTotalRejectedRecords: Option[Long] = {
    ingestionResult.map(_.flowResults.map(_.rejectedRecords).sum)
  }
  
  /**
   * Returns overall rejection rate from Ingestion if available
   */
  def getOverallRejectionRate: Option[Double] = {
    for {
      totalInput <- getTotalInputRecords
      totalRejected <- getTotalRejectedRecords
      if totalInput > 0
    } yield totalRejected.toDouble / totalInput
  }
  
  /**
   * Returns batch model record count if available
   */
  def getBatchModelRecordCount: Option[Long] = {
    batchModel.map(_.count())
  }
  
  /**
   * Returns final model record count if available
   */
  def getFinalModelRecordCount: Option[Long] = {
    finalModel.map(_.count())
  }
  
  /**
   * Returns a summary string of the pipeline execution
   */
  def summary: String = {
    val sb = new StringBuilder
    sb.append("=" * 80).append("\n")
    sb.append("Pipeline Execution Summary\n")
    sb.append("=" * 80).append("\n")
    sb.append(s"Success: $success\n")
    sb.append(s"Execution Time: ${executionTimeMs}ms (${executionTimeMs / 1000.0}s)\n")
    
    error.foreach { err =>
      sb.append(s"Error: $err\n")
    }
    
    getBatchId.foreach { batchId =>
      sb.append(s"\nBatch ID: $batchId\n")
    }
    
    getTotalInputRecords.foreach { count =>
      sb.append(s"Total Input Records: $count\n")
    }
    
    getTotalValidRecords.foreach { count =>
      sb.append(s"Total Valid Records: $count\n")
    }
    
    getTotalRejectedRecords.foreach { count =>
      sb.append(s"Total Rejected Records: $count\n")
    }
    
    getOverallRejectionRate.foreach { rate =>
      sb.append(f"Overall Rejection Rate: ${rate * 100}%.2f%%\n")
    }
    
    getBatchModelRecordCount.foreach { count =>
      sb.append(s"Batch Model Records: $count\n")
    }
    
    getFinalModelRecordCount.foreach { count =>
      sb.append(s"Final Model Records: $count\n")
    }
    
    sb.append("=" * 80)
    sb.toString()
  }
}
