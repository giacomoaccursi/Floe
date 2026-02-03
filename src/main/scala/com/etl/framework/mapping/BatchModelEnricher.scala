package com.etl.framework.mapping

import com.etl.framework.exceptions.MappingExpressionException
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Supports user-defined enrichment functions on Dataset[BatchModel]
 * Provides full access to Spark Dataset APIs
 */
class BatchModelEnricher[T: Encoder](
  implicit spark: SparkSession
) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Applies user-defined enrichment function to the dataset
   * The enrichment function has full access to Spark Dataset APIs
   */
  def enrich(dataset: Dataset[T], enrichmentFn: BatchModelEnricher.EnrichmentFunction[T]): Dataset[T] = {
    logger.info("Applying user-defined enrichment function to Batch Model")
    
    try {
      val enriched = enrichmentFn(dataset)
      logger.info(s"Enrichment completed. Record count: ${enriched.count()}")
      enriched
    } catch {
      case e: Exception =>
        logger.error(s"Enrichment function failed: ${e.getMessage}")
        throw MappingExpressionException(
          expression = "enrichment function",
          field = "batch model",
          details = e.getMessage,
          cause = e
        )
    }
  }
  
  /**
   * Applies multiple enrichment functions in sequence
   */
  def enrichMultiple(dataset: Dataset[T], enrichmentFns: Seq[BatchModelEnricher.EnrichmentFunction[T]]): Dataset[T] = {
    logger.info(s"Applying ${enrichmentFns.size} enrichment functions to Batch Model")
    
    enrichmentFns.foldLeft(dataset) { (currentDataset, enrichmentFn) =>
      enrich(currentDataset, enrichmentFn)
    }
  }
}

/**
 * Companion object for creating BatchModelEnricher instances
 */
object BatchModelEnricher {
  
  /**
   * Type alias for enrichment functions
   * User-defined functions that transform Dataset[T] -> Dataset[T]
   * These functions have full access to Spark Dataset APIs
   */
  type EnrichmentFunction[T] = Dataset[T] => Dataset[T]
  
  /**
   * Creates a BatchModelEnricher instance
   */
  def apply[T: Encoder](implicit spark: SparkSession): BatchModelEnricher[T] = {
    new BatchModelEnricher[T]()
  }
}
