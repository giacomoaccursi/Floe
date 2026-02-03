package com.etl.framework.mapping

import com.etl.framework.exceptions.FinalModelMapperLoadException
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Maps Dataset[BatchModel] to Dataset[FinalModel]
 * 
 * This trait defines the contract for transforming the internal Batch Model
 * to the external Final Model used for export and APIs.
 * 
 * Users implement this trait to define custom mapping logic between their
 * BatchModel and FinalModel case classes.
 * 
 * @tparam B BatchModel type
 * @tparam F FinalModel type
 */
trait FinalModelMapper[B, F] {
  
  /**
   * Maps Dataset[BatchModel] to Dataset[FinalModel]
   * 
   * This method should contain the transformation logic to convert
   * the internal batch model to the external final model.
   * 
   * @param batchModel Dataset containing batch model records
   * @return Dataset containing final model records
   */
  def map(batchModel: Dataset[B]): Dataset[F]
}

/**
 * Abstract base class for user-defined Final Model mappers
 * 
 * Users should extend this class and implement the map() method
 * to define their custom mapping logic.
 * 
 * Example:
 * {{{
 * class MyFinalModelMapper extends UserFinalModelMapper[MyBatchModel, MyFinalModel] {
 *   override def map(batchModel: Dataset[MyBatchModel]): Dataset[MyFinalModel] = {
 *     import spark.implicits._
 *     batchModel.map { batch =>
 *       MyFinalModel(
 *         id = batch.id,
 *         name = batch.fullName,
 *         status = batch.status
 *       )
 *     }
 *   }
 * }
 * }}}
 * 
 * @tparam B BatchModel type (must have an Encoder)
 * @tparam F FinalModel type (must have an Encoder)
 */
abstract class UserFinalModelMapper[B: Encoder, F: Encoder](implicit val spark: SparkSession) 
  extends FinalModelMapper[B, F] {
  
  protected val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * User implements this method to define mapping logic
   * 
   * The implementation has full access to:
   * - Spark Dataset APIs (map, flatMap, filter, etc.)
   * - Spark SQL functions
   * - Custom business logic
   * 
   * @param batchModel Dataset containing batch model records
   * @return Dataset containing final model records
   */
  def map(batchModel: Dataset[B]): Dataset[F]
}

/**
 * Companion object for FinalModelMapper utilities
 */
object FinalModelMapper {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Dynamically loads a FinalModelMapper implementation by class name
   * 
   * This method uses reflection to instantiate a user-defined mapper class.
   * The mapper class must have a constructor that accepts a SparkSession.
   * 
   * @param className Fully qualified class name of the mapper
   * @param spark Implicit SparkSession
   * @tparam B BatchModel type
   * @tparam F FinalModel type
   * @return Instance of the mapper
   * @throws FinalModelMapperLoadException if loading fails
   */
  def loadMapper[B, F](className: String)(implicit spark: SparkSession): FinalModelMapper[B, F] = {
    logger.info(s"Loading FinalModelMapper: $className")
    
    try {
      val mapperClass = Class.forName(className)
      
      // Try to find constructor that accepts SparkSession
      try {
        val constructor = mapperClass.getConstructor(classOf[SparkSession])
        val mapper = constructor.newInstance(spark).asInstanceOf[FinalModelMapper[B, F]]
        logger.info(s"Successfully loaded mapper: $className")
        mapper
      } catch {
        case _: NoSuchMethodException =>
          // Try no-arg constructor
          logger.debug(s"No SparkSession constructor found, trying no-arg constructor")
          val constructor = mapperClass.getConstructor()
          val mapper = constructor.newInstance().asInstanceOf[FinalModelMapper[B, F]]
          logger.info(s"Successfully loaded mapper with no-arg constructor: $className")
          mapper
      }
    } catch {
      case e: ClassNotFoundException =>
        logger.error(s"Mapper class not found: $className")
        throw FinalModelMapperLoadException(
          className = className,
          details = "Mapper class not found. Ensure the class is in the classpath.",
          cause = e
        )
      case e: ClassCastException =>
        logger.error(s"Class $className does not implement FinalModelMapper")
        throw FinalModelMapperLoadException(
          className = className,
          details = "Class does not implement FinalModelMapper[B, F]",
          cause = e
        )
      case e: Exception =>
        logger.error(s"Failed to load mapper $className: ${e.getMessage}")
        throw FinalModelMapperLoadException(
          className = className,
          details = e.getMessage,
          cause = e
        )
    }
  }
}

