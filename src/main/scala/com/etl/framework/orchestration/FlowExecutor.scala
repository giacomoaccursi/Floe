package com.etl.framework.orchestration

import com.etl.framework.config.{DomainsConfig, FlowConfig, GlobalConfig}
import com.etl.framework.core.{AdditionalTableMetadata, TransformationContext}
import com.etl.framework.io.readers.DataReaderFactory
import com.etl.framework.merge.DeltaMergerFactory
import com.etl.framework.util.TimingUtil
import com.etl.framework.validation.{ValidationEngine, ValidationResult}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import java.time.Instant
import scala.collection.mutable
import java.nio.file.{Files, Paths, StandardOpenOption}


/**
 * Executes a single flow through Read → Merge → Validate → Split → Write
 * Works exclusively with DataFrame (no case classes)
 */
class FlowExecutor(
  flowConfig: FlowConfig,
  globalConfig: GlobalConfig,
  validatedFlows: Map[String, DataFrame] = Map.empty,
  domainsConfig: Option[DomainsConfig] = None
)(implicit spark: SparkSession) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Storage for additional tables created during transformations
  private val additionalTables = mutable.Map[String, AdditionalTableInfo]()

  implicit val formats: Formats = Serialization.formats(NoTypeHints)


  /**
   * Executes the complete flow
   */
  def execute(batchId: String): FlowResult = {
    val startTime = System.nanoTime()
    
    try {
      logger.info(s"Starting flow ${flowConfig.name} - batchId: $batchId, loadMode: ${flowConfig.loadMode.`type`}")
      
      // 1. Read data from source
      val rawData = TimingUtil.timed(logger, s"Read ${flowConfig.source.`type`} from ${flowConfig.source.path}") {
        readData()
      }
      val inputCount = rawData.count()
      
      // 2. Apply pre-validation transformations
      val preTransformedData = applyPreValidationTransformations(rawData, batchId)
      
      // 3. Merge with existing data (delta mode)
      val mergedData = TimingUtil.timed(logger, "Merge with existing data") {
        mergeWithExisting(preTransformedData)
      }
      val mergedCount = mergedData.count()
      
      // 4. Validate data
      val validationResult = TimingUtil.timed(logger, "Validate data") {
        validateData(mergedData)
      }
      val validCount = validationResult.valid.count()
      val rejectedCount = validationResult.rejected.map(_.count()).getOrElse(0L)
      
      // Log validation results
      if (rejectedCount > 0) {
        val rejectionRate = (rejectedCount.toDouble / mergedCount * 100)
        logger.warn(f"Validation rejected $rejectedCount/$mergedCount records ($rejectionRate%.2f%%)")
        validationResult.rejectionReasons.foreach { case (reason, count) =>
          logger.warn(s"  - $reason: $count records")
        }
      }
      
      // 5. Apply post-validation transformations
      val postTransformedData = applyPostValidationTransformations(
        validationResult.valid,
        batchId,
        validatedFlows
      )
      
      // 6. Verify invariant: input = valid + rejected
      verifyInvariant(inputCount, validCount, rejectedCount)
      logger.debug(s"Invariant verified: $inputCount = $validCount + $rejectedCount")
      
      // 7. Write validated data
      writeValidated(postTransformedData, batchId)
      
      // 8. Write rejected data
      if (rejectedCount > 0) {
        writeRejected(validationResult.rejected.get, batchId)
      }
      
      // 9. Write additional tables
      writeAdditionalTables(batchId)
      
      // 10. Write metadata
      val executionTimeMs = (System.nanoTime() - startTime) / 1000000
      val result = FlowResult(
        flowName = flowConfig.name,
        batchId = batchId,
        success = true,
        inputRecords = inputCount,
        mergedRecords = mergedCount,
        validRecords = validCount,
        rejectedRecords = rejectedCount,
        rejectionRate = if (inputCount > 0) rejectedCount.toDouble / inputCount else 0.0,
        executionTimeMs = executionTimeMs,
        rejectionReasons = validationResult.rejectionReasons
      )
      
      writeMetadata(result, batchId)
      
      // Log flow summary
      val rejectionRate = if (inputCount > 0) (rejectedCount.toDouble / inputCount * 100) else 0.0
      logger.info(f"Flow ${flowConfig.name} completed in ${executionTimeMs}ms - input: $inputCount, valid: $validCount, rejected: $rejectedCount ($rejectionRate%.2f%%)")
      
      result
      
    } catch {
      case e: InvariantViolationException =>
        val executionTimeMs = (System.nanoTime() - startTime) / 1000000
        logger.error(s"Flow ${flowConfig.name} failed after ${executionTimeMs}ms - invariant violation: ${e.getMessage}", e)
        FlowResult(
          flowName = flowConfig.name,
          batchId = batchId,
          success = false,
          error = Some(s"Invariant violation: ${e.getMessage}")
        )
      
      case e: Exception =>
        val executionTimeMs = (System.nanoTime() - startTime) / 1000000
        logger.error(s"Flow ${flowConfig.name} failed after ${executionTimeMs}ms: ${e.getMessage}", e)
        FlowResult(
          flowName = flowConfig.name,
          batchId = batchId,
          success = false,
          error = Some(e.getMessage)
        )
    }
  }
  
  /**
   * Reads data from source
   */
  private def readData(): DataFrame = {
    logger.debug(s"Creating reader for source type: ${flowConfig.source.`type`}")
    val reader = DataReaderFactory.create(flowConfig.source)
    reader.read()
  }
  
  /**
   * Applies pre-validation transformations
   */
  private def applyPreValidationTransformations(
    data: DataFrame, 
    batchId: String
  ): DataFrame = {
    flowConfig.preValidationTransformation match {
      case Some(transformation) =>
        TimingUtil.timed(logger, "Pre-validation transformation") {
          val inputCount = data.count()
          val context = TransformationContext(
            currentFlow = flowConfig.name,
            currentData = data,
            validatedFlows = Map.empty, // No validated flows available yet
            batchId = batchId,
            spark = spark
          )
          val transformed = transformation(context)
          val outputCount = transformed.count()
          logger.info(s"Pre-validation transformation: $inputCount → $outputCount records")
          transformed
        }
      
      case None =>
        data
    }
  }
  
  /**
   * Merges with existing data (delta mode)
   */
  private def mergeWithExisting(newData: DataFrame): DataFrame = {
    if (flowConfig.loadMode.`type` == "full") {
      // Full load: no merge needed
      logger.debug("Full load mode, skipping merge")
      return newData
    }
    
    // Try to load existing data
    val outputPath = flowConfig.output.path.getOrElse(
      s"${globalConfig.paths.validatedPath}/${flowConfig.name}"
    )
    
    val existingData = try {
      Some(spark.read.parquet(outputPath))
    } catch {
      case _: Exception =>
        logger.info(s"No existing data found at $outputPath, treating as initial load")
        None
    }
    
    val newCount = newData.count()
    val existingCount = existingData.map(_.count()).getOrElse(0L)
    
    // Create merger and merge
    val merger = DeltaMergerFactory.create(flowConfig.loadMode)
    val result = merger.merge(newData, existingData)
    val resultCount = result.count()
    
    logger.info(s"Merge ${flowConfig.loadMode.`type`}: existing $existingCount + new $newCount → result $resultCount records")
    
    result
  }
  
  /**
   * Validates data
   */
  private def validateData(data: DataFrame): ValidationResult = {
    logger.debug(s"Starting validation on ${data.count()} records")
    val engine = new ValidationEngine(domainsConfig)
    engine.validate(data, flowConfig, validatedFlows)
  }
  
  /**
   * Applies post-validation transformations
   */
  private def applyPostValidationTransformations(
    data: DataFrame,
    batchId: String,
    validatedFlows: Map[String, DataFrame]
  ): DataFrame = {
    flowConfig.postValidationTransformation match {
      case Some(transformation) =>
        TimingUtil.timed(logger, "Post-validation transformation") {
          val inputCount = data.count()
          
          // Create context with custom addTable implementation
          val context = new TransformationContext(
            currentFlow = flowConfig.name,
            currentData = data,
            validatedFlows = validatedFlows,
            batchId = batchId,
            spark = spark
          ) {
            override def addTable(
              tableName: String,
              data: DataFrame,
              outputPath: Option[String] = None,
              dagMetadata: Option[AdditionalTableMetadata] = None
            ): Unit = {
              // Store table info for later writing
              additionalTables(tableName) = AdditionalTableInfo(
                tableName = tableName,
                data = data,
                outputPath = outputPath,
                dagMetadata = dagMetadata
              )
            }
          }
          
          val transformed = transformation(context)
          val outputCount = transformed.count()
          logger.info(s"Post-validation transformation: $inputCount → $outputCount records")
          transformed
        }
      
      case None =>
        data
    }
  }

  /**
   * Writes validated data
   */
  private def writeValidated(validData: DataFrame, batchId: String): Unit = {
    val outputPath = flowConfig.output.path.getOrElse(
      s"${globalConfig.paths.validatedPath}/${flowConfig.name}"
    )

    TimingUtil.timed(logger, s"Write validated data to $outputPath") {
      val recordCount = validData.count()

      var writer = validData.write
        .mode(SaveMode.Overwrite)
        .format(flowConfig.output.format)
        .option("compression", flowConfig.output.compression)

      // Apply additional options
      flowConfig.output.options.foreach { case (key, value) =>
        writer = writer.option(key, value)
      }

      // Apply partitioning if configured
      if (flowConfig.output.partitionBy.nonEmpty) {
        writer = writer.partitionBy(flowConfig.output.partitionBy: _*)
      }

      writer.save(outputPath)
      logger.info(s"Wrote $recordCount validated records")
    }
  }

  /**
   * Writes rejected data
   */
  private def writeRejected(rejectedData: DataFrame, batchId: String): Unit = {
    val rejectedPath = flowConfig.output.rejectedPath.getOrElse(
      s"${globalConfig.paths.rejectedPath}/${flowConfig.name}"
    )

    TimingUtil.timed(logger, s"Write rejected data to $rejectedPath") {
      val recordCount = rejectedData.count()

      // Add audit fields
      val rejectedWithAudit = rejectedData
        .withColumn("_rejected_at", lit(Instant.now().toString))
        .withColumn("_batch_id", lit(batchId))

      // Overwrite previous rejected records
      rejectedWithAudit.write
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .save(rejectedPath)

      logger.info(s"Wrote $recordCount rejected records")
    }
  }

  /**
   * Writes additional tables created during transformations
   */
  private def writeAdditionalTables(batchId: String): Unit = {
    additionalTables.foreach { case (tableName, tableInfo) =>
      val outputPath = tableInfo.outputPath.getOrElse(
        s"${globalConfig.paths.validatedPath}/${flowConfig.name}_${tableName}"
      )

      val recordCount = tableInfo.data.count()

      var writer = tableInfo.data.write
        .mode(SaveMode.Overwrite)
        .format("parquet")

      // Apply partitioning if specified in metadata
      tableInfo.dagMetadata.foreach { metadata =>
        if (metadata.partitionBy.nonEmpty) {
          writer = writer.partitionBy(metadata.partitionBy: _*)
        }
      }

      writer.save(outputPath)

      logger.info(s"Additional table $tableName: $recordCount records → $outputPath")

      // Write metadata for the additional table
      writeAdditionalTableMetadata(tableName, tableInfo, outputPath, batchId)
    }
  }

  /**
   * Writes metadata for an additional table
   */
  private def writeAdditionalTableMetadata(
    tableName: String,
    tableInfo: AdditionalTableInfo,
    outputPath: String,
    batchId: String
  ): Unit = {
    val metadataPath = s"${globalConfig.paths.metadataPath}/$batchId/additional_tables/${tableName}.json"

    val metadata = Map(
      "table_name" -> tableName,
      "table_type" -> "additional",
      "created_by_flow" -> flowConfig.name,
      "record_count" -> tableInfo.data.count(),
      "path" -> outputPath,
      "dag_metadata" -> tableInfo.dagMetadata.map { dm =>
        Map(
          "primary_key" -> dm.primaryKey,
          "join_keys" -> dm.joinKeys,
          "description" -> dm.description.getOrElse(""),
          "partition_by" -> dm.partitionBy
        )
      }.getOrElse(Map.empty),
      "schema" -> Map(
        "fields" -> tableInfo.data.schema.fields.map { field =>
          Map(
            "name" -> field.name,
            "type" -> field.dataType.typeName
          )
        }
      )
    )
    
    // Convert to JSON and write

    val jsonString = write(metadata)
    
    // Write to file
    val path = Paths.get(metadataPath)
    Files.createDirectories(path.getParent)
    Files.write(path, jsonString.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    
    logger.debug(s"Additional table metadata written: $tableName → $metadataPath")
  }
  
  /**
   * Writes execution metadata
   */
  private def writeMetadata(result: FlowResult, batchId: String): Unit = {
    val metadataPath = s"${globalConfig.paths.metadataPath}/$batchId/flows/${flowConfig.name}.json"
    
    val metadata = Map(
      "flow_name" -> result.flowName,
      "batch_id" -> result.batchId,
      "success" -> result.success,
      "load_mode" -> flowConfig.loadMode.`type`,
      "input_records" -> result.inputRecords,
      "merged_records" -> result.mergedRecords,
      "valid_records" -> result.validRecords,
      "rejected_records" -> result.rejectedRecords,
      "rejection_rate" -> result.rejectionRate,
      "execution_time_ms" -> result.executionTimeMs,
      "rejection_reasons" -> result.rejectionReasons,
      "error" -> result.error.getOrElse("")
    )
    
    // Convert to JSON and write


    val jsonString = write(metadata)
    
    // Write to file
    val path = Paths.get(metadataPath)
    Files.createDirectories(path.getParent)
    Files.write(path, jsonString.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    
    logger.debug(s"Flow metadata written: ${flowConfig.name} → $metadataPath")
  }
  
  /**
   * Verifies invariant: input = valid + rejected
   */
  private def verifyInvariant(input: Long, valid: Long, rejected: Long): Unit = {
    if (input != valid + rejected) {
      val message = s"Invariant violation in flow ${flowConfig.name}: " +
        s"input_count ($input) != valid_count ($valid) + rejected_count ($rejected)"
      logger.error(message)
      throw new InvariantViolationException(message)
    }
  }
}

/**
 * Result of flow execution
 */
case class FlowResult(
  flowName: String,
  batchId: String,
  success: Boolean,
  inputRecords: Long = 0,
  mergedRecords: Long = 0,
  validRecords: Long = 0,
  rejectedRecords: Long = 0,
  rejectionRate: Double = 0.0,
  executionTimeMs: Long = 0,
  rejectionReasons: Map[String, Long] = Map.empty,
  error: Option[String] = None
)

/**
 * Information about an additional table
 */
private case class AdditionalTableInfo(
  tableName: String,
  data: DataFrame,
  outputPath: Option[String],
  dagMetadata: Option[AdditionalTableMetadata]
)

/**
 * Exception thrown when record invariant is violated
 */
class InvariantViolationException(message: String) extends Exception(message)
