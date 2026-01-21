package com.etl.framework.monitoring

import scala.collection.mutable

/**
 * Metrics collector for tracking framework execution metrics
 * Supports export to various monitoring systems (Prometheus, CloudWatch, Datadog)
 */
trait MetricsCollector {
  
  /**
   * Record a counter metric (monotonically increasing)
   */
  def recordCounter(name: String, value: Long, tags: Map[String, String] = Map.empty): Unit
  
  /**
   * Record a gauge metric (point-in-time value)
   */
  def recordGauge(name: String, value: Double, tags: Map[String, String] = Map.empty): Unit
  
  /**
   * Record a histogram metric (distribution of values)
   */
  def recordHistogram(name: String, value: Double, tags: Map[String, String] = Map.empty): Unit
  
  /**
   * Record a timer metric (duration in milliseconds)
   */
  def recordTimer(name: String, durationMs: Long, tags: Map[String, String] = Map.empty): Unit
  
  /**
   * Increment a counter by 1
   */
  def incrementCounter(name: String, tags: Map[String, String] = Map.empty): Unit = {
    recordCounter(name, 1, tags)
  }
  
  /**
   * Get all collected metrics
   */
  def getMetrics: Map[String, Metric]
  
  /**
   * Reset all metrics
   */
  def reset(): Unit
  
  /**
   * Export metrics to external system
   */
  def export(): Unit
}

/**
 * Metric data structure
 */
sealed trait Metric {
  def name: String
  def tags: Map[String, String]
  def timestamp: Long
}

case class CounterMetric(
  name: String,
  value: Long,
  tags: Map[String, String] = Map.empty,
  timestamp: Long = System.currentTimeMillis()
) extends Metric

case class GaugeMetric(
  name: String,
  value: Double,
  tags: Map[String, String] = Map.empty,
  timestamp: Long = System.currentTimeMillis()
) extends Metric

case class HistogramMetric(
  name: String,
  values: Seq[Double],
  tags: Map[String, String] = Map.empty,
  timestamp: Long = System.currentTimeMillis()
) extends Metric {
  def count: Int = values.size
  def sum: Double = values.sum
  def mean: Double = if (values.nonEmpty) sum / count else 0.0
  def min: Double = if (values.nonEmpty) values.min else 0.0
  def max: Double = if (values.nonEmpty) values.max else 0.0
  def percentile(p: Double): Double = {
    if (values.isEmpty) 0.0
    else {
      val sorted = values.sorted
      val index = (p / 100.0 * sorted.size).toInt
      sorted(math.min(index, sorted.size - 1))
    }
  }
}

case class TimerMetric(
  name: String,
  durationMs: Long,
  tags: Map[String, String] = Map.empty,
  timestamp: Long = System.currentTimeMillis()
) extends Metric

/**
 * In-memory metrics collector (default implementation)
 */
class InMemoryMetricsCollector extends MetricsCollector {
  
  private val counters = mutable.Map[String, CounterMetric]()
  private val gauges = mutable.Map[String, GaugeMetric]()
  private val histograms = mutable.Map[String, mutable.ArrayBuffer[Double]]()
  private val timers = mutable.Map[String, mutable.ArrayBuffer[Long]]()
  private val lock = new Object()
  
  override def recordCounter(name: String, value: Long, tags: Map[String, String] = Map.empty): Unit = {
    lock.synchronized {
      val key = metricKey(name, tags)
      val existing = counters.getOrElse(key, CounterMetric(name, 0, tags))
      counters(key) = existing.copy(value = existing.value + value, timestamp = System.currentTimeMillis())
    }
  }
  
  override def recordGauge(name: String, value: Double, tags: Map[String, String] = Map.empty): Unit = {
    lock.synchronized {
      val key = metricKey(name, tags)
      gauges(key) = GaugeMetric(name, value, tags)
    }
  }
  
  override def recordHistogram(name: String, value: Double, tags: Map[String, String] = Map.empty): Unit = {
    lock.synchronized {
      val key = metricKey(name, tags)
      val values = histograms.getOrElseUpdate(key, mutable.ArrayBuffer[Double]())
      values += value
    }
  }
  
  override def recordTimer(name: String, durationMs: Long, tags: Map[String, String] = Map.empty): Unit = {
    lock.synchronized {
      val key = metricKey(name, tags)
      val durations = timers.getOrElseUpdate(key, mutable.ArrayBuffer[Long]())
      durations += durationMs
    }
  }
  
  override def getMetrics: Map[String, Metric] = {
    lock.synchronized {
      val allMetrics = mutable.Map[String, Metric]()
      
      counters.foreach { case (key, metric) =>
        allMetrics(key) = metric
      }
      
      gauges.foreach { case (key, metric) =>
        allMetrics(key) = metric
      }
      
      histograms.foreach { case (key, values) =>
        val parts = key.split("\\|")
        val name = parts(0)
        val tags = if (parts.length > 1) parseTags(parts(1)) else Map.empty[String, String]
        allMetrics(key) = HistogramMetric(name, values, tags)
      }
      
      timers.foreach { case (key, durations) =>
        val parts = key.split("\\|")
        val name = parts(0)
        val tags = if (parts.length > 1) parseTags(parts(1)) else Map.empty[String, String]
        // Store as histogram for statistical analysis
        allMetrics(key) = HistogramMetric(name, durations.map(_.toDouble), tags)
      }
      
      allMetrics.toMap
    }
  }
  
  override def reset(): Unit = {
    lock.synchronized {
      counters.clear()
      gauges.clear()
      histograms.clear()
      timers.clear()
    }
  }
  
  override def export(): Unit = {
    // Default implementation does nothing
    // Subclasses can override to export to external systems
  }
  
  private def metricKey(name: String, tags: Map[String, String]): String = {
    if (tags.isEmpty) name
    else s"$name|${tags.map { case (k, v) => s"$k:$v" }.mkString(",")}"
  }
  
  private def parseTags(tagStr: String): Map[String, String] = {
    tagStr.split(",").map { pair =>
      val parts = pair.split(":")
      parts(0) -> parts(1)
    }.toMap
  }
}

/**
 * Framework-specific metrics tracker
 */
class FrameworkMetrics(collector: MetricsCollector = new InMemoryMetricsCollector()) {
  
  // Metric names
  object MetricNames {
    // Flow metrics
    val FLOW_EXECUTION_TIME = "etl.flow.execution_time_ms"
    val FLOW_INPUT_RECORDS = "etl.flow.input_records"
    val FLOW_VALID_RECORDS = "etl.flow.valid_records"
    val FLOW_REJECTED_RECORDS = "etl.flow.rejected_records"
    val FLOW_REJECTION_RATE = "etl.flow.rejection_rate"
    val FLOW_SUCCESS = "etl.flow.success"
    val FLOW_FAILURE = "etl.flow.failure"
    
    // Batch metrics
    val BATCH_EXECUTION_TIME = "etl.batch.execution_time_ms"
    val BATCH_FLOWS_PROCESSED = "etl.batch.flows_processed"
    val BATCH_TOTAL_INPUT = "etl.batch.total_input_records"
    val BATCH_TOTAL_VALID = "etl.batch.total_valid_records"
    val BATCH_TOTAL_REJECTED = "etl.batch.total_rejected_records"
    val BATCH_SUCCESS = "etl.batch.success"
    val BATCH_FAILURE = "etl.batch.failure"
    
    // Validation metrics
    val VALIDATION_SCHEMA_ERRORS = "etl.validation.schema_errors"
    val VALIDATION_PK_VIOLATIONS = "etl.validation.pk_violations"
    val VALIDATION_FK_VIOLATIONS = "etl.validation.fk_violations"
    val VALIDATION_RULE_VIOLATIONS = "etl.validation.rule_violations"
    val VALIDATION_WARNINGS = "etl.validation.warnings"
    
    // Data source metrics
    val DATA_READ_TIME = "etl.data.read_time_ms"
    val DATA_READ_RECORDS = "etl.data.read_records"
    val DATA_WRITE_TIME = "etl.data.write_time_ms"
    val DATA_WRITE_RECORDS = "etl.data.write_records"
    
    // Merge metrics
    val MERGE_TIME = "etl.merge.time_ms"
    val MERGE_EXISTING_RECORDS = "etl.merge.existing_records"
    val MERGE_NEW_RECORDS = "etl.merge.new_records"
    val MERGE_RESULT_RECORDS = "etl.merge.result_records"
    
    // Transformation metrics
    val TRANSFORM_TIME = "etl.transform.time_ms"
    val TRANSFORM_INPUT_RECORDS = "etl.transform.input_records"
    val TRANSFORM_OUTPUT_RECORDS = "etl.transform.output_records"
    
    // DAG metrics
    val DAG_NODE_EXECUTION_TIME = "etl.dag.node_execution_time_ms"
    val DAG_NODE_RECORDS = "etl.dag.node_records"
    val DAG_JOIN_TIME = "etl.dag.join_time_ms"
    val DAG_JOIN_PARENT_RECORDS = "etl.dag.join_parent_records"
    val DAG_JOIN_CHILD_RECORDS = "etl.dag.join_child_records"
    val DAG_JOIN_RESULT_RECORDS = "etl.dag.join_result_records"
    
    // System metrics
    val ROLLBACK_COUNT = "etl.system.rollback_count"
    val INVARIANT_VIOLATIONS = "etl.system.invariant_violations"
    val ERRORS = "etl.system.errors"
  }
  
  /**
   * Record flow execution metrics
   */
  def recordFlowExecution(
    flowName: String,
    batchId: String,
    inputRecords: Long,
    validRecords: Long,
    rejectedRecords: Long,
    durationMs: Long,
    success: Boolean
  ): Unit = {
    val tags = Map("flow" -> flowName, "batch" -> batchId)
    
    collector.recordTimer(MetricNames.FLOW_EXECUTION_TIME, durationMs, tags)
    collector.recordCounter(MetricNames.FLOW_INPUT_RECORDS, inputRecords, tags)
    collector.recordCounter(MetricNames.FLOW_VALID_RECORDS, validRecords, tags)
    collector.recordCounter(MetricNames.FLOW_REJECTED_RECORDS, rejectedRecords, tags)
    
    val rejectionRate = if (inputRecords > 0) {
      rejectedRecords.toDouble / inputRecords.toDouble
    } else 0.0
    collector.recordGauge(MetricNames.FLOW_REJECTION_RATE, rejectionRate, tags)
    
    if (success) {
      collector.incrementCounter(MetricNames.FLOW_SUCCESS, tags)
    } else {
      collector.incrementCounter(MetricNames.FLOW_FAILURE, tags)
    }
  }
  
  /**
   * Record batch execution metrics
   */
  def recordBatchExecution(
    batchId: String,
    flowsProcessed: Int,
    totalInput: Long,
    totalValid: Long,
    totalRejected: Long,
    durationMs: Long,
    success: Boolean
  ): Unit = {
    val tags = Map("batch" -> batchId)
    
    collector.recordTimer(MetricNames.BATCH_EXECUTION_TIME, durationMs, tags)
    collector.recordGauge(MetricNames.BATCH_FLOWS_PROCESSED, flowsProcessed.toDouble, tags)
    collector.recordCounter(MetricNames.BATCH_TOTAL_INPUT, totalInput, tags)
    collector.recordCounter(MetricNames.BATCH_TOTAL_VALID, totalValid, tags)
    collector.recordCounter(MetricNames.BATCH_TOTAL_REJECTED, totalRejected, tags)
    
    if (success) {
      collector.incrementCounter(MetricNames.BATCH_SUCCESS, tags)
    } else {
      collector.incrementCounter(MetricNames.BATCH_FAILURE, tags)
    }
  }
  
  /**
   * Record validation metrics
   */
  def recordValidation(
    flowName: String,
    validationType: String,
    violationCount: Long,
    warningCount: Long = 0
  ): Unit = {
    val tags = Map("flow" -> flowName, "type" -> validationType)
    
    validationType match {
      case "schema" =>
        collector.recordCounter(MetricNames.VALIDATION_SCHEMA_ERRORS, violationCount, tags)
      case "pk" =>
        collector.recordCounter(MetricNames.VALIDATION_PK_VIOLATIONS, violationCount, tags)
      case "fk" =>
        collector.recordCounter(MetricNames.VALIDATION_FK_VIOLATIONS, violationCount, tags)
      case _ =>
        collector.recordCounter(MetricNames.VALIDATION_RULE_VIOLATIONS, violationCount, tags)
    }
    
    if (warningCount > 0) {
      collector.recordCounter(MetricNames.VALIDATION_WARNINGS, warningCount, tags)
    }
  }
  
  /**
   * Record data read metrics
   */
  def recordDataRead(
    sourceType: String,
    sourcePath: String,
    recordCount: Long,
    durationMs: Long
  ): Unit = {
    val tags = Map("source_type" -> sourceType, "source_path" -> sourcePath)
    collector.recordTimer(MetricNames.DATA_READ_TIME, durationMs, tags)
    collector.recordCounter(MetricNames.DATA_READ_RECORDS, recordCount, tags)
  }
  
  /**
   * Record data write metrics
   */
  def recordDataWrite(
    outputType: String,
    outputPath: String,
    recordCount: Long,
    durationMs: Long
  ): Unit = {
    val tags = Map("output_type" -> outputType, "output_path" -> outputPath)
    collector.recordTimer(MetricNames.DATA_WRITE_TIME, durationMs, tags)
    collector.recordCounter(MetricNames.DATA_WRITE_RECORDS, recordCount, tags)
  }
  
  /**
   * Record merge metrics
   */
  def recordMerge(
    flowName: String,
    mergeStrategy: String,
    existingRecords: Long,
    newRecords: Long,
    resultRecords: Long,
    durationMs: Long
  ): Unit = {
    val tags = Map("flow" -> flowName, "strategy" -> mergeStrategy)
    collector.recordTimer(MetricNames.MERGE_TIME, durationMs, tags)
    collector.recordGauge(MetricNames.MERGE_EXISTING_RECORDS, existingRecords.toDouble, tags)
    collector.recordGauge(MetricNames.MERGE_NEW_RECORDS, newRecords.toDouble, tags)
    collector.recordGauge(MetricNames.MERGE_RESULT_RECORDS, resultRecords.toDouble, tags)
  }
  
  /**
   * Record transformation metrics
   */
  def recordTransformation(
    flowName: String,
    transformationType: String,
    inputRecords: Long,
    outputRecords: Long,
    durationMs: Long
  ): Unit = {
    val tags = Map("flow" -> flowName, "type" -> transformationType)
    collector.recordTimer(MetricNames.TRANSFORM_TIME, durationMs, tags)
    collector.recordGauge(MetricNames.TRANSFORM_INPUT_RECORDS, inputRecords.toDouble, tags)
    collector.recordGauge(MetricNames.TRANSFORM_OUTPUT_RECORDS, outputRecords.toDouble, tags)
  }
  
  /**
   * Record DAG node execution metrics
   */
  def recordDAGNodeExecution(
    nodeId: String,
    recordCount: Long,
    durationMs: Long
  ): Unit = {
    val tags = Map("node" -> nodeId)
    collector.recordTimer(MetricNames.DAG_NODE_EXECUTION_TIME, durationMs, tags)
    collector.recordGauge(MetricNames.DAG_NODE_RECORDS, recordCount.toDouble, tags)
  }
  
  /**
   * Record join metrics
   */
  def recordJoin(
    parentNode: String,
    childNode: String,
    joinStrategy: String,
    parentRecords: Long,
    childRecords: Long,
    resultRecords: Long,
    durationMs: Long
  ): Unit = {
    val tags = Map(
      "parent" -> parentNode,
      "child" -> childNode,
      "strategy" -> joinStrategy
    )
    collector.recordTimer(MetricNames.DAG_JOIN_TIME, durationMs, tags)
    collector.recordGauge(MetricNames.DAG_JOIN_PARENT_RECORDS, parentRecords.toDouble, tags)
    collector.recordGauge(MetricNames.DAG_JOIN_CHILD_RECORDS, childRecords.toDouble, tags)
    collector.recordGauge(MetricNames.DAG_JOIN_RESULT_RECORDS, resultRecords.toDouble, tags)
  }
  
  /**
   * Record rollback
   */
  def recordRollback(batchId: String, flowCount: Int): Unit = {
    val tags = Map("batch" -> batchId)
    collector.incrementCounter(MetricNames.ROLLBACK_COUNT, tags)
  }
  
  /**
   * Record invariant violation
   */
  def recordInvariantViolation(flowName: String): Unit = {
    val tags = Map("flow" -> flowName)
    collector.incrementCounter(MetricNames.INVARIANT_VIOLATIONS, tags)
  }
  
  /**
   * Record error
   */
  def recordError(errorType: String, errorCode: String): Unit = {
    val tags = Map("type" -> errorType, "code" -> errorCode)
    collector.incrementCounter(MetricNames.ERRORS, tags)
  }
  
  /**
   * Get all metrics
   */
  def getMetrics: Map[String, Metric] = collector.getMetrics
  
  /**
   * Reset all metrics
   */
  def reset(): Unit = collector.reset()
  
  /**
   * Export metrics
   */
  def export(): Unit = collector.export()
}

/**
 * Monitoring hook for external systems
 */
trait MonitoringHook {
  def onFlowStart(flowName: String, batchId: String): Unit
  def onFlowComplete(flowName: String, batchId: String, success: Boolean, durationMs: Long): Unit
  def onBatchStart(batchId: String): Unit
  def onBatchComplete(batchId: String, success: Boolean, durationMs: Long): Unit
  def onError(error: Throwable, context: Map[String, Any]): Unit
}



