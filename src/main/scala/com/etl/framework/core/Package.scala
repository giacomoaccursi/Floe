typackage com.etl.framework

package object core {
  /**
   * Function type for flow transformations
   */
  type FlowTransformation = TransformationContext => org.apache.spark.sql.DataFrame
}
