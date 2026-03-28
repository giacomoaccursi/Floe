package com.etl.framework

import org.apache.spark.sql.DataFrame

package object core {

  /** Function type for flow transformations
    */
  type FlowTransformation = TransformationContext => TransformationContext
}
