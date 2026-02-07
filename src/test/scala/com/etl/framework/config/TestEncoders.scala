package com.etl.framework.config

import io.circe.{Encoder, Json}
import com.etl.framework.core.FlowTransformation

/** Provides Circe Encoders for Enums to be used in tests. Since Circe is a
  * test-only dependency, we cannot define these in the main ConfigEnums.scala.
  */
object TestEncoders {
  implicit val sourceTypeEncoder: Encoder[SourceType] = Encoder.instance {
    case t: SourceType => Json.fromString(t.name)
  }

  implicit val loadModeEncoder: Encoder[LoadMode] = Encoder.instance {
    case t: LoadMode => Json.fromString(t.name)
  }

  implicit val validationRuleTypeEncoder: Encoder[ValidationRuleType] =
    Encoder.instance { case t: ValidationRuleType =>
      Json.fromString(t.name)
    }

  implicit val onFailureActionEncoder: Encoder[OnFailureAction] =
    Encoder.instance { case t: OnFailureAction =>
      Json.fromString(t.name)
    }

  implicit val fileFormatEncoder: Encoder[FileFormat] = Encoder.instance {
    case t: FileFormat => Json.fromString(t.name)
  }

  implicit val mergeStrategyEncoder: Encoder[MergeStrategy] = Encoder.instance {
    case t: MergeStrategy => Json.fromString(t.name)
  }

  implicit val joinTypeEncoder: Encoder[JoinType] = Encoder.instance {
    case t: JoinType => Json.fromString(t.name)
  }

  implicit val joinStrategyEncoder: Encoder[JoinStrategy] = Encoder.instance {
    case t: JoinStrategy => Json.fromString(t.name)
  }

  implicit val aggregationFunctionEncoder: Encoder[AggregationFunction] =
    Encoder.instance { case t: AggregationFunction =>
      Json.fromString(t.name)
    }

  implicit val flowTransformationEncoder: Encoder[FlowTransformation] =
    Encoder.instance((_: FlowTransformation) =>
      Json.fromString("transformation_function")
    )
}
