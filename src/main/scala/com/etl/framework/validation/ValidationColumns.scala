package com.etl.framework.validation

/** Column names used in validation and rejection tracking
  */
object ValidationColumns {
  // Rejection tracking columns
  val REJECTION_CODE = "_rejection_code"
  val REJECTION_REASON = "_rejection_reason"
  val REJECTED_AT = "_rejected_at"
  val BATCH_ID = "_batch_id"
  val VALIDATION_STEP = "_validation_step"

  // Warning tracking columns (written to separate Parquet, not to Iceberg)
  val WARNING_RULE = "_warning_rule"
  val WARNING_MESSAGE = "_warning_message"
  val WARNING_COLUMN = "_warning_column"
  val WARNED_AT = "_warned_at"
}
