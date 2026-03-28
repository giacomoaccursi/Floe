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

  // Validation warning columns
  val WARNINGS = "_warnings"

  // Internal processing columns
  private[validation] val WARNING_MSG = "_warning_msg"
}
