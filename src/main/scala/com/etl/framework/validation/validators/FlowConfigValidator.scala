package com.etl.framework.validation.validators

import com.etl.framework.config.FlowConfig
import com.etl.framework.validation.{ValidationUtils, Validator}

/**
 * Base class for validators that need access to FlowConfig
 * These validators validate structural aspects (schema, PK, FK, not-null)
 * rather than individual field values
 */
abstract class FlowConfigValidator(
  protected val flowConfig: FlowConfig,
  protected val flowName: Option[String] = None
) extends Validator
