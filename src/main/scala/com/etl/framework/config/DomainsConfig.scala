package com.etl.framework.config

/**
 * Configuration for domain values (allowed values for validation)
 */
case class DomainsConfig(
  domains: Map[String, DomainConfig]
)

/**
 * Domain configuration with allowed values
 */
case class DomainConfig(
  name: String,
  description: String,
  values: Seq[String],
  caseSensitive: Boolean = true
)
