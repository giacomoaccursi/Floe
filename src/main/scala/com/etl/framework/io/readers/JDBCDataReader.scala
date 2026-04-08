package com.etl.framework.io.readers

import com.etl.framework.config.{SchemaConfig, SourceConfig}
import com.etl.framework.exceptions.DataSourceException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class JDBCDataReader(
    sourceConfig: SourceConfig,
    schemaConfig: Option[SchemaConfig] = None
)(implicit spark: SparkSession)
    extends DataReader {

  private val logger = LoggerFactory.getLogger(getClass)

  override def read(): DataFrame = {
    val options = sourceConfig.options
    val url = options.getOrElse(
      "url",
      throw DataSourceException("jdbc", sourceConfig.path, "Missing required option 'url'")
    )

    logger.info(s"Reading JDBC source: ${sourceConfig.path} from $url")

    var reader = spark.read.format("jdbc").option("url", url)

    // Use 'query' option if provided, otherwise use 'path' as dbtable
    if (options.contains("query")) {
      reader = reader.option("dbtable", s"(${options("query")}) AS _subquery")
    } else {
      reader = reader.option("dbtable", sourceConfig.path)
    }

    // Pass through all other options (user, password, driver, fetchSize, etc.)
    val reservedKeys = Set("url", "query")
    options.filterKeys(!reservedKeys.contains(_)).foreach { case (k, v) =>
      reader = reader.option(k, v)
    }

    reader.load()
  }
}
