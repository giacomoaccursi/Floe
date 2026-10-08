package com.etl.framework.config

import com.etl.framework.exceptions.ConfigFileException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, PrintWriter}
import scala.util.Using

class DAGConfigLoaderTest extends AnyFlatSpec with Matchers {

  private val loader = new DAGConfigLoader()

  private def withTempFile[A](content: String)(test: File => A): A = {
    val file = File.createTempFile("floe_dag_config", ".yaml")
    try {
      Using.resource(new PrintWriter(file))(_.write(content))
      test(file)
    } finally {
      val _ = file.delete()
    }
  }

  "DAGConfigLoader" should "reject nodes with both sourceFlow and sourceTable" in {
    val yaml =
      """
        |name: ambiguous_source
        |nodes:
        |  - id: customers
        |    sourceFlow: customers
        |    sourceTable: external_catalog.sales.customers
        |""".stripMargin

    withTempFile(yaml) { file =>
      val result = loader.load(file.getAbsolutePath)

      result.left.get shouldBe a[ConfigFileException]
      result.left.get.getMessage should include("exactly one of sourceFlow or sourceTable")
    }
  }
}
