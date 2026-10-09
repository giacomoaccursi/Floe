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

  it should "reject unknown YAML fields instead of ignoring typos" in {
    val yaml =
      """
        |name: typo
        |parallelNdoes: true
        |nodes:
        |  - id: customers
        |    sourceFlow: customers
        |    soruceTable: catalog.ns.customers
        |""".stripMargin

    withTempFile(yaml) { file =>
      val result = loader.load(file.getAbsolutePath)

      result.left.get shouldBe a[ConfigFileException]
      result.left.get.getMessage should include("parallelNdoes")
      result.left.get.getMessage should include("soruceTable")
    }
  }

  it should "reject removed driver-side node parallelism settings" in {
    val yaml =
      """
        |name: obsolete_parallelism
        |parallelNodes: true
        |maxParallelNodes: 8
        |nodes:
        |  - id: customers
        |    sourceFlow: customers
        |""".stripMargin

    withTempFile(yaml) { file =>
      val message = loader.load(file.getAbsolutePath).left.get.getMessage

      message should include("parallelNodes")
      message should include("maxParallelNodes")
    }
  }

  it should "load explicit nest ordering and reject a nest without it" in {
    val validYaml =
      """
        |name: ordered_nest
        |nodes:
        |  - id: orders
        |    sourceFlow: orders
        |  - id: customers
        |    sourceFlow: customers
        |    joins:
        |      - type: left_outer
        |        with: orders
        |        conditions:
        |          - left: customer_id
        |            right: customer_id
        |        strategy: nest
        |        nestAs: orders
        |        orderBy: [created_at, order_id]
        |""".stripMargin

    withTempFile(validYaml) { file =>
      val config = loader.load(file.getAbsolutePath).toOption.get

      config.nodes.last.joins.head.orderBy shouldBe Seq("created_at", "order_id")
    }

    val missingOrderYaml = validYaml.replace("        orderBy: [created_at, order_id]\n", "")
    withTempFile(missingOrderYaml) { file =>
      loader.load(file.getAbsolutePath).left.get.getMessage should include("nest strategy requires orderBy")
    }
  }
}
