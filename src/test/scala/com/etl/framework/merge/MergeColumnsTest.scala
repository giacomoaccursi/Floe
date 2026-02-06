package com.etl.framework.merge

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MergeColumnsTest extends AnyFlatSpec with Matchers {

  "MergeColumns" should "define ROW_NUM constant" in {
    MergeColumns.ROW_NUM shouldBe "_row_num"
  }

  it should "define COMPARE_KEY constant" in {
    MergeColumns.COMPARE_KEY shouldBe "_compare_key"
  }

  it should "define COUNT constant" in {
    MergeColumns.COUNT shouldBe "_count"
  }

  it should "use underscore prefix for internal columns" in {
    MergeColumns.ROW_NUM should startWith("_")
    MergeColumns.COMPARE_KEY should startWith("_")
    MergeColumns.COUNT should startWith("_")
  }
}
