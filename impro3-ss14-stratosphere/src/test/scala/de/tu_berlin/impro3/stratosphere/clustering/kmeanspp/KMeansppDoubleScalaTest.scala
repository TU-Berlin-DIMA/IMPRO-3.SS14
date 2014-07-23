package de.tu_berlin.impro3.stratosphere.clustering.kmeanspp

import eu.stratosphere.client.LocalExecutor
import org.junit.Test

class KMeansppDoubleScalaTest {

  @Test
  def testX(): Unit = {
    val plan = new KMeansppDoubleScala().getScalaPlan(10, "file://tmp/input.csv", "file://tmp/output.csv", 1, 3, 10)
    LocalExecutor.execute(plan)
  }
}
