package de.tu_berlin.impro3.scala.classification.randomforest

import java.lang.Double

import de.tu_berlin.impro3.scala.common.AlgorithmTest
import net.sourceforge.argparse4j.inf.Namespace
import org.junit.Test

import scala.io.Source

object RandomForestTest {
  def validate(dimensions: Int, scale: Int, cardinality: Int, file: String) {
    val reader = Source.fromFile(file).bufferedReader()

    val line: String = reader.readLine()
    val oob = line.trim().toDouble

    assert(oob < 0.25) // weak test
  }
}

class RandomForestTest extends AlgorithmTest {

  def getAlgorithm(ns: Namespace) = {
    new RandomForest(ns)
  }

  @Test
  def test_3_100_1000() = {
    test(3, 100, 1000)(Map((RandomForest.KEY_B, new Integer(5)), (RandomForest.KEY_M, new Integer(4)), (RandomForest.KEY_MAX_DEPTH, new Integer(10)), (RandomForest.KEY_RATIO, new Double(0.7))))
  }

  @Test
  def test_4_100_1000() = {
    test(4, 100, 1000)(Map((RandomForest.KEY_B, new Integer(2)), (RandomForest.KEY_M, new Integer(4)), (RandomForest.KEY_MAX_DEPTH, new Integer(10)), (RandomForest.KEY_RATIO, new Double(0.7))))
  }

  def validate(dimensions: Int, scale: Int, cardinality: Int, file: String) = RandomForestTest.validate(dimensions, scale, cardinality, file)
}
