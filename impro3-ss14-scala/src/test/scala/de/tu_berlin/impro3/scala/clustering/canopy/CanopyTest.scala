package de.tu_berlin.impro3.scala.clustering.canopy

import org.junit.Assert._
import org.junit.Test
import scala.io.Source
import de.tu_berlin.impro3.scala.core.Vector
import de.tu_berlin.impro3.scala.ScalaAlgorithm
import de.tu_berlin.impro3.scala.common.AlgorithmTest

object CanopyTest{
  def validate(dimensions: Int, scale: Int, cardinality: Int, file: String) {
    testAllPointsAreIncluded(dimensions, scale, cardinality, file)
  }

  /**
   * Test whether the CanopyAlgorithm covers all points that are in the input distribution
   */
  def testAllPointsAreIncluded(dimensions: Int, scale: Int, cardinality: Int, file: String){
    val reader = Source.fromFile(file).bufferedReader()
    val calculatedCardinality = {
      val builder = Set.newBuilder[Vector]
      while (reader.ready()) {
        val line = reader.readLine().split( """\""" + ScalaAlgorithm.DELIMITER).grouped(dimensions)
        for (el <- line){
          val vector = Vector((for (i <- 0 until el.length) yield el(i).trim.toDouble).toList)
          builder += vector
        }
      }
      builder.result().size
    }
    println(calculatedCardinality)
    assertEquals("Sizes do not match ", cardinality, calculatedCardinality)
  }
}

class CanopyTest extends AlgorithmTest {

  def getAlgorithm(args: Map[String, Object]) = {
    new Canopy(args)
  }

  @Test
  def test_2_100_1000() = {
    test(2, 100, 1000)(Map(
      (Canopy.KEY_T1, new Integer(7)),(Canopy.KEY_T2, new Integer(3))
    ))
  }

  @Test
  def test_3_100_1000() = {
    test(3, 100, 1000)(Map(
      (Canopy.KEY_T1, new Integer(7)),(Canopy.KEY_T2, new Integer(3))
    ))
  }

  @Test
  def test_4_100_1000() = {
    test(4, 100, 1000)(Map(
      (Canopy.KEY_T1, new Integer(7)),(Canopy.KEY_T2, new Integer(3))
    ))
  }

  @Test
  def test_5_100_1000() = {
    test(5, 100, 1000)(Map(
      (Canopy.KEY_T1, new Integer(7)),(Canopy.KEY_T2, new Integer(3))
    ))
  }

  def validate(dimensions: Int, scale: Int, cardinality: Int, file: String) = CanopyTest.validate(dimensions, scale, cardinality, file)
}
