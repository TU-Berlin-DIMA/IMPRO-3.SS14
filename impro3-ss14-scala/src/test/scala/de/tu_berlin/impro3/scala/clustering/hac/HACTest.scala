package de.tu_berlin.impro3.scala.clustering.hac

import de.tu_berlin.impro3.scala.clustering.hac._
import de.tu_berlin.impro3.scala.core.Vector
import org.junit.Test
import org.junit.Assert._
import org.junit.Before
import de.tu_berlin.impro3.scala.ScalaAlgorithm
import scala.io.Source
import java.nio.file.{Paths, Files, Path}
import java.nio.charset.Charset
import java.io.{IOException, BufferedWriter}
import de.tu_berlin.impro3.scala.common.AlgorithmTest

object HACTest {
  def validate(dimensions: Int, scale: Int, cardinality: Int, file: String) {
    val reader = Source.fromFile(file).bufferedReader()
    val calculatedCenters = {
      val builder = List.newBuilder[Vector]
      while (reader.ready()) {
        val line = reader.readLine().split( """\""" + ScalaAlgorithm.DELIMITER)
        val vector = Vector((for (i <- 0 until dimensions) yield line(i).trim.toDouble).toList)
        builder += vector
      }
      builder.result()
    }

    for (dim <- 0 until Math.pow(2.0, dimensions).toInt) {
      val perfectCenter = Vector((for (i <- 0 until dimensions) yield if (((dim >> i) & 1) == 1) 1.0 * scale else 0.0).toList)
      val matched = calculatedCenters.filter(
        calculatedCenter => {
          calculatedCenter.euclideanDistance(perfectCenter) <= AlgorithmTest.Delta
        }
      )

      assertEquals("no matching center for: " + perfectCenter, 1, matched.size)
      println("matched center " + perfectCenter + " => " + matched.head + " with [" + matched.head.euclideanDistance(perfectCenter) + "]")
    }
  }
}

class HACTest extends AlgorithmTest {

  def getAlgorithm(args: Map[String, Object]) = {
    new HAC(args)
  }

  @Test
  def testFindingClosestClusters() = {
    val testCluster1: Cluster = new Cluster(List(new DataPoint(24,44), new DataPoint(26,41)))
    val testCluster2: Cluster = new Cluster(List(new DataPoint(57,14), new DataPoint(56,18)))
    val testCluster3: Cluster = new Cluster(List(new DataPoint(20,47), new DataPoint(21,49)))
    val testCluster4: Cluster = new Cluster(List(new DataPoint(57.3,10), new DataPoint(56.4,21)))
  
    val testClusters: List[Cluster] = List(testCluster1, testCluster2, testCluster3, testCluster4)
    
    assertEquals((testCluster2, testCluster4), HAC.compareThemAllFindSmallestDist(testClusters))
  }
  
  @Test
  def testClusterAverage() = {
    val testC: Cluster = new Cluster(List(new DataPoint(1,2), new DataPoint(3.5,7)))
    var testC2: Cluster = new Cluster(List(new DataPoint(1,1)))
    assertEquals(2.25, testC.getAvg().x, 0.00001)
    assertEquals(4.5, testC.getAvg().y, 0.00001)
    
    testC2 = testC.addCluster(testC2)
    
    assertEquals(1.8333333, testC2.getAvg().x, 0.00001)
    assertEquals(3.3333333, testC2.getAvg().y, 0.00001)
  }
  
  @Test
  def testDistance() = {
    val testC: Cluster = new Cluster(List(new DataPoint(1,2), new DataPoint(3.5,7)))
    var testC2: Cluster = new Cluster(List(new DataPoint(1,1)))
    testC2 = testC.addCluster(testC2)
    
    assertEquals(1.238839, testC.distanceTo(testC2), 0.00001)
    assertEquals(1.238839, testC2.distanceTo(testC), 0.00001)
  }

  def validate(dimensions: Int, scale: Int, cardinality: Int, file: String) = HACTest.validate(dimensions, scale, cardinality, file)
}
