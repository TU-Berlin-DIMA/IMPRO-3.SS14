package de.tu_berlin.impro3.scala.clustering

import _root_.de.tu_berlin.impro3.scala.Algorithm
import _root_.de.tu_berlin.impro3.scala.clustering.kmeans.KMeans
import _root_.de.tu_berlin.impro3.scala.util.RandomSphere
import org.junit.Test
import java.nio.file.{Files, Paths}

object AlgorithmTest {

  val resourcesPath = Paths.get(getClass.getResource("/dummy.txt").getFile).toAbsolutePath.getParent.toString

  val fileDummy = Paths.get(getClass.getResource("/dummy.txt").getFile).toAbsolutePath.toString

  val Delta = 0.1

  private def generateData(dimension: Int, scale: Int, cardinality: Int) = {
    val fileName = s"${dimension}_${scale}_$cardinality.txt"
    val absPath = Paths.get(resourcesPath, fileName)
    if (!Files.exists(absPath)) {
      val generator = RandomSphere(dimension, scale, cardinality, fileDummy, absPath.toAbsolutePath.toString)
      generator.run()
    }

    absPath.toAbsolutePath.toString
  }
}

@Test
abstract class AlgorithmTest {

  def getAlgorithm(args: Map[String, Object]): Algorithm

  @Test
  def test_3_100_10() = {
    val inputFile = AlgorithmTest.generateData(3, 100, 10)
    val outputFile = "/tmp/3_100_10_output.txt"
    val builder = Map.newBuilder[String, Object]
    builder += Tuple2(Algorithm.KEY_INPUT, inputFile)
    builder += Tuple2(Algorithm.KEY_OUTPUT, outputFile)
    builder += Tuple2(Algorithm.KEY_DIMENSIONS, new Integer(3))

    // get rid of algorithm specific arguments
    builder += Tuple2(KMeans.KEY_K, new Integer(Math.pow(2.0, 3).toInt))

    getAlgorithm(builder.result()).run()
  }
}
