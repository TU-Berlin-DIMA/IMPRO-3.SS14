package de.tu_berlin.impro3.scala.clustering

import _root_.de.tu_berlin.impro3.scala.Algorithm
import _root_.de.tu_berlin.impro3.scala.clustering.kmeans.KMeans
import _root_.de.tu_berlin.impro3.scala.util.RandomSphere
import org.junit.Test
import java.nio.file.{Files, Paths}

object AlgorithmTest {

  val resourcesPath = Paths.get(getClass.getResource("/dummy.txt").getFile).toAbsolutePath.getParent.toString

  val Delta = 5.0

  def generateData(dimension: Int, scale: Int, cardinality: Int) = {
    val fileName = s"${dimension}_${scale}_$cardinality.txt"
    val absPath = Paths.get(resourcesPath, fileName)
    if (!Files.exists(absPath)) {
      val generator = RandomSphere(dimension, scale, cardinality, absPath.toAbsolutePath.toString)
      generator.run()
    }

    absPath.toAbsolutePath.toString
  }
}

@Test
abstract class AlgorithmTest {

  def test(dimensions: Int, scale: Int, cardinality: Int)(specificArgs: Map[String, Object]) = {
    val inputFile = AlgorithmTest.generateData(dimensions, scale, cardinality)
    val outputFile = s"/tmp/${dimensions}_${scale}_${cardinality}_output.txt"
    val builder = Map.newBuilder[String, Object]
    builder += Tuple2(Algorithm.KEY_INPUT, inputFile)
    builder += Tuple2(Algorithm.KEY_OUTPUT, outputFile)
    builder += Tuple2(Algorithm.KEY_DIMENSIONS, new Integer(dimensions))

    // algorithm specific arguments
    for (arg <- specificArgs) builder += arg

    println("TEST(" + dimensions + ", " + scale + ", " + cardinality + ") _________________________\n")
    getAlgorithm(builder.result()).run()

    validate(dimensions, scale, cardinality, outputFile)
    println()
  }

  def getAlgorithm(args: Map[String, Object]): Algorithm

  def validate(dimensions: Int, scale: Int, cardinality: Int, file: String)
}
