package de.tu_berlin.impro3.scala.common

import de.tu_berlin.impro3.core.Algorithm
import _root_.de.tu_berlin.impro3.scala.ScalaAlgorithm
import _root_.de.tu_berlin.impro3.scala.util.RandomSphere
import net.sourceforge.argparse4j.inf.Namespace
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

  def test(dimensions: Int, scale: Int, cardinality: Int)(specificArgs: java.util.HashMap[String, AnyRef]) = {
    val inputFile = AlgorithmTest.generateData(dimensions, scale, cardinality)
    val outputFile = s"/tmp/${dimensions}_${scale}_${cardinality}_output.txt"
    val args = new java.util.HashMap[String, AnyRef]
    args.put(Algorithm.Command.KEY_INPUT, inputFile)
    args.put(Algorithm.Command.KEY_OUTPUT, outputFile)
    args.put(ScalaAlgorithm.Command.KEY_DIMENSIONS, new Integer(dimensions))

    args.putAll(specificArgs)

    println("TEST(" + dimensions + ", " + scale + ", " + cardinality + ") _________________________\n")
    getAlgorithm(new Namespace(args)).run()

    validate(dimensions, scale, cardinality, outputFile)
    println()
  }

  def getAlgorithm(ns: Namespace): ScalaAlgorithm

  def validate(dimensions: Int, scale: Int, cardinality: Int, file: String)
}
