package de.tu_berlin.impro3.scala

import _root_.de.tu_berlin.impro3.core.Algorithm
import _root_.net.sourceforge.argparse4j.inf.{Namespace, Subparser}

import _root_.scala.io.Source

object ScalaAlgorithm {

  // constants
  val DELIMITER = ","

  abstract class Command[A <: ScalaAlgorithm](name: String, help: String, clazz: Class[A]) extends Algorithm.Command(name, help, clazz) {

    override def setup(parser: Subparser): Unit = {
      // add options (prefixed with --)
      parser.addArgument("--dimensions")
        .`type`[Integer](classOf[Integer])
        .dest(Command.KEY_DIMENSIONS)
        .metavar("N")
        .help("input dimensions")

      // add defaults for options
      parser.setDefault(Command.KEY_DIMENSIONS, new Integer(Command.DEFAULT_ITERATIONS))
    }
  }

  object Command {

    // argument names
    val KEY_DIMENSIONS = "app.dimensions"

    val DEFAULT_ITERATIONS = 3
  }

}

abstract class ScalaAlgorithm(ns: Namespace) extends Algorithm(ns) {

  // common parameters for all algorithms
  val inputPath = ns.get[String](Algorithm.Command.KEY_INPUT)
  val outputPath = ns.get[String](Algorithm.Command.KEY_OUTPUT)

  val dimensions = ns.get[Int](ScalaAlgorithm.Command.KEY_DIMENSIONS)

  def iterator() = new Iterator[(List[Double], List[Double], String)] {
    val reader = Source.fromFile(inputPath).bufferedReader()

    override def hasNext = reader.ready()

    override def next() = {
      val line = reader.readLine().split( """\""" + ScalaAlgorithm.DELIMITER)
      val vector = (for (i <- 0 until dimensions) yield line(i).trim.toDouble).toList
      val center = (for (i <- dimensions until dimensions * 2) yield line(i).trim.toDouble).toList
      val identifier = line(dimensions * 2).trim
      (vector, center, identifier)
    }
  }

  def run()
}
