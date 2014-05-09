package de.tu_berlin.impro3.scala.clustering.canopy

import _root_.net.sourceforge.argparse4j.inf.Subparser
import _root_.de.tu_berlin.impro3.scala.Algorithm

object Canopy {

  // argument names
  val KEY_T1 = "T1"
  val KEY_T2 = "T2"

  class Config extends Algorithm.Config[Canopy] {

    // algorithm names
    override val CommandName = "canopy"
    override val Name = "Canopy Clustering"

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)
      // add options (prefixed with --)
      parser.addArgument(s"-${Canopy.KEY_T1}")
        .`type`[Integer](classOf[Integer])
        .dest(Canopy.KEY_T1)
        .metavar("T1")
        .help("T1 threshold")

      parser.addArgument(s"-${Canopy.KEY_T2}")
        .`type`[Integer](classOf[Integer])
        .dest(Canopy.KEY_T2)
        .metavar("T2")
        .help("T2 threshold")

      // add defaults for options
      parser.setDefault(Canopy.KEY_T1, new Integer(7))
      parser.setDefault(Canopy.KEY_T2, new Integer(3))
    }
  }

}

class Canopy(args: Map[String, Object]) extends Algorithm(args) {
  // algorithm specific parameters
  val T1 = arguments.get(Canopy.KEY_T1).get.asInstanceOf[Int]
  val T2 = arguments.get(Canopy.KEY_T2).get.asInstanceOf[Int]
  // Check on parameter constraints
  if (T1 < T2) {
    throw new IllegalArgumentException("T1=" + T1 + " needs to be grater than T2=" + T2)
  }

  def run(): Unit = {}
}
