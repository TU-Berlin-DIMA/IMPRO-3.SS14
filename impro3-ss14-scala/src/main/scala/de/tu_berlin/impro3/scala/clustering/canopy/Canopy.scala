package de.tu_berlin.impro3.scala.clustering.canopy

import _root_.net.sourceforge.argparse4j.inf.Subparser
import _root_.de.tu_berlin.impro3.scala.Algorithm

object Canopy {

  class Config extends Algorithm.Config[Canopy] {

    // algorithm names
    override val CommandName = "canopy"
    override val Name = "Canopy Clustering"

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)
      // add options (prefixed with --)
      // add defaults for options
    }
  }

}

class Canopy(args: Map[String, Object]) extends Algorithm(args) {

  def run(): Unit = {}
}
