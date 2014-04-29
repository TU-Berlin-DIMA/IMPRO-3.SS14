package de.tu_berlin.impro3.scala.clustering.hac

import _root_.net.sourceforge.argparse4j.inf.Subparser
import _root_.de.tu_berlin.impro3.scala.Algorithm

object HAC {

  class Config extends Algorithm.Config {

    // algorithm names
    override val CommandName = "hac"
    override val Name = "Hierarchical Agglomerative Clustering"

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)
      // add options (prefixed with --)
      // add defaults for options
    }
  }

}

class HAC(args: Map[String, Object]) extends Algorithm(args) {

  def run(): Unit = {}
}
