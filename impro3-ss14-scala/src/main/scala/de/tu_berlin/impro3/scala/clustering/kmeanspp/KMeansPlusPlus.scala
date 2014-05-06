package de.tu_berlin.impro3.scala.clustering.kmeanspp

import _root_.net.sourceforge.argparse4j.inf.Subparser
import _root_.de.tu_berlin.impro3.scala.Algorithm

object KMeansPlusPlus {

  class Config extends Algorithm.Config[KMeansPlusPlus] {

    // algorithm names
    override val CommandName = "k-means++"
    override val Name = "K-Means++ Clustering"

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)
      // add options (prefixed with --)
      // add defaults for options
    }
  }

}

class KMeansPlusPlus(args: Map[String, Object]) extends Algorithm(args) {

  def run(): Unit = {}
}
