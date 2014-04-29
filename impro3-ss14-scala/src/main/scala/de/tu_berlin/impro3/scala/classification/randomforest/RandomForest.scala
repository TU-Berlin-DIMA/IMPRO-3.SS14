package de.tu_berlin.impro3.scala.classification.randomforest

import _root_.net.sourceforge.argparse4j.inf.Subparser
import _root_.de.tu_berlin.impro3.scala.Algorithm

object RandomForest {

  class Config extends Algorithm.Config[RandomForest] {

    // algorithm names
    override val CommandName = "random-forest"
    override val Name = "Random Forest Classifier"

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)
      // add options (prefixed with --)
      // add defaults for options
    }
  }

}

class RandomForest(args: Map[String, Object]) extends Algorithm(args) {

  def run(): Unit = {}
}
