package de.tu_berlin.impro3.scala.classification.naivebayes

import _root_.net.sourceforge.argparse4j.inf.Subparser
import _root_.de.tu_berlin.impro3.scala.Algorithm

object NaiveBayes {

  class Config extends Algorithm.Config[NaiveBayes] {

    // algorithm names
    override val CommandName = "naive-bayes"
    override val Name = "Naive Bayes Classifier"

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)
      // add options (prefixed with --)
      // add defaults for options
    }
  }

}

class NaiveBayes(args: Map[String, Object]) extends Algorithm(args) {

  def run(): Unit = {}
}
