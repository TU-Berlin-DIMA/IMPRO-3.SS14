package de.tu_berlin.impro3.scala.classification.naivebayes

import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import _root_.de.tu_berlin.impro3.scala.ScalaAlgorithm

object NaiveBayes {

  class Command extends ScalaAlgorithm.Command[NaiveBayes]("naive-bayes", "Naive Bayes Classifier", classOf[NaiveBayes]) {

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)
      // add options (prefixed with --)
      // add defaults for options
    }
  }

}

class NaiveBayes(ns: Namespace) extends ScalaAlgorithm(ns) {

  override def run(): Unit = {}
}
