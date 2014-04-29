package de.tu_berlin.impro3.scala.classification.logreg

import _root_.net.sourceforge.argparse4j.inf.Subparser
import _root_.de.tu_berlin.impro3.scala.Algorithm

object LogReg {

  class Config extends Algorithm.Config[LogReg] {

    // algorithm names
    override val CommandName = "log-reg"
    override val Name = "Logistic Regression Classifier"

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)
      // add options (prefixed with --)
      // add defaults for options
    }
  }

}

class LogReg(args: Map[String, Object]) extends Algorithm(args) {

  def run(): Unit = {}
}
