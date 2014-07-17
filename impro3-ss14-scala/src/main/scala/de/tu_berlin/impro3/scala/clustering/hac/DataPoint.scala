package de.tu_berlin.impro3.scala.clustering.hac

import _root_.scala.util.Random
import _root_.net.sourceforge.argparse4j.inf.Subparser
import _root_.de.tu_berlin.impro3.scala.ScalaAlgorithm
import java.nio.file.{Paths, Files, Path}
import java.nio.charset.Charset
import java.io.{IOException, BufferedWriter}

class DataPoint(var x: Double, var y: Double) {
  override def toString = {
    "x: " + x + " y: " + y
  }
}
