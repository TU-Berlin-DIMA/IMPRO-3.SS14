package de.tu_berlin.impro3.scala.classification.randomforest

import scala.util.Random
import _root_.de.tu_berlin.impro3.scala.core.Vector

trait Generator {
  def generate(size: Int): (List[Vector], List[String])
}

class OneDimGenerator(val separator: Double) extends Generator {
  def generate(size: Int): (List[Vector], List[String]) = {
    val vectors: Array[Vector] = Array.fill(size)(Vector(Random.nextDouble()))
    val labels = Array.tabulate(size)(f => if(vectors(f).elementAt(0) > separator) "1" else "-1" )
    (vectors.toList, labels.toList)
  }
}

class TwoDimGenerator(val separatorX: Double, val separatorY: Double) extends Generator {
  def generate(size: Int): (List[Vector], List[String]) = {
    val vectors: Array[Vector] = Array.fill(size)(Vector(Random.nextDouble(), Random.nextDouble()))
    val labels = Array.tabulate(size)(f => if(vectors(f).elementAt(0) > separatorX && vectors(f).elementAt(1) > separatorY) "1" else "-1" )
    (vectors.toList, labels.toList)
  }
}
