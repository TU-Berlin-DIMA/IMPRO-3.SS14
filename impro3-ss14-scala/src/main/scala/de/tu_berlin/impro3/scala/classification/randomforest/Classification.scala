package de.tu_berlin.impro3.scala.classification.randomforest

import _root_.de.tu_berlin.impro3.scala.core.Vector

trait Classifier {
  def classify(input: Vector): (String, Double)
}

trait Trainer {
  def train(vectors: List[Vector], labels: List[String]): Classifier
}

trait Tester {
  /** Returns confidence matrix */
  def test(testing: List[Vector]): Array[Array[String]]
}
