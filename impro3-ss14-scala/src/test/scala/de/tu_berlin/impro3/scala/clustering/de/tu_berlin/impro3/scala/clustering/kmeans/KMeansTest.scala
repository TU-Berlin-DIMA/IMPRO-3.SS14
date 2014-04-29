package de.tu_berlin.impro3.scala.clustering.de.tu_berlin.impro3.scala.clustering.kmeans

import de.tu_berlin.impro3.scala.clustering.AlgorithmTest
import de.tu_berlin.impro3.scala.clustering.kmeans.KMeans

class KMeansTest extends AlgorithmTest {

  def getAlgorithm(args: Map[String, Object]) = {
    new KMeans(args)
  }
}
