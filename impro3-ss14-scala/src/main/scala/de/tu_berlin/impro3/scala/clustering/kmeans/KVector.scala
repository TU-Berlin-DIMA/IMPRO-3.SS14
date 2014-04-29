package de.tu_berlin.impro3.scala.clustering.kmeans

import de.tu_berlin.impro3.scala.core.Vector

class KVector(data: List[Double]) extends Vector(data) {
  private var clusterID = -1

  def updateID(clusterID: Int) = {
    val isNewID = this.clusterID != clusterID
    if (isNewID) this.clusterID = clusterID
    isNewID
  }
}
