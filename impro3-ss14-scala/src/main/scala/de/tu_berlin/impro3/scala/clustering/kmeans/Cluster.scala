package de.tu_berlin.impro3.scala.clustering.kmeans

import _root_.scala.collection.mutable
import _root_.scala.collection.immutable
import _root_.de.tu_berlin.impro3.scala.core.Vector

class Cluster(var center: KVector = null) {

  private val buffer = mutable.ListBuffer[Vector]()
  var points: immutable.List[Vector] = null

  def updateCenter() {
    points = buffer.result()
    buffer.clear()

    if (!points.isEmpty) {
      center = new KVector(points.reduce((a, b) => a + b).components.map(_ / points.size))
    }
  }

  def add(point: Vector) {
    buffer += point
  }

  override def toString = {
    "Cluster[" + center + "] = " + points
  }
}
