package de.tu_berlin.impro3.scala.clustering.hac

import scala.math._
import scala.collection._

/**
 * cluster using average link implementation
 */
class Cluster(val data: List[DataPoint]) {
  var avg: DataPoint = {
    var sumX: Double = 0
    var sumY: Double = 0
    
    for(d <- data) {
      sumX += d.x;
      sumY += d.y;
    }
    
    new DataPoint(sumX / data.size, sumY / data.size)
  }
  
  def getAvg() : DataPoint = {
    avg
  }
  
  def getSize() : Int = {
    data.size
  }
  
  def addCluster(cluster: Cluster) : Cluster = {
    new Cluster(data ++ cluster.data)
  }
  
  /**
   * Its the average link implementation
   */
  def distanceTo(cluster: Cluster) : Double = {
    val otherAvg: DataPoint = cluster.getAvg()
    sqrt((avg.x - otherAvg.x) * (avg.x - otherAvg.x) + (avg.y - otherAvg.y) * (avg.y - otherAvg.y))
  }

  override def toString = {
    "|++ Cluster[" + data + "] ++|"
  }
}
