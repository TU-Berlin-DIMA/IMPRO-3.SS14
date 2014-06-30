package de.tu_berlin.impro3.stratosphere.clustering.kmeans

import _root_.eu.stratosphere.client.LocalExecutor
import _root_.eu.stratosphere.api.common.{Program, ProgramDescription}

import _root_.eu.stratosphere.api.scala._
import _root_.eu.stratosphere.api.scala.operators._

class KMeans extends Program with ProgramDescription with Serializable {

  case class Point(x: Double, y: Double) {

    def +(other: Point) = {
      Point(x + other.x, y + other.y)
    }

    def /(div: Int) = {
      Point(x / div, y / div)
    }

    def euclidianDistance(other: Point) = {
      math.sqrt(math.pow(x - other.x, 2) + math.pow(y - other.y, 2))
    }
  }

  def formatCenterOutput = ((p: Point, cid: Int, d: Double) => "%d,%.1f,%.1f".format(cid, p.x, p.y)).tupled

  def getScalaPlan(dop: Int, dataPointInput: String, clusterInput: String, clusterOutput: String, numIterations: Int) = {

    val dataPoints = DataSource(dataPointInput, CsvInputFormat[(Double, Double)]("\n", ' ')).map { case (x, y) => Point(x, y)}

    val clusterPoints = DataSource(clusterInput, CsvInputFormat[(Int, Double, Double)]("\n", ' '))
      .map { case (id, x, y) => (id, Point(x, y))}


    // iterate the K-Means function, starting with the initial cluster points
    val finalCenters = clusterPoints.iterate(numIterations, { clusterPoints =>

      // compute the distance between each point and all current centroids
      val distances = dataPoints cross clusterPoints map { (point, center) =>
        val (dataPoint, (cid, clusterPoint)) = (point, center)
        val distToCluster = dataPoint euclidianDistance clusterPoint
        (dataPoint, cid, distToCluster)
      }

      // pick for each point the closest centroid
      val nearestCenters = distances groupBy { case (point, _, _) => point} reduceGroup { ds => ds.minBy(_._3)}

      // for each centroid, average among all data points that have chosen it as the closest one
      // the average is computed as sum, count, finalized as sum/count
      val nextClusterPoints = nearestCenters
        .map { case (dataPoint, cid, _) => (cid, dataPoint, 1)}
        .groupBy { _._1 }.reduce { (a, b) => (a._1, a._2 + b._2, a._3 + b._3)}
        .map { case (cid, centerPoint, num) => (cid, centerPoint / num)}

      nextClusterPoints
    })

    // compute the distance between each point and all current centroids
    val finalDistances = dataPoints cross finalCenters map { (point, center) =>
      val (dataPoint, (cid, clusterPoint)) = (point, center)
      val distToCluster = dataPoint euclidianDistance clusterPoint
      (dataPoint, cid, distToCluster)
    }

    // pick for each point the closest centroid
    val finalNearestCenters = finalDistances groupBy { case (point, _, _) => point} reduceGroup { ds => ds.minBy(_._3)}

    val output = finalNearestCenters.write(clusterOutput, DelimitedOutputFormat(formatCenterOutput))

    new ScalaPlan(Seq(output), "KMeans")
  }


  /**
   * The program entry point for the packaged version of the program.
   *
   * @param args The command line arguments, including, consisting of the following parameters:
   *             <numSubStasks> <dataPoints> <clusterCenters> <output> <numIterations>"
   * @return The program plan of the kmeans example program.
   */
  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1), args(2), args(3), args(4).toInt)
  }

  override def getDescription() = {
    "Parameters: <numSubStasks> <dataPoints> <clusterCenters> <output> <numIterations>"
  }

}

/**
 * Entry point to make the example standalone runnable with the local executor
 */
object RunKMeans {

  def main(args: Array[String]) {
    val km = new KMeans
    if (args.size < 5) {
      println(km.getDescription)
      return
    }
    val plan = km.getScalaPlan(args(0).toInt, args(1), args(2), args(3), args(4).toInt)
    LocalExecutor.execute(plan)
  }
}
