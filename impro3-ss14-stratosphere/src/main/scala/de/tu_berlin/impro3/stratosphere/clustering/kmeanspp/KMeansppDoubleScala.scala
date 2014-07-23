package de.tu_berlin.impro3.stratosphere.clustering.kmeanspp


import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription

import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.core.fs.Path
import scala.util.Random


class KMeansppDoubleScala extends Program with ProgramDescription with Serializable {

  case class Point(x: Double, y: Double) {

    def +(other: Point) = {
      Point(x + other.x, y + other.y)
    }

    def /(div: Int) = {
      Point(x / div, y / div)
    }

    def computeEuclidianDistance(other: Point) = {
      math.sqrt(math.pow(x - other.x, 2) + math.pow(y - other.y, 2))
    }
  }

  case class Distance(dataPoint: Point, clusterId: Int, distance: Double)

  def formatCenterOutput = ((cid: Int, p: Point) => "%d|%.1f|%.1f|".format(cid, p.x, p.y)).tupled

  def formatDataPointOutput = ((pid: Int, distance: Distance) => "%d|%.1f|%.1f|".format(distance.clusterId, distance.dataPoint.x, distance.dataPoint.y)).tupled

  def generateNextCenter(eI: Iterator[(Int, Distance)]): (Int, Point) = {
    val eL = eI.toList
    var sum = 0.0
    for (p <- eL) {
      sum = sum + math.pow(p._2.distance, 2)
    }
    var pos = Random.nextDouble() * sum
    var newClusterPoint: (Int, Point) = null
    var flag = false
    for (p <- eL) {
      pos = pos - math.pow(p._2.distance, 2)
      if (pos < 0 && !flag) {
        newClusterPoint = (p._1, p._2.dataPoint)
        flag = true
      }
    }
    newClusterPoint
  }

  def getScalaPlan(dop: Int, dataPointInput: String, clusterOutput: String, n: Int, k: Int, numIterations: Int) = {

    val dataPoints = DataSource(dataPointInput, CsvInputFormat[(Int, Double, Double)]("\n", '|')) map { case (id, x, y) => (id, Point(x, y))}

    // initialization
    val pos = Random.nextInt(n) + 1
    val startCentroid = dataPoints filter { point => point._1 == pos}

    val t = (1 to k + 1).iterator
    val clusterPoints = startCentroid iterate(k - 1, { centers =>

      // compute the distance between each point and all current centroids
      val distances = dataPoints cross centers map { (point, center) =>
        val ((pid, dataPoint), (cid, clusterPoint)) = (point, center)
        val distToCluster = dataPoint.computeEuclidianDistance(clusterPoint)
        (pid, Distance(dataPoint, cid, distToCluster))
      }

      // compute the minimum distance between each point to the current centroids
      val nearestCenter = distances groupBy { case (pid, _) => pid} reduceGroup { ds => ds.minBy(_._2.distance)}

      val nextCenter = nearestCenter reduceAll {
        generateNextCenter
      }

      centers union {
        nextCenter
      } map { center => center}

    }) map { case (id, point) => (t.next(), point)}

    // iterate the K-Means function, starting with the initialized cluster points
    val finalCenters = clusterPoints iterate(numIterations, { centers =>

      // compute the distance between each point and all current centroids
      val distances = dataPoints cross centers map { (point, center) =>
        val ((pid, dataPoint), (cid, clusterPoint)) = (point, center)
        val distToCluster = dataPoint.computeEuclidianDistance(clusterPoint)
        (pid, Distance(dataPoint, cid, distToCluster))
      }

      // pick for each point the closest centroid
      val nearestCenters = distances groupBy { case (pid, _) => pid} reduceGroup { ds => ds.minBy(_._2.distance)}

      // for each centroid, average among all data points that have chosen it as the closest one
      // the average is computed as sum, count, finalized as sum/count
      nearestCenters map { case (_, Distance(dataPoint, cid, _)) => (cid, dataPoint, 1)
      } groupBy {
        _._1
      } reduce { (a, b) => (a._1, a._2 + b._2, a._3 + b._3)
      } map { case (cid, centerPoint, num) => (cid, centerPoint / num)}
    })

    val outputCenters = finalCenters.write(new Path(clusterOutput, "centers").toString, DelimitedOutputFormat(formatCenterOutput))

    val distances = dataPoints cross finalCenters map { (point, center) =>
      val ((pid, dataPoint), (cid, clusterPoint)) = (point, center)
      val distToCluster = dataPoint.computeEuclidianDistance(clusterPoint)
      (pid, Distance(dataPoint, cid, distToCluster))
    }

    // pick for each point the closest centroid
    val nearestCenters = distances groupBy { case (pid, _) => pid} reduceGroup { ds => ds.minBy(_._2.distance)}

    val outputDataPoints = nearestCenters.write(new Path(clusterOutput, "points").toString, DelimitedOutputFormat(formatDataPointOutput))

    val plan = new ScalaPlan(Seq(outputCenters, outputDataPoints), "KMeans++ Iteration")
    plan.setDefaultParallelism(dop)
    plan
  }


  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1), args(2), args(3).toInt, args(4).toInt, args(5).toInt)
  }

  override def getDescription() = {
    "Parameters: <numSubStasks> <dataPoints> <output> <pointsNum> <clustersNum>  <numIterations>"
  }
}