package de.tu_berlin.impro3.scala.clustering.kmeans

import _root_.scala.util.Random
import _root_.net.sourceforge.argparse4j.inf.Subparser
import _root_.de.tu_berlin.impro3.scala.Algorithm

object KMeans {

  // argument names
  val KEY_K = "K"

  // constnats
  val Seed = 5431423142056L

  class Config extends Algorithm.Config[KMeans] {

    // algorithm names
    override val CommandName = "k-means"
    override val Name = "K-Means Clustering"

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)

      // add options (prefixed with --)
      parser.addArgument(s"-${KMeans.KEY_K}")
        .`type`[Integer](classOf[Integer])
        .dest(KMeans.KEY_K)
        .metavar("K")
        .help("number of clusters")

      // add defaults for options
      parser.setDefault(KMeans.KEY_K, new Integer(3))
    }
  }

}

class KMeans(args: Map[String, Object]) extends Algorithm(args) {

  // algorithm specific parameters
  val K = arguments.get(KMeans.KEY_K).get.asInstanceOf[Int]

  // extra objects
  val Random = new Random(KMeans.Seed)

  // algorithm state
  val centers = for (i <- 0 until K) yield {
    val c = List.newBuilder[Double]
    for (j <- 0 until dimensions) {
      c += Random.nextDouble()
    }
    new KVector(c.result())
  }

  val points: List[KVector] = {
    val p = List.newBuilder[KVector]
    for (i <- input) p += new KVector(i._2)
    p.result()
  }

  def run(): Unit = {
    val clusters = for (i <- 0 until centers.size) yield (i, new Cluster(centers(i)))

    var iterate = false
    do {
      iterate = false
      // update clusters for points
      for (point <- points) {
        val nearestCluster = clusters.fold(clusters.head)((a, b) => {
          if (Math.pow(point euclideanDistance a._2.center, 2) <= Math.pow(point euclideanDistance b._2.center, 2))
            a
          else
            b
        })
        iterate |= point.updateID(nearestCluster._1)

        nearestCluster._2.add(point)
      }

      // update cluster centers
      for (cluster <- clusters) {
        cluster._2.updateCenter()
      }
    } while (iterate)

    // FIXME: write clusters to output
  }
}
