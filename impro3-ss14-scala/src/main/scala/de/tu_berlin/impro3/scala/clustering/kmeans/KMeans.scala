package de.tu_berlin.impro3.scala.clustering.kmeans

import java.io.{BufferedWriter, IOException}
import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths}

import _root_.de.tu_berlin.impro3.scala.ScalaAlgorithm
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

import _root_.scala.util.Random

object KMeans {

  // argument names
  val KEY_K = "K"

  // constnats
  val Seed = 5431423142056L

  class Command extends ScalaAlgorithm.Command[KMeans]("k-means", "K-Means Clustering", classOf[KMeans]) {

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

class KMeans(ns: Namespace) extends ScalaAlgorithm(ns) {

  // algorithm specific parameters
  val K = ns.get[Int](KMeans.KEY_K)

  // extra objects
  val Random = new Random(KMeans.Seed)

  // algorithm state
  val centers = {
    val builder = Set.newBuilder[KVector]
    for (i <- iterator()) {
      builder += new KVector(i._2)
    }
    builder.result().toList
  }

  val points: List[KVector] = {
    val p = List.newBuilder[KVector]
    for (i <- iterator()) p += new KVector(i._1)
    p.result()
  }

  val clusters = for (i <- 0 until centers.size) yield (i, new Cluster(centers(i)))

  override def run(): Unit = {

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

    writeToFile()
  }

  def writeToFile() {
    val file: Path = Paths.get(outputPath)
    val charSet: Charset = Charset.forName("UTF-8")
    val writer: BufferedWriter = Files.newBufferedWriter(file, charSet)

    try {
      val builder = new StringBuilder
      for (cluster <- clusters) {
        cluster._2.center.addString(builder, "", ", ", ", ")
        for (point <- cluster._2.points) {
          point.addString(builder, "", ", ", ", ")
        }
        builder.delete(builder.size - 2, builder.size)
        writer.write(builder.result())
        writer.newLine()
        builder.clear()
      }
      writer.flush()
    } catch {
      case ex: IOException => println(ex)
    } finally {
      writer.close()
    }
  }
}
