package de.tu_berlin.impro3.scala.clustering.kmeanspp

import _root_.net.sourceforge.argparse4j.inf.Subparser
import _root_.de.tu_berlin.impro3.scala.Algorithm
import de.tu_berlin.impro3.scala.clustering.kmeans.{Cluster, KVector}
import java.util.Random
import java.nio.file.{Files, Paths, Path}
import java.nio.charset.Charset
import java.io.{IOException, BufferedWriter}

object KMeansPlusPlus {

	// argument names
	val KEY_K = "K"

	// constnats
	val Seed = 543142314133L

  class Config extends Algorithm.Config[KMeansPlusPlus] {

    // algorithm names
    override val CommandName = "k-means++"
    override val Name = "K-Means++ Clustering"

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)
      // add options (prefixed with --)
      // add defaults for options
			parser.addArgument(s"-${KMeansPlusPlus.KEY_K}")
				.`type`[Integer](classOf[Integer])
				.dest(KMeansPlusPlus.KEY_K)
				.metavar("K")
				.help("number of clusters")

			// add defaults for options
			parser.setDefault(KMeansPlusPlus.KEY_K, new Integer(3))
    }
  }

}

class KMeansPlusPlus(args: Map[String, Object]) extends Algorithm(args) {
	// algorithm specific parameters
	val K = arguments.get(KMeansPlusPlus.KEY_K).get.asInstanceOf[Int]

	// extra objects
	val Random = new Random(KMeansPlusPlus.Seed)


	val points: List[KVector] = {
		val p = List.newBuilder[KVector]
		for (i <- iterator()) p += new KVector(i._1)
		p.result()
	}


	def run(): Unit = {
		// initialization
		val in = List.newBuilder[KVector]
		in += points(Random.nextInt(points.size))

		for (i <- 1 until K) {
			val c = in.result()
			var sum = 0.0
			val dis = List.newBuilder[Double]
			for (point <- points) {
				val newarestCluster = c.fold(c.head)((a, b) => {
					if (Math.pow(point euclideanDistance a, 2) <= Math.pow(point euclideanDistance b, 2))
						a
					else
						b
				})
				sum = sum + Math.pow(point euclideanDistance newarestCluster, 2)
				dis += Math.pow(point euclideanDistance newarestCluster, 2)
			}
			var r = Random.nextDouble() * sum
			val distance = dis.result()
			var pos = 0
			while (r > 0) {
				r = r - distance(pos)
				pos = pos + 1
			}
			in += points(pos - 1)
		}
		val centers = in.result()

		//start KMeans
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

		writeToFile(clusters)
	}

	def writeToFile(clusters: IndexedSeq[(Int, Cluster)]) {
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
