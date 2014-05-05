package de.tu_berlin.impro3.scala.util

import _root_.scala.collection.JavaConverters._
import scala.util.Random
import scala.collection.mutable
import scala.collection.immutable.HashMap
import java.nio.file.{Paths, Files, Path}
import java.nio.charset.Charset
import java.io.{IOException, BufferedWriter}
import de.tu_berlin.impro3.scala.Algorithm
import net.sourceforge.argparse4j.inf.Subparser

object RandomSphere {

  val SEED = 23454638945312L

  val Delimiter = ","

  def apply(dimensions: Int, scale: Int, cardinality: Int, inputFile: String, outputFile: String) = {
    val builder = Map.newBuilder[String, Object]
    builder += Tuple2(Algorithm.KEY_DIMENSIONS, dimensions.asInstanceOf[Object])
    builder += Tuple2(Algorithm.KEY_INPUT, inputFile)
    builder += Tuple2(Algorithm.KEY_OUTPUT, outputFile)
    builder += Tuple2(RandomSphere.KEY_SCALE, scale.asInstanceOf[Object])
    builder += Tuple2(RandomSphere.KEY_CARDINALITY, cardinality.asInstanceOf[Object])
    new RandomSphere(builder.result())
  }

  def writeToDisk(absPath: String, values: HashMap[List[Double], List[List[Double]]]) {
    val file: Path = Paths.get(absPath)
    val charSet: Charset = Charset.forName("UTF-8")
    val writer: BufferedWriter = Files.newBufferedWriter(file, charSet)
    try {
      for ((key, value) <- values) {
        val builder = new StringBuilder
        for (point <- value) {
          point.addString(builder, "", ", ", ", ")
          key.addString(builder, "", ", ", ", ")
          key.map(_.toInt).addString(builder, "|")
          writer.write(builder.result())
          writer.newLine()
          builder.clear()
        }
      }
      writer.flush()
    } catch {
      case ex: IOException => println(ex)
    } finally {
      writer.close()
    }
  }

  // argument names
  val KEY_SCALE = "S"

  val KEY_CARDINALITY = "C"

  class Config extends Algorithm.Config[RandomSphere] {

    // algorithm names
    override val CommandName = "sphereGenerator"
    override val Name = "Random sphere generator"

    override def setup(parser: Subparser) = {
      // configure common command line arguments
      super.setup(parser)
      // configure algorithm specific command line arguments
      parser.addArgument(s"-${RandomSphere.KEY_SCALE}")
        .`type`[Integer](classOf[Integer])
        .dest(RandomSphere.KEY_SCALE)
        .metavar("<scale>")
        .help("distance between cluster centroids")

      parser.addArgument(s"-${RandomSphere.KEY_CARDINALITY}")
        .`type`[Integer](classOf[Integer])
        .dest(RandomSphere.KEY_CARDINALITY)
        .metavar("<cardinality>")
        .help("number of points per centroid")

      val defaults = scala.collection.mutable.Map[String, AnyRef]()
      defaults.put(Algorithm.KEY_DIMENSIONS, new Integer(3))
      defaults.put(RandomSphere.KEY_SCALE, new Integer(100))
      defaults.put(RandomSphere.KEY_CARDINALITY, new Integer(50))
      parser.setDefaults(defaults.asJava)
    }
  }
}

class RandomSphere(args: Map[String, Object]) extends Algorithm(args) {
  import scala.collection.immutable

  // algorithm specific parameters
  val scale = arguments.get(RandomSphere.KEY_SCALE).get.asInstanceOf[Int]
  val cardinality = arguments.get(RandomSphere.KEY_CARDINALITY).get.asInstanceOf[Int]

  val random: Random = new Random(RandomSphere.SEED)

  val clusters = {
    val builder = immutable.HashMap.newBuilder[List[Double], List[List[Double]]]

    for (dim <- 0 until Math.pow(2.0, dimensions).toInt) {

      val center = (for (i <- 0 until dimensions) yield if (((dim >> i) & 1) == 1) 1.0 * scale else 0.0).toList

      val buffer = mutable.ListBuffer[List[Double]]()
      for (i <- 0 until cardinality) {
        var reject = false
        do {
          val coords = for (i <- 0 until dimensions) yield random.nextGaussian()
          val r = coords.map(x => x * x).reduce(_ + _)
          if (r == 0)
            reject = true
          else {
            reject = false
            val radius = Math.sqrt(r)
            val p = (for (i <- 0 until dimensions) yield (coords(i) / radius) + center(i)).toList

            buffer += p
          }
        } while (reject)
      }

      builder += Tuple2(center, buffer.result())
    }
    builder.result()
  }

  def run() {
    RandomSphere.writeToDisk(outputPath, clusters)
  }
}