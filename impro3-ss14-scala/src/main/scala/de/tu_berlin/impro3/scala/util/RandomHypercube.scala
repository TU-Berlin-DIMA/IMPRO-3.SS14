package de.tu_berlin.impro3.scala.util

import _root_.scala.collection.JavaConverters._
import scala.util.Random
import java.nio.file.{Paths, Files, Path}
import java.nio.charset.Charset
import java.io.{IOException, BufferedWriter}
import de.tu_berlin.impro3.scala.Algorithm
import net.sourceforge.argparse4j.inf.Subparser

object RandomHypercube {

  val SEED = 23454638945312L

  val Delimiter = ","

  private def serializeCenter(point: List[Int], pad: Int) = {
    val builder = new StringBuilder
    for (p <- point) {
      builder.append(p.toString.padTo(pad, ' ') + " , ")
    }
    builder.result()
  }

  private def serializePoint(point: List[Double], pad: Int) = {
    val builder = new StringBuilder
    for (p <- point) {
      builder.append(p.toString.padTo(pad, ' ') + " , ")
    }
    builder.result()
  }

  // argument names
  val KEY_SCALE = "S"

  val KEY_CARDINALITY = "C"

  class Config extends Algorithm.Config[RandomHypercube] {

    // algorithm names
    override val CommandName = "cube-gen"
    override val Name = "Random cube generator"

    override def setup(parser: Subparser) = {
      // configure common command line arguments
      super.setup(parser)
      // configure algorithm specific command line arguments
      parser.addArgument(s"-${RandomHypercube.KEY_SCALE}")
        .`type`[Integer](classOf[Integer])
        .dest(RandomHypercube.KEY_SCALE)
        .metavar("<scale>")
        .help("distance between cluster centroids")

      parser.addArgument(s"-${RandomHypercube.KEY_CARDINALITY}")
        .`type`[Integer](classOf[Integer])
        .dest(RandomHypercube.KEY_CARDINALITY)
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

class RandomHypercube(args: Map[String, Object]) extends Algorithm(args) with Iterator[(List[Int], List[Double])] {

  // algorithm specific parameters
  val scale = arguments.get(RandomHypercube.KEY_SCALE).get.asInstanceOf[Int]
  val cardinality = arguments.get(RandomHypercube.KEY_CARDINALITY).get.asInstanceOf[Int]

  val random: Random = new Random(RandomHypercube.SEED)

  val spread: Double = Math.sqrt(scale)

  var current: Int = 0

  var currCenter: Array[Int] = Array.fill(dimensions)(0)

  var currPoint: Array[Double] = Array.fill(dimensions)(0)

  def hasNext: Boolean = current < cardinality

  def next() = {
    for (i <- 0 until dimensions) {
      currCenter.update(i, if (random.nextBoolean()) 0 else scale)
      currPoint.update(i, currCenter(i) + (2 * random.nextDouble() - 1) * spread)
    }

    current += 1

    (currCenter.toList, currPoint.toList)
  }

  def run() {
    val file: Path = Paths.get(outputPath)
    val charSet: Charset = Charset.forName("UTF-8")
    val writer: BufferedWriter = Files.newBufferedWriter(file, charSet)

    try {
      val builder = new StringBuilder
      for ((center: List[Int], point: List[Double]) <- this) {
        point.addString(builder, "", ", ", ", ")
        center.addString(builder, "", ", ", ", ")
        center.addString(builder, "|")
        writer.write(builder.result())
        writer.newLine()
        builder.clear()
//        writer.write(RandomHypercube.serializePoint(point, Math.log10(scale).ceil.toInt + 20))
//        writer.write(RandomHypercube.serializeCenter(center, Math.log10(scale).ceil.toInt + 2))
//        writer.write('\n')
      }
      writer.flush()
    } catch {
      case ex: IOException => println(ex)
    } finally {
      writer.close()
    }
  }
}