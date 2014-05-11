package de.tu_berlin.impro3.scala.util

import scala.util.Random
import java.nio.file.{Paths, Files, Path}
import java.nio.charset.Charset
import java.io.{IOException, BufferedWriter}
import de.tu_berlin.impro3.scala.Algorithm
import net.sourceforge.argparse4j.inf.Subparser

object RandomSphere {

  val SEED = 23454638945312L
  val Delimiter = ","

  // argument names
  val KEY_SCALE = "S"
  val KEY_CARDINALITY = "C"

  def apply(dimensions: Int, scale: Int, cardinality: Int, outputFile: String) = {
    val builder = Map.newBuilder[String, Object]
    builder += Tuple2(Algorithm.KEY_DIMENSIONS, dimensions.asInstanceOf[Object])
    builder += Tuple2(Algorithm.KEY_OUTPUT, outputFile)
    builder += Tuple2(RandomSphere.KEY_SCALE, scale.asInstanceOf[Object])
    builder += Tuple2(RandomSphere.KEY_CARDINALITY, cardinality.asInstanceOf[Object])
    new RandomSphere(builder.result())
  }

  class Config extends Algorithm.Config[RandomSphere] {

    // algorithm names
    override val CommandName = "sphere-gen"
    override val Name = "Random Sphere Generator"

    override def setup(parser: Subparser) = {
      // add arguments
      parser.addArgument(Algorithm.KEY_OUTPUT)
        .`type`[String](classOf[String])
        .dest(Algorithm.KEY_OUTPUT)
        .metavar("OUTPUT")
        .help("output file ")

      // add options (prefixed with --)
      parser.addArgument(s"--${Algorithm.KEY_DIMENSIONS}")
        .`type`[Integer](classOf[Integer])
        .dest(Algorithm.KEY_DIMENSIONS)
        .metavar("N")
        .help("input dimensions (default 3)")
      parser.addArgument(s"-${RandomSphere.KEY_SCALE}")
        .`type`[Integer](classOf[Integer])
        .dest(RandomSphere.KEY_SCALE)
        .metavar("<scale>")
        .help("length of the hypercube edge (default 100)")
      parser.addArgument(s"-${RandomSphere.KEY_CARDINALITY}")
        .`type`[Integer](classOf[Integer])
        .dest(RandomSphere.KEY_CARDINALITY)
        .metavar("<cardinality>")
        .help("number of generated points (default 1000)")

      // add defaults for options
      parser.setDefault(Algorithm.KEY_DIMENSIONS, new Integer(3))
      parser.setDefault(RandomSphere.KEY_SCALE, new Integer(100))
      parser.setDefault(RandomSphere.KEY_CARDINALITY, new Integer(1000))
    }
  }

}

class RandomSphere(args: Map[String, Object]) extends Algorithm(args.updated(Algorithm.KEY_INPUT, args.get(Algorithm.KEY_OUTPUT).get.asInstanceOf[String])) with Iterator[(List[Int], List[Double])] {

  // algorithm specific parameters
  val scale = arguments.get(RandomSphere.KEY_SCALE).get.asInstanceOf[Int]
  val cardinality = arguments.get(RandomSphere.KEY_CARDINALITY).get.asInstanceOf[Int]

  val random: Random = new Random(RandomSphere.SEED)

  val spread: Double = Math.sqrt(scale)

  var current: Int = 0

  var currCenter: Array[Int] = Array.fill(dimensions)(0)

  var currPoint: Array[Double] = Array.fill(dimensions)(0)

  def hasNext: Boolean = current < cardinality

  def next() = {
    for (i <- 0 until dimensions) {
      currCenter.update(i, if (random.nextBoolean()) 0 else scale)
    }

    var reject = false
    do {
      val coords = for (i <- 0 until dimensions) yield random.nextGaussian()
      val r = coords.map(x => x * x).reduce(_ + _)
      if (r == 0)
        reject = true
      else {
        reject = false
        val radius = Math.sqrt(r)
        for (i <- 0 until dimensions) currPoint.update(i, (coords(i) / radius) * spread + currCenter(i))
      }
    } while (reject)

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
      }
      writer.flush()
    } catch {
      case ex: IOException => println(ex)
    } finally {
      writer.close()
    }
  }
}