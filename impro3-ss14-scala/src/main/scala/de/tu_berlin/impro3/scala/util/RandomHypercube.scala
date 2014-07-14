package de.tu_berlin.impro3.scala.util

import scala.util.Random
import java.nio.file.{Paths, Files, Path}
import java.nio.charset.Charset
import java.io.{IOException, BufferedWriter}
import de.tu_berlin.impro3.scala.ScalaAlgorithm
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import _root_.de.tu_berlin.impro3.core.Algorithm


object RandomHypercube {

  val SEED = 23454638945312L
  val Delimiter = ","

  // argument names
  val KEY_SCALE = "S"
  val KEY_CARDINALITY = "C"

  def apply(dimensions: Int, scale: Int, cardinality: Int, outputFile: String) = {
    val args = new java.util.HashMap[String, AnyRef]
    args.put(ScalaAlgorithm.Command.KEY_DIMENSIONS, dimensions.asInstanceOf[Object])
    args.put(Algorithm.Command.KEY_OUTPUT, outputFile)
    args.put(RandomHypercube.KEY_SCALE, scale.asInstanceOf[Object])
    args.put(RandomHypercube.KEY_CARDINALITY, cardinality.asInstanceOf[Object])
    new RandomSphere(new Namespace(args))
  }

  class Command extends ScalaAlgorithm.Command[RandomHypercube]("cube-gen", "Random Cube Generator", classOf[RandomHypercube]) {

    override def setup(parser: Subparser) = {
      // add arguments
      parser.addArgument(Algorithm.Command.KEY_OUTPUT)
        .`type`[String](classOf[String])
        .dest(Algorithm.Command.KEY_OUTPUT)
        .metavar("OUTPUT")
        .help("output file ")

      // add options (prefixed with --)
      parser.addArgument(s"--${ScalaAlgorithm.Command.KEY_DIMENSIONS}")
        .`type`[Integer](classOf[Integer])
        .dest(ScalaAlgorithm.Command.KEY_DIMENSIONS)
        .metavar("N")
        .help("input dimensions (default 3)")
      parser.addArgument(s"-${RandomHypercube.KEY_SCALE}")
        .`type`[Integer](classOf[Integer])
        .dest(RandomSphere.KEY_SCALE)
        .metavar("<scale>")
        .help("length of the hypercube edge (default 100)")
      parser.addArgument(s"-${RandomHypercube.KEY_CARDINALITY}")
        .`type`[Integer](classOf[Integer])
        .dest(RandomHypercube.KEY_CARDINALITY)
        .metavar("<cardinality>")
        .help("number of generated points (default 1000)")

      // add defaults for options
      parser.setDefault(ScalaAlgorithm.Command.KEY_DIMENSIONS, new Integer(3))
      parser.setDefault(RandomHypercube.KEY_SCALE, new Integer(100))
      parser.setDefault(RandomHypercube.KEY_CARDINALITY, new Integer(1000))
    }
  }

}

class RandomHypercube(ns: Namespace) extends ScalaAlgorithm(ns) with Iterator[(List[Int], List[Double])] {

  // algorithm specific parameters
  val scale = ns.get[Int](RandomHypercube.KEY_SCALE)
  val cardinality = ns.get[Int](RandomHypercube.KEY_CARDINALITY)

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
      }
      writer.flush()
    } catch {
      case ex: IOException => println(ex)
    } finally {
      writer.close()
    }
  }
}