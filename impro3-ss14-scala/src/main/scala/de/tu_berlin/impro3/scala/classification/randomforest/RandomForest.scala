package de.tu_berlin.impro3.scala.classification.randomforest

import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import _root_.de.tu_berlin.impro3.scala.ScalaAlgorithm
import _root_.de.tu_berlin.impro3.scala.core.Vector

import scala.util.Random
import java.nio.file.{Files, Paths, Path}
import java.nio.charset.Charset
import java.io.{IOException, BufferedWriter}

object RandomForest {

  // constnats
  val Seed = 5431423142056L

  // argument names
  val KEY_B = "B"
  val KEY_M = "M"
  val KEY_RATIO = "ratio"
  val KEY_MAX_DEPTH = "max-depth"

  class Command extends ScalaAlgorithm.Command[RandomForest]("random-forest", "Random Forest Classifier", classOf[RandomForest]) {

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)

      // add options (prefixed with -)
      parser.addArgument(s"-${RandomForest.KEY_B}")
        .`type`[Integer](classOf[Integer])
        .dest(RandomForest.KEY_B)
        .metavar("B")
        .help("number of trees")

      parser.addArgument(s"-${RandomForest.KEY_M}")
        .`type`[Integer](classOf[Integer])
        .dest(RandomForest.KEY_M)
        .metavar("M")
        .help("number of attributes used to find the best split per node")

      // add options (prefixed with --)
      parser.addArgument(s"--${RandomForest.KEY_RATIO}")
        .`type`[java.lang.Double](classOf[java.lang.Double])
        .dest(RandomForest.KEY_RATIO)
        .metavar("ratio")
        .help("relative size of bootstrap samples (between 0. and 1.)")

      parser.addArgument(s"--${RandomForest.KEY_MAX_DEPTH}")
        .`type`[Integer](classOf[Integer])
        .dest(RandomForest.KEY_MAX_DEPTH)
        .metavar("max-depth")
        .help("maximal depth of used random trees (defaults to inf)")

      // add defaults for options
      parser.setDefault(RandomForest.KEY_B, new Integer(200))
      parser.setDefault(RandomForest.KEY_M, new Integer(10))
      parser.setDefault(RandomForest.KEY_RATIO, new java.lang.Double(0.66))
      parser.setDefault(RandomForest.KEY_MAX_DEPTH, new Integer(Integer.MAX_VALUE))
    }
  }

  def getLabeledData(iter: Iterator[(List[Double], List[Double], String)]): (List[Vector], List[String]) = {
    val data = (for (line <- iter) yield (new Vector(line._1), line._3)).toList
    data.unzip
  }

}

class RandomForest(ns: Namespace) extends ScalaAlgorithm(ns) {

  // algorithm specific parameters
  val noTrees = ns.get[Int](RandomForest.KEY_B)
  val noAttributesPerSplit = ns.get[Int](RandomForest.KEY_M)
  // sqrt (dimension)
  val bootstrapRatio = ns.get[Int](RandomForest.KEY_RATIO)
  val maxDepth = ns.get[Int](RandomForest.KEY_MAX_DEPTH)

  override def run(): Unit = {

    val (vectors, labels) = RandomForest.getLabeledData(this.iterator())

    val classes = labels.distinct

    println("classes: " + classes)

    val forest = new RandomForestTrainer(noTrees, noAttributesPerSplit, maxDepth, bootstrapRatio)
    val classifier = forest.train(vectors, labels)

    if (vectors(0).elements == 3) {
      var testPoint = new Vector(List[Double](Random.nextDouble() * 100 * Random.nextInt(2), Random.nextDouble() * 100 * Random.nextInt(2), Random.nextDouble() * 100 * Random.nextInt(2)))
      val (a, b) = classifier.classify(testPoint)
      println("the value (" + testPoint.elementAt(0) + " , " + testPoint.elementAt(1) + " , " + testPoint.elementAt(2) + ") has a probability of (" + b + ") that its in class " + a)

    }

    println("error: " + classifier.oobEstimate)
    val file: Path = Paths.get(outputPath)
    val charSet: Charset = Charset.forName("UTF-8")
    val writer: BufferedWriter = Files.newBufferedWriter(file, charSet)

    try {
      writer.write(classifier.oobEstimate.toString)
      writer.flush()
    } catch {
      case ex: IOException => println(ex)
    } finally {
      writer.close()
    }

  }
}
