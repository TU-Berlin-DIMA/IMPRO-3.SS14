package de.tu_berlin.impro3.scala.clustering.canopy

import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import _root_.de.tu_berlin.impro3.scala.ScalaAlgorithm
import de.tu_berlin.impro3.scala.clustering.kmeans.KVector
import scala.collection.mutable
import scala.collection.immutable.Stack
import java.nio.file.{Files, Paths, Path}
import java.nio.charset.Charset
import java.io.{IOException, BufferedWriter}

object Canopy {

  // argument names
  val KEY_T1 = "T1"
  val KEY_T2 = "T2"

  class Command extends ScalaAlgorithm.Command[Canopy]("canopy", "Canopy Clustering", classOf[Canopy]) {

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)
      // add options (prefixed with --)
      parser.addArgument(s"-${Canopy.KEY_T1}")
        .`type`[Integer](classOf[Integer])
        .dest(Canopy.KEY_T1)
        .metavar("T1")
        .help("T1 threshold")

      parser.addArgument(s"-${Canopy.KEY_T2}")
        .`type`[Integer](classOf[Integer])
        .dest(Canopy.KEY_T2)
        .metavar("T2")
        .help("T2 threshold")

      // add defaults for options
      parser.setDefault(Canopy.KEY_T1, new Integer(7))
      parser.setDefault(Canopy.KEY_T2, new Integer(3))
    }
  }

}

class Canopy(ns: Namespace) extends ScalaAlgorithm(ns) {
  // algorithm specific parameters
  val T1 = ns.get[Int](Canopy.KEY_T1)
  val T2 = ns.get[Int](Canopy.KEY_T2)
  // Check on parameter constraints
  if (T1 < T2) {
    throw new IllegalArgumentException("T1=" + T1 + " needs to be grater than T2=" + T2)
  }

  var points: Stack[KVector] = {
    val p = Stack.newBuilder[KVector]
    for (i <- iterator()) {
      p += new KVector(i._1)
    }
    p.result()
  }

  val canopies = mutable.Map[KVector, mutable.Set[KVector]]()

  def run(): Unit = {
    while (points.size > 0){
      val center = points.head
      points = points.filter(x => x != center) // remove center from list
      val pointsSet = mutable.Set[KVector]()

      for(point <- points){
        val dist = point.euclideanDistance(center)

        if(dist <= T1){
          pointsSet.add(point)

          if(dist <= T2) {
            points = points.filter(x => x != point)
          }
        }
      }
      canopies.put(center, pointsSet)
    }
    writeToFile()
  }

  // write output to csv file
  def writeToFile() {
    val file: Path = Paths.get(outputPath)
    val charSet: Charset = Charset.forName("UTF-8")
    val writer: BufferedWriter = Files.newBufferedWriter(file, charSet)

    try {
      val builder = new StringBuilder
      for (canopy <- canopies) {
        canopy._1.addString(builder, "", ", ", "")     // centroid: a,b
        for (point <- canopy._2) {
          point.addString(builder, ", ", ", ", "")     // canopy points: x_1,y_1,x_2,y_2 ...
        }
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
