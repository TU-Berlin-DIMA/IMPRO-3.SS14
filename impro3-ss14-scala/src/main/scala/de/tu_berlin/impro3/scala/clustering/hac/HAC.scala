package de.tu_berlin.impro3.scala.clustering.hac

import _root_.net.sourceforge.argparse4j.inf.Subparser
import _root_.de.tu_berlin.impro3.scala.Algorithm
import java.nio.file.{Paths, Files, Path}
import java.nio.charset.Charset
import java.io.{IOException, BufferedWriter}

object HAC {

  class Config extends Algorithm.Config[HAC] {

    // algorithm names
    override val CommandName = "hac"
    override val Name = "Hierarchical Agglomerative Clustering"

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)
      // add options (prefixed with --)
      // add defaults for options
      parser.setDefault("dimensions", new Integer(2))
    }
  }

}

class HAC(args: Map[String, Object]) extends Algorithm(args) {

  val clusters: List[Cluster] = {
    val p = List.newBuilder[Cluster]
    for (i <- 1 to 10) p += new Cluster(List(new DataPoint(i, i)))
    p.result()
  }
  
  // just tests
//  val testC: Cluster = new Cluster(List(new DataPoint(1,2), new DataPoint(3.5,7)))
//  var testC2: Cluster = new Cluster(List(new DataPoint(1,1)))
//  println("testavg: x: " + testC.getAvg().x + " , y: " + testC.getAvg().y)
//  testC2 = testC.addCluster(testC2)
//  println("testavg: x: " + testC2.getAvg().x + " , y: " + testC2.getAvg().y)
//  println("distance 1,2: " + testC.distanceTo(testC2))
//  println("distance 1,2: " + testC2.distanceTo(testC))
  
  def run(): Unit = {
    println("Starting HAC Algorithm")
    
    while (clusters.size > 1) {
      println("prob endless")
    }
//    for (i <- iterator()) println(i); //TODO: delete
 
  }
  
  def compareThemAllFindSmallestDist(clusters1: List[Cluster], clusters2: List[Cluster]): (Cluster, Cluster) = {
    var smallestDist = (clusters1(0), clusters2(0), clusters1(0).distanceTo(clusters2(0)))
    
    for(c1 <- clusters1) {
      for(c2 <- clusters2) {
        if(c1.distanceTo(c2) < smallestDist._3)
          smallestDist = (c1, c2, c1.distanceTo(c2))
        }
    }
    
    (smallestDist._1, smallestDist._2)
  }
  
  def writeToFile() {
    val file: Path = Paths.get(outputPath)
    val charSet: Charset = Charset.forName("UTF-8")
    val writer: BufferedWriter = Files.newBufferedWriter(file, charSet)

    //TODO
    try {
      val builder = new StringBuilder
      for (cluster <- clusters) {
        builder.append("ne line..");
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
