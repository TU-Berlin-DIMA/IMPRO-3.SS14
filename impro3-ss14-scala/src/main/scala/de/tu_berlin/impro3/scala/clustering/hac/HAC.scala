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
  var writer: BufferedWriter = null
  
  var clusters: List[Cluster] = {
    val c = List.newBuilder[Cluster]
    for (i <- iterator()) c += new Cluster(List(new DataPoint(i._1(0), i._2(1))))
    c.result()
  }
   
  // var clusters: List[Cluster] = for(i <- iterator()) yield new Cluster(List(new DataPoint(i._1(0), i._2(1))))
      
  // just tests
//  val testC: Cluster = new Cluster(List(new DataPoint(1,2), new DataPoint(3.5,7)))
//  var testC2: Cluster = new Cluster(List(new DataPoint(1,1)))
//  println("testavg: x: " + testC.getAvg().x + " , y: " + testC.getAvg().y)
//  testC2 = testC.addCluster(testC2)
//  println("testavg: x: " + testC2.getAvg().x + " , y: " + testC2.getAvg().y)
//  println("distance 1,2: " + testC.distanceTo(testC2))
//  println("distance 1,2: " + testC2.distanceTo(testC))
  
  val testCluster1: Cluster = new Cluster(List(new DataPoint(24,44), new DataPoint(26,41)))
  val testCluster2: Cluster = new Cluster(List(new DataPoint(57,14), new DataPoint(56,18)))
  val testCluster3: Cluster = new Cluster(List(new DataPoint(20,47), new DataPoint(21,49)))
  val testCluster4: Cluster = new Cluster(List(new DataPoint(57.3,10), new DataPoint(56.4,21)))
  
  val testClusters: List[Cluster] = List(testCluster1, testCluster2, testCluster3, testCluster4)
  
  println(compareThemAllFindSmallestDist(testClusters))
  
  def run(): Unit = {
    println("Starting HAC Algorithm")
    
    println("before while amount of clusters: " + clusters.size)
    
    startWrite()
    while (clusters.size > 1) {
      var closest = compareThemAllFindSmallestDist(clusters)
      
      clusters = clusters.filter(_!=closest._1)
      clusters = clusters.filter(_!=closest._2)
      
      val newCluster:Cluster = closest._1.addCluster(closest._2)
      clusters = clusters ++ List(newCluster)
      
      println("amount of clusters: " + clusters.size)
      writeClustersToFile()
    }
    stopWrite()
  }
  
  def compareThemAllFindSmallestDist(clusters: List[Cluster]): (Cluster, Cluster) = {
    var smallestDist = (clusters(0), clusters(1), clusters(0).distanceTo(clusters(1)))
    
    for(c1 <- clusters) {
      for(c2 <- clusters) {
        if(!(c1 eq c2) && c1.distanceTo(c2) < smallestDist._3)
          smallestDist = (c1, c2, c1.distanceTo(c2))
        }
    }
    
    (smallestDist._1, smallestDist._2)
  }
  
  def startWrite() {
    val file: Path = Paths.get(outputPath)
    val charSet: Charset = Charset.forName("UTF-8")
    writer = Files.newBufferedWriter(file, charSet)
  }
  
  def stopWrite() {
    writer.close() 
  }
  
  def writeClustersToFile() {
    var i: Int = 0
    writer.write("############ START OF CLUSTER LIST ############")
    writer.newLine()
    for (cluster <- clusters) {
      writer.write("Cluster No. " + i + ": " + cluster)
      writer.newLine()
      writer.flush()
      i+=1
    }
    writer.write("############ END OF CLUSTER LIST ############")
    writer.newLine()
  }
}
