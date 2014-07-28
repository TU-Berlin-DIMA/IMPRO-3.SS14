package de.tu_berlin.impro3.spark.clustering.hac

import de.tu_berlin.impro3.core.Algorithm
import net.sourceforge.argparse4j.inf.Namespace
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import de.tu_berlin.impro3.core.Algorithm
import net.sourceforge.argparse4j.inf.Subparser
import net.sourceforge.argparse4j.inf.Namespace

object HAC {
  val KEY_ITERATIONS = "algorithm.hac.iterations"

  val KEY_LINKAGE = "algorithm.hac.linkage"

  object LinkageMode extends Enumeration {
	type LinkageMode = Value
	val SingleLinkage, CompleteLinkage = Value
  }

  class Command extends Algorithm.Command[HAC]("hac", "Hierarchical clustering", classOf[HAC]) {

    override def setup(parser: Subparser) = {
      parser.addArgument("iterations")
                .`type`[Integer](classOf[Integer])
                .dest(KEY_ITERATIONS)
                .metavar("N")
                .help("Number of iterations (#documents - iterations = #cluster)")

      parser.addArgument("linkage")
                .`type`[String](classOf[String])
                .dest(KEY_LINKAGE)
                .metavar("L")
                .help("Linkage mode (SINGLE or COMPLETE)");

      super.setup(parser)
    }
  }
}
import HAC.LinkageMode._

/**
 * Spark HAC scala implementation.
 */
class HAC(input: String, output: String, iterations: Int, linkage: LinkageMode) extends Algorithm {
  def this(ns: Namespace) = this(
    ns.get[String](Algorithm.Command.KEY_INPUT),
    ns.get[String](Algorithm.Command.KEY_OUTPUT),
    ns.get[Int](HAC.KEY_ITERATIONS),
    if (ns.get[String](HAC.KEY_LINKAGE) == "COMPLETE") CompleteLinkage else SingleLinkage
  )

  override def run(): Unit = {
    val conf = new SparkConf().setAppName("HAC")
    val sc = new SparkContext(conf)

    runProgramm(sc, input, output, iterations, linkage)
  }

  def runProgramm(sc: SparkContext, input: String, output: String, cluster: Int, linkage: LinkageMode) {
    println("Running with " + input + ", " + output + " and " + cluster + " and " + linkage)

    val similarityLinkageMethod = linkage match {
      case SingleLinkage => math.max(_: Long, _: Long) // minimal distance = maximal similarity
      case CompleteLinkage => math.min(_: Long, _: Long) // maximal distance = minimal similarity
    }

    val clusterToMergeSelector = linkage match {
      case SingleLinkage => ((a: ((Int, Int), Long), b: ((Int, Int), Long)) => if (a._2 > b._2) a else b) // max
      case CompleteLinkage => ((a: ((Int, Int), Long), b: ((Int, Int), Long)) => if (a._2 < b._2) a else b) // min
    }

    // docID, termID, term count
    val docTermCounts = sc.textFile(input).map(line => {
      val values = line.split(" ")
      (values(0).toInt, values(1).toInt, values(2).toLong)
    })

    // initialize documents with cluster id = document id as (clusterID, docID) tuples
    var documents = docTermCounts.map(_._1).distinct.map(docID => (docID, docID))

    // calculate similarity matrix with ((firstDocID, secondDocID), similarity) tuples
    val termCount = docTermCounts.map(dtc => (dtc._2, (dtc._1, dtc._3))).groupByKey()
    var similarities = termCount.flatMap {
      case (termID, counts) =>
        counts.flatMap {
          case (leftDocID, leftTermCount) =>
            counts.flatMap {
              case (rightDocID, rightTermCount) =>
                if (leftDocID < rightDocID)
                  Some(((leftDocID, rightDocID), leftTermCount * rightTermCount))
                else
                  None
            }
        }
    }.reduceByKey(_ + _)

    // val iterations = (documents.count - cluster).toInt
    println("Running " + iterations + " iterations.")

    for (i <- 1 to Math.min(iterations, documents.count.toInt - 1)) {
      // println("Running " + i + ". iteration...");

      // find cluster to merge
      val clusterToMerge = similarities.reduce(clusterToMergeSelector)
      val (removedClusterID, mergedClusterID) = clusterToMerge._1

      // debug
      // println("Merging cluster pair: " + clusterToMerge)
      // Files.write(Paths.get(outputFilePrefix + ".iteration-" + i +".mergedCluster"), clusterToMerge.toString().getBytes())
      // documents.saveAsTextFile(outputFilePrefix + ".iteration-" + i +".documents")
      // similarities.saveAsTextFile(outputFilePrefix + ".iteration-" + i +".similarities")

      // update document clusters ids
      documents = documents.map {
        case (clusterID, docID) =>
          if (clusterID == removedClusterID)
            (mergedClusterID, docID)
          else
            (clusterID, docID)
      }

      // update similarity matrix
      similarities = similarities.flatMap {
        case ((firstClusterID, secondClusterID), similarity) =>
          if (firstClusterID == removedClusterID && secondClusterID == mergedClusterID) {
            None
          } else if (firstClusterID == removedClusterID) {
            if (mergedClusterID < secondClusterID)
              Some((mergedClusterID, secondClusterID), similarity)
            else
              Some((secondClusterID, mergedClusterID), similarity)
          } else if (secondClusterID == removedClusterID) {
            if (firstClusterID < mergedClusterID)
              Some((firstClusterID, mergedClusterID), similarity)
            else
              Some((mergedClusterID, firstClusterID), similarity)
          } else
            Some((firstClusterID, secondClusterID), similarity)
      }.reduceByKey(similarityLinkageMethod)
    }

    documents.saveAsTextFile(output)
  }
}