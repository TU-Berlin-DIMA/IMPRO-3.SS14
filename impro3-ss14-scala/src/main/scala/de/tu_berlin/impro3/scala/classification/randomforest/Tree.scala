package de.tu_berlin.impro3.scala.classification.randomforest

import _root_.de.tu_berlin.impro3.scala.core.Vector
import scala.util.Random

class Tree(
    val splittingValue: Double,
    val splittingDimension: Int,
    val left: Tree,
    val right: Tree,
    val leafLabels: Map[String, Double]) {

  def isLeaf() = { left == null && right == null }

  def findLeaf(vector: Vector): Tree = {
    if (isLeaf()) return this
    var route: Tree = left
    if (vector.elementAt(splittingDimension) > splittingValue) {
      route = right
    }

    if (route == null) return this

    route.findLeaf(vector)
  }

  def labels(): Map[String, Double] = {
    var labelList: List[(String, Double)] = leafLabels.toList
    if(left != null) {
      labelList = labelList ::: left.labels.toList
    }
    if(right != null) {
      labelList = labelList ::: right.labels.toList
    }
    labelList.groupBy(_._1).mapValues(_.map(_._2).reduceLeft(_ + _))
  }
}

class RandomTreeTrainer(noAttributesPerSplit: Int, maxDepth: Int) extends Trainer {
  def train(vectors: List[Vector], labels: List[String]): SimpleTreeClassifier = {
    val labeledVectors = vectors.zip(labels)
    new SimpleTreeClassifier(trainInternal(labeledVectors, 0))
  }

  def trainInternal(labeledVectors: List[(Vector, String)], depth: Int): Tree = {
    var tree: Tree = null

    if (labeledVectors.length > 0) {
      if (depth < maxDepth) {
        val (splitValue, splitDim) = calcSplit(labeledVectors)
        val (left, right) = labeledVectors
          .partition(_._1.elementAt(splitDim)
          <= splitValue)
        val leftChild = trainInternal(left, depth + 1)
        val rightChild = trainInternal(right, depth + 1)
        tree = new Tree(splitValue,
          splitDim,
          leftChild,
          rightChild,
          Map[String, Double]().empty)
      } else {
        val labels = labeledVectors.map(v => (v._2, 1.0))
          .groupBy(_._1)
          .mapValues(_.map(_._2)
          .reduceLeft(_ + _))
        tree = new Tree(0.0, 0, null, null, labels)
      }
    }

    tree
  }

  def calcSplit(labeledVectors: List[(Vector, String)]): (Double, Int) = {
    val columns = labeledVectors(0)._1.elements
    val split = sampleColumns(columns)
                   .map( column => findSplitValue(labeledVectors, column) )
                   .maxBy( _._1 )
    (split._3, split._2)
  }

  def sampleColumns(columns: Int): Set[Int] = {
    var attributes = Set[Int]()

    do {
      attributes += Random.nextInt(columns)
    } while(attributes.size < noAttributesPerSplit && attributes.size < columns)

    attributes
  }

  def findSplitValue(labeledVectors: List[(Vector, String)], splitFeature: Int) : (Double, Int, Double) = {

    val entropyTotal = calcEntropy(labeledVectors)

    var best_splitValue = 0.0
    var best_gain = 0.0

    for (i <- 0 until labeledVectors.length) {

      val split_Value = labeledVectors(i)._1.elementAt(splitFeature)

      val (splitLeft, splitRight) = labeledVectors.partition(x => x._1.elementAt(splitFeature) <= split_Value)

      val gain = calcGainRatioOfPartition(entropyTotal, splitLeft, splitRight)

      if (gain >= best_gain) {
        best_gain = gain
        best_splitValue = split_Value
      }
    }

    (best_gain, splitFeature, best_splitValue)
  }

  def calcEntropy(labeledVectors: List[(Vector, String)]): Double = {
    // get the probabilities of the classes
    val counts = labeledVectors.groupBy(cls => cls._2).map( group => (group._1, group._2.length) )
    val probs = counts.map( count => (count._1, count._2.toDouble / labeledVectors.length.toDouble) )

    // get the entropy
    -1.0 * probs.map( classProb => classProb._2 * (math.log(classProb._2) / math.log(2)) ).sum
  }

  def calcGainRatioOfPartition(entropyTotal: Double, left: List[(Vector, String)], right: List[(Vector, String)])
  : Double = {

    val total = left.length + right.length

    val entropyLeft = calcEntropy(left)
    val entropyRight = calcEntropy(right)

    val pLeft = left.length / total.toDouble
    val pRight = right.length / total.toDouble

    entropyTotal - (pLeft * entropyLeft + pRight * entropyRight)
  }

}

class SimpleTreeClassifier(val tree: Tree) extends Classifier {
  def classify(input: Vector): (String, Double) = {
    val c = tree.findLeaf(input).labels().maxBy(_._2)
    (c._1, c._2.toDouble)
  }
}
