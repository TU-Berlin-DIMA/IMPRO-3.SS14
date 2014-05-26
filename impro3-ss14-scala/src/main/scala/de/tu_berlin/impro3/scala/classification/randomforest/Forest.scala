package de.tu_berlin.impro3.scala.classification.randomforest

import scala.collection.mutable.ListBuffer
import _root_.de.tu_berlin.impro3.scala.core.Vector
import scala.util.Random

class Forest(val treeClassifiers: List[SimpleTreeClassifier]) {
}

class RandomForestTrainer(noTrees: Int, noAttributesPerSplit: Int, maxDepth: Int, bootstrapRatio: Double)
  extends Trainer {

  def sample(data: List[Vector] , labels: List[String]) = {
    val labeledData = data.zip(labels)
    val bootstrapSize = math.round( this.bootstrapRatio * labeledData.length ).toInt
    var bootstrap = Set[(Vector, String)]()

    do {
      bootstrap += labeledData(Random.nextInt(labeledData.length))
    } while(bootstrap.size < bootstrapSize)

    val oob = labeledData.toSet &~ bootstrap

    (bootstrap.toList.unzip, oob.toList.unzip)
  }

  def calculateOOBEstimate(tree: SimpleTreeClassifier, oobData: List[Vector], oobLabels: List[String]): Double = {
    val oob = oobData.zip(oobLabels)

    val oobFalse = oob.filter(sample => {
      tree.classify(sample._1)._1 != sample._2
    })

    oobFalse.length.toDouble / oob.length.toDouble
  }

  def train(vectors: List[Vector], labels: List[String]): SimpleForestClassifier = {
    var treeClassifiers = new ListBuffer[SimpleTreeClassifier]
    var oobErrorEstimate = 0.0

    for (i <- 1 to noTrees){
      val (sample, oob) = this.sample(vectors, labels)
      val trainer = new RandomTreeTrainer(noAttributesPerSplit, maxDepth)
      val tree = trainer.train(sample._1, sample._2)
      treeClassifiers += tree

      // apply the oob-samples to the grown tree and accumulate error
      oobErrorEstimate += ( calculateOOBEstimate(tree, oob._1, oob._2) / noTrees )
    }

    new SimpleForestClassifier(new Forest(treeClassifiers.toList), oobErrorEstimate)
  }
}

class SimpleForestClassifier(val forest: Forest, val oobEstimate: Double) extends Classifier {

  def votes(input: Vector): Map[String, Double] = {
    val votes = forest.treeClassifiers.map(_.classify(input)._1)
    val normfac = votes.length.toDouble

    votes.groupBy( cls => cls ).map( classVote => (classVote._1, classVote._2.length.toDouble / normfac) )
  }

  def classify(input: Vector): (String, Double) = {
    this.votes(input).maxBy(_._2)
  }

}

class SimpleTester(val classifier: Classifier) extends Tester {
  def test(testing: List[Vector]): Array[Array[String]] = {
    var confusionMatrix = Array.ofDim[String](2, 2)

    confusionMatrix
  }
}
