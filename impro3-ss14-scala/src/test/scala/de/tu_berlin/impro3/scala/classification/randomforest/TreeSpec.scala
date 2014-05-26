package de.tu_berlin.impro3.scala.classification.randomforest

import org.scalatest._
import _root_.de.tu_berlin.impro3.scala.core.Vector

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TreeSpec extends FlatSpec with Matchers {

  /*
    example from Han, J. and Kamber, M. (2006). "Data Mining. Concepts
    and Techniques", Morgan Kaufmann
    and http://www.informatik.hu-berlin.de/forschung/gebiete/wbi/teaching/archive/ws1112/
               vl_datawarehousing/16_classification.pdf
  */
  def createBuyComputerData(): (List[Vector], List[String]) = {
    val vectors = List(
      new Vector(List(0.0, 2.0, 0.0, 0.0))
      , new Vector(List(0.0, 2.0, 0.0, 1.0))
      , new Vector(List(1.0, 2.0, 0.0, 0.0))
      , new Vector(List(2.0, 1.0, 0.0, 0.0))
      , new Vector(List(2.0, 0.0, 1.0, 0.0))
      , new Vector(List(2.0, 0.0, 1.0, 1.0))
      , new Vector(List(1.0, 0.0, 1.0, 1.0))
      , new Vector(List(0.0, 1.0, 0.0, 0.0))
      , new Vector(List(0.0, 0.0, 1.0, 0.0))
      , new Vector(List(2.0, 1.0, 1.0, 0.0))
      , new Vector(List(0.0, 1.0, 1.0, 1.0))
      , new Vector(List(1.0, 1.0, 0.0, 1.0))
      , new Vector(List(1.0, 2.0, 1.0, 0.0))
      , new Vector(List(2.0, 1.0, 0.0, 1.0))
    )
    val labels = List(
      "no"
      , "no"
      , "yes"
      , "yes"
      , "yes"
      , "no"
      , "yes"
      , "no"
      , "yes"
      , "yes"
      , "yes"
      , "yes"
      , "yes"
      , "no"
    )

    (vectors, labels)
  }

  val emptyTree = new Tree(0.0, 0, null, null, Map[String, Double]().empty)
  val emptyVector = Vector(List())

  "A tree" should "always be happy!" in {
    true should be (true)
  }

  "A tree" should " be a leaf if there is no left child and no right child" in {
    val splittingValue: Double = 0.0
    val splittingDimension: Int = 0
    val left: Tree = null
    val right: Tree = null
    val leafLabels = Map[String, Double]().empty

    val tree: Tree = new Tree(splittingValue, splittingDimension, left, right, leafLabels)
    tree.isLeaf should be (true)
  }

  "A tree" should "not be a leaf if there is a left child and but no right child" in {
    val splittingValue: Double = 0.0
    val splittingDimension: Int = 0
    val left: Tree = emptyTree
    val right: Tree = null
    val leafLabels = Map[String, Double]().empty

    val tree: Tree = new Tree(splittingValue, splittingDimension, left, right, leafLabels)
    tree.isLeaf should be (false)
  }

  "A tree" should " not be a leaf if there is no left child and but a right child" in {
    val splittingValue: Double = 0.0
    val splittingDimension: Int = 0
    val left: Tree = emptyTree
    val right: Tree = null
    val leafLabels = Map[String, Double]().empty

    val tree: Tree = new Tree(splittingValue, splittingDimension, left, right, leafLabels)
    tree.isLeaf should be (false)
  }

  "A tree" should " return itself on findLeaf if it is a leaf" in {
    val splittingValue: Double = 0.5
    val splittingDimension: Int = 0
    val left: Tree = null
    val right: Tree = null
    val leafLabels = Map[String, Double]().empty

    val vector: Vector = Vector(0.4,0.6)

    val tree: Tree = new Tree(splittingValue, splittingDimension, left, right, leafLabels)
    tree.findLeaf(vector) should be theSameInstanceAs tree
  }

  "A tree" should " return its the correct child on findLeaf" in {
    val leafLabels = Map[String, Double]().empty

    val leftChild = new Tree(0.0, 0, null, null, leafLabels)
    val rightChild = new Tree(0.0, 0, null, null, leafLabels)

    val treeA: Tree = new Tree(0.5, 0, leftChild, rightChild, leafLabels)
    val treeB: Tree = new Tree(0.2, 1, leftChild, rightChild, leafLabels)

    val vectorA = Vector(0.4, 0.3)
    val vectorB = Vector(0.5, 0.2)
    val vectorC = Vector(0.6, 0.1)

    // (0.4,0.3) from vector is smaller than (0.5,0.2) from splittingVector in dimension 0
    treeA.findLeaf( vectorA ) should be theSameInstanceAs leftChild

    // (0.5,0.2) from vector is equals (0.5,0.2) from splittingVector in dimension 0
    treeA.findLeaf( vectorB ) should be theSameInstanceAs leftChild

    // (0.6,0.1) from vector is bigger than (0.5,0.2) from splittingVector in dimension 0
    treeA.findLeaf( vectorC ) should be theSameInstanceAs rightChild

    // Attention: reversed direction now!

    // (0.4,0.3) from vector is bigger than (0.5,0.2) from splittingVector in dimension 1
    treeB.findLeaf( vectorA ) should be theSameInstanceAs rightChild

    // (0.5,0.2) from vector is equals (0.5,0.2) from splittingVector in dimension 1
    treeB.findLeaf( vectorB ) should be theSameInstanceAs leftChild

    // (0.6,0.1) from vector is smaller than (0.5,0.2) from splittingVector in dimension 1
    treeB.findLeaf( vectorC ) should be theSameInstanceAs leftChild
  }

  "A tree" should " return it's leafLabels if it's a leaf" in {
    val splittingValue: Double = 0.0
    val splittingDimension: Int = 0
    val left: Tree = emptyTree
    val right: Tree = null
    val leafLabels: Map[String, Double] = Map("A" -> 5, "B" -> 7, "C" -> 3)

    val tree: Tree = new Tree(splittingValue, splittingDimension, left, right, leafLabels)
    tree.labels should equal (leafLabels)
  }

  "A tree" should " return it's correct labels" in {
    val splittingValue: Double = 0.0
    val splittingDimension: Int = 0
    val left: Tree = emptyTree
    val right: Tree = null
    val leafLabels: Map[String, Double] = Map("A" -> 1, "B" -> 2, "C" -> 3)
    val leafLabelsLeft: Map[String, Double] = Map("A" -> 2, "B" -> 3, "C" -> 4)
    val leafLabelsRight: Map[String, Double] = Map("A" -> 3, "B" -> 4, "C" -> 5)
    val leafLabelsAll: Map[String, Double] = Map("A" -> 6, "B" -> 9, "C" -> 12)

    val treeLeft: Tree = new Tree(splittingValue, splittingDimension, left, right, leafLabelsLeft)
    val treeRight: Tree = new Tree(splittingValue, splittingDimension, left, right, leafLabelsRight)
    val tree: Tree = new Tree(splittingValue, splittingDimension, treeLeft, treeRight, leafLabels)
    tree.labels should equal (leafLabelsAll)
  }

  "A simple tree classifier " should " return the correct class" in {
    val leafLabels = Map[String, Double]().empty

    val leftChild = new Tree(0.0, 0, null, null, Map("A" -> 1, "B" -> 0))
    val rightChild = new Tree(0.0, 0, null, null, Map("A" -> 0, "B" -> 1))

    val tree: Tree = new Tree(0.5, 0, leftChild, rightChild, leafLabels)
    val vector = Vector(0.4, 0.3)

    val classifier = new SimpleTreeClassifier(tree)

    // (0.4,0.3) from vector is smaller than (0.5,0.2) from splittingVector in dimension 0
    classifier.classify( Vector(0.4, 0.3) ) should equal (("A"->1.0))

    // (0.4,0.3) from vector is smaller than (0.5,0.2) from splittingVector in dimension 0
    classifier.classify( Vector(0.6, 0.3) ) should equal (("B"->1.0))
  }

  "A random tree trainer " should " calculate the correct entropy" in {
    val vectors = List(
        Vector(0.5, 0.1),
        Vector(0.5, 0.2),
        Vector(0.5, 0.3),
        Vector(0.5, 0.4),
        Vector(0.5, 0.5),
        Vector(0.5, 0.6),
        Vector(0.5, 0.7),
        Vector(0.5, 1.0))
    val labels1 = List("A", "A", "A", "A", "B", "B", "B", "B")
    val labels2 = List("A", "A", "A", "A", "A", "A", "A", "B")

    val trainer = new RandomTreeTrainer(2, 5)
    trainer.calcEntropy(vectors.zip(labels1)) should equal (1.0)
    trainer.calcEntropy(vectors.zip(labels2)) should (
      be > (0.53) and
      be < (0.55)
    )
  }

  "The Information Gain" should "be correctly calculated" in {
    val (vectors, labels) = createBuyComputerData()
    val (left, right) = vectors.zip(labels).partition(el => el._1.elementAt(2) == 0)

    val trainer = new RandomTreeTrainer(2, 5)
    val E = trainer.calcEntropy(vectors.zip(labels))
    E should (
      be > (0.93) and
        be < (0.95)
      )

    val studentWgain = trainer.calcGainRatioOfPartition(E, left, right)

    studentWgain should (
      be > (0.14) and
        be < (0.16)
      )
  }

  "A random tree trainer" should "train a node correctly" in {
    val vectors = List(
        Vector(0.5, 0.1),
        Vector(0.5, 0.2),
        Vector(0.5, 0.3),
        Vector(0.5, 0.4),
        Vector(0.5, 0.5),
        Vector(0.5, 0.6),
        Vector(0.5, 0.7),
        Vector(0.5, 1.0))
    val labels1 = List("A", "A", "A", "A", "B", "B", "B", "B")
    val trainer = new RandomTreeTrainer(2, 2)
    val t = trainer.train(vectors, labels1)
    SimpleTreePrinter.printTree(t.tree)
    t.tree.splittingValue should be (0.4)
    t.tree.splittingDimension should be (1)
  }

  "A random tree trainer" should "train a tree" in {
    val (vectors, labels) = createBuyComputerData;
    val trainer = new RandomTreeTrainer(4, 3)
    val t = trainer.train(vectors, labels)
    SimpleTreePrinter.printTree(t.tree)
  }
}

object SimpleTreePrinter {
  def printTree(tree: Tree) {
    println
    printTreeInternal(tree, 0)
  }

  def printTreeInternal(tree: Tree, depth: Int) {
    printWhitespace(depth)
    println("(depth: " + depth + " v: " + tree.splittingValue + " d: " + tree.splittingDimension + " dist: " + tree.labels + ")")
    if (tree.left != null) {
      printWhitespace(depth)
      printTreeInternal(tree.left, depth+1)
    }
    if (tree.right != null) {
      printWhitespace(depth)
      printTreeInternal(tree.right, depth+1)
    }
  }

  def printWhitespace(length: Int) {
      for (i <- 0 to length) {
        print("  ")
      }    
  }
}
