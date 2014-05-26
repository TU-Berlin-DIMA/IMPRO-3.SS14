package de.tu_berlin.impro3.scala.classification.randomforest

import org.scalatest._
import _root_.de.tu_berlin.impro3.scala.core.Vector

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


object RandomForestSpec {

  /*
   creates data similar to the randomSphere-generated data with known values and labels
   and extracts the labeled data using getLabeledData
  */
  def labeledData(len: Int): (List[Vector], List[String]) = {
    val testIterator = new Iterator[(List[Double], List[Double], String)] {
      var cnt = 0
      override def hasNext = { cnt < len }
      override def next() = {
        val ret = (List(cnt.toDouble), List(-1.0), cnt.toString)
        cnt += 1
        ret
      }
    }

    RandomForest.getLabeledData(testIterator)
  }

}

class BiasedTreeClassifier(vote: String) extends SimpleTreeClassifier(tree=null) {

  override def classify(input: Vector) = (vote, 1.0)

}

@RunWith(classOf[JUnitRunner])
class RandomForestSpec extends FlatSpec with Matchers {

  "getLabeledData" should "transform  datastream to a list with valuas and a list with labels" in {
    val len = 100
    val (data, labels) = RandomForestSpec.labeledData(len)

    data.length should equal (len)
    labels.length should equal (len)
    data.zip(0 to (len-1)).foreach(el => {
      val (x, cnt) = el
      x.elementAt(0) should equal (cnt.toDouble)
    })

    labels.zip(0 to (len-1)).foreach(el => {
      val (y, cnt) = el
      y should equal (cnt.toString)
    })
  }

  "Samples" should "be of specified size" in {
    val (len, ratio) = (100, 0.75)
    val (data, labels) = RandomForestSpec.labeledData(len)
    val (sample1, sample2) = new RandomForestTrainer(1, 1, 1, ratio) sample(data, labels)
    sample1._1.size should equal (3 * len/4)
    sample1._2.size should equal (3 * len/4)
    sample2._1.size should equal (1 * len/4)
    sample2._2.size should equal (1 * len/4)
  }

  it should "be a 2-partition of provided data" in {
    val (len, ratio) = (100, 0.75)
    val (data, labels) = RandomForestSpec.labeledData(len)
    val (sample1, sample2) = new RandomForestTrainer(1, 1, 1, ratio) sample(data, labels)
    data.foreach(el => {
      if(sample1._1.contains(el)) sample2._1.contains(el) should be (false)
      else if(sample2._1.contains(el)) sample1._1.contains(el) should be (false)
      else fail
    })
  }

  "Normalized votes of Trees" should "count up to one" in {
    val treeClassifiers = for(cls <- List("A", "B", "A")) yield new BiasedTreeClassifier(cls)
    val rf = new SimpleForestClassifier(new Forest(treeClassifiers.toList), 1.0)

    val votes = rf.votes(new Vector(List()))
    votes.map(v => v._2).sum should equal (1.0)
  }

  "Classify" should "return the class and confidence of best most probable vote" in {
    val treeClassifiers = for(cls <- List("A", "B", "A")) yield new BiasedTreeClassifier(cls)
    val rf = new SimpleForestClassifier(new Forest(treeClassifiers.toList), 1.0)

    rf.classify(new Vector(List())) should equal (("A", 2.0/3.0))
  }

  "Out of Bag estimate" should "be the avg error of an out-of-bag set" in {
    val trainer = new RandomForestTrainer(100, 0, 0, 0.66)
    val data = List(new Vector(List()), new Vector(List()), new Vector(List()))
    val labels = List("1", "0", "1")

    val error = trainer.calculateOOBEstimate(new BiasedTreeClassifier("1"), data, labels)
    error should equal (1.0/3.0)
  }

}
