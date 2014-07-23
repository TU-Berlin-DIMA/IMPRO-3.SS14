package de.tu_berlin.impro3.scala.classification

import de.tu_berlin.impro3.core.Algorithm
import de.tu_berlin.impro3.scala.ScalaAlgorithm
import de.tu_berlin.impro3.scala.classification.logreg.LogReg
import de.tu_berlin.impro3.scala.common.AlgorithmTest
import net.sourceforge.argparse4j.inf.Namespace
import org.junit.Assert._
import org.junit.Test

object LogRegTest{

  //BASE_PATH
  val BASE_PATH = AlgorithmTest.resourcesPath + "/"
}

/**
 * This class contains several tests to validate the correctness of the logistic regression implementation written in
 * scala. The tests validate both, the BGD (Batch Gradient Descent) and SGD (Stochastic Gradient Descent), versions.
 *
 * The tests use test data sets created with the cub gen tool. The correct results were calculated with an log. reg.
 * implementation in Matlab according to the ML course helt by Andrew Ng (coursera).
 */
class LogRegTest {

  def getAlgorithm(ns: Namespace) : LogReg = {
    new LogReg(ns)
  }

  def getAlgorithm(input:String, version:String, alpha:Integer, iterations:Integer, trainingSubset:Integer) : LogReg = {

    val args = new java.util.HashMap[String, Object]
    args.put(LogReg.KEY_ALPHA, alpha)
    args.put(LogReg.KEY_GD, version)
    args.put(LogReg.KEY_TRAINING_SUBSET, trainingSubset)
    args.put(LogReg.KEY_ITERATIONS, iterations)
    args.put(ScalaAlgorithm.Command.KEY_DIMENSIONS, new Integer(1)) //Dimension is always 1 in current implementation
    args.put(Algorithm.Command.KEY_INPUT, input)
    args.put(Algorithm.Command.KEY_OUTPUT, "X") //No output will be generated

    getAlgorithm(new Namespace(args))
  }

  @Test
  def testReferenceDataSetWithBGD_10()={
    val a:LogReg=getAlgorithm(LogRegTest.BASE_PATH + "cub_gen_output_10","batch",100,5000,0)
    a.run()
    //ecpected result (calculated with matlab) is 6.1
    assertTrue(
      "The test run did not return the expected theta.",
      (a.theta.elementAt(0)>=6.0)&&(a.theta.elementAt(0)<=6.2)
    )
  }

  @Test
  def testReferenceDataSetWithBGD_100()={
    val a:LogReg=getAlgorithm(LogRegTest.BASE_PATH + "cub_gen_output_100","batch",100,5000,0)
    a.run()
    //ecpected result (calculated with matlab) is 1.3
    assertTrue(
      "The test run did not return the expected theta.",
      (a.theta.elementAt(0)>=1.2)&&(a.theta.elementAt(0)<=1.4)
    )
  }

  @Test
  def testReferenceDataSetWithBGD_1000()={
    val a:LogReg=getAlgorithm(LogRegTest.BASE_PATH + "cub_gen_output_1000","batch",100,5000,0)
    a.run()
    //ecpected result (calculated with matlab) is 1.7
    assertTrue(
      "The test run did not return the expected theta.",
      (a.theta.elementAt(0)>=1.6)&&(a.theta.elementAt(0)<=1.8)
    )
  }

  @Test
  def testReferenceDataSetWithSGD_10()={
    val a:LogReg=getAlgorithm(LogRegTest.BASE_PATH + "cub_gen_output_10","stochastic",10,5000,10)
    a.run()
    //ecpected result (calculated with matlab) is 6.1
    assertTrue(
      "The test run did not return the expected theta.",
      (a.theta.elementAt(0)>=6.0)&&(a.theta.elementAt(0)<=6.2)
    )
  }

  @Test
  def testReferenceDataSetWithSGD_100()={
    val a:LogReg=getAlgorithm(LogRegTest.BASE_PATH + "cub_gen_output_100","stochastic",1,500,100)
    a.run()
    //ecpected result (calculated with matlab) is 1.3
    assertTrue(
      "The test run did not return the expected theta.",
      (a.theta.elementAt(0)>=1.2)&&(a.theta.elementAt(0)<=1.4)
    )
  }

  @Test
  def testReferenceDataSetWithSGD_1000()={
    val a:LogReg=getAlgorithm(LogRegTest.BASE_PATH + "cub_gen_output_1000","stochastic",1,500,1000)
    a.run()
    //ecpected result (calculated with matlab) is 1.7
    assertTrue(
      "The test run did not return the expected theta.",
      (a.theta.elementAt(0)>=1.6)&&(a.theta.elementAt(0)<=1.8)
    )
  }

}
