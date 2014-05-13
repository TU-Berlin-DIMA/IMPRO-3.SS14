package de.tu_berlin.impro3.scala.classification.logreg

import _root_.net.sourceforge.argparse4j.inf.Subparser
import _root_.de.tu_berlin.impro3.scala.Algorithm
import de.tu_berlin.impro3.scala.core.Vector
import de.tu_berlin.impro3.scala.clustering.kmeans.KVector

/**
 * This object is used to read and store the arguments given for
 * the execution of logistic regression with gradient descent.
 */
object LogReg {

  // argument names
  val KEY_ITERATIONS = "iterations"
  val KEY_ALPHA = "alpha"

  class Config extends Algorithm.Config[LogReg] {

    // algorithm names
    override val CommandName = "log-reg"
    override val Name = "Logistic Regression Classifier"

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)

      // add options (prefixed with --)
      parser.addArgument(s"--${LogReg.KEY_ITERATIONS}")
        .`type`[Integer](classOf[Integer])
        .dest(LogReg.KEY_ITERATIONS)
        .metavar("I")
        .help("number of iterations for gradient descent")

      // add defaults for options
      parser.setDefault(LogReg.KEY_ITERATIONS, new Integer(100))

      // add options (prefixed with --)
      parser.addArgument(s"--${LogReg.KEY_ALPHA}")
        .`type`[Integer](classOf[Integer])
        .dest(LogReg.KEY_ALPHA)
        .metavar("A")
        .help("learning rate alpha")

      // add defaults for options
      parser.setDefault(LogReg.KEY_ALPHA, new Integer(100))
    }
  }

}

/**
 * This is an implementation of logistic regression using gradient descent.
 * It can be used to determine an optimal theta for the sigmoid function g(z)=(1/(1+e^(-z))) were z is used to be
 * replaced according to the hypothesis h_theta(x)=(1/(1+e^(-theta*x))).
 * @param args Contains the following arguments:
 *             - number of iterations (--iterations)
 *             - learning rate alpha (in hundredth) (--alpha)
 *             - and standard arguments (--dimensions, INPUT, OUTPUT) as described in the super class
 */
class LogReg(args: Map[String, Object]) extends Algorithm(args) {

  // algorithm specific parameters
  val number_of_iterations = arguments.get(LogReg.KEY_ITERATIONS).get.asInstanceOf[Int];
  val alpha =  arguments.get(LogReg.KEY_ALPHA).get.asInstanceOf[Int] / 100.toDouble;

  // read list of point vectors
  val points: List[Vector] = {
    val p = List.newBuilder[Vector]
    for (i <- iterator()) p += new Vector(i._1)
    p.result()
  }

  // read list of labels
  val label: List[Int] = {
    val p = List.newBuilder[Int]
    for (i <- iterator()) p += Integer.parseInt(i._3)
    p.result()
  }

  // initialize theta
  var theta: Vector = new Vector(List.fill(points.head.elements)(0.0))

  def run(): Unit = {

   // optional used for plotting to debug
  /*
    var i = -1;
    for(point <- points) {
      i = i + 1
      print("{"+point.elementAt(0)+","+label(i)+"},")
    }
    */
    for(i <- 1 to number_of_iterations) {

      var grad: Vector = new Vector(List.fill(points.head.elements)(0.0));

      // For all features
      for(feature <- 0 to points.head.elements-1) {

        var i = -1;
        // For every point of the training set
        for(point <- points) {
          i = i + 1

          // Calculate new weighted gradient
          val new_grad = ((sigmoid(point * theta) - label(i)) * point.elementAt(feature)) * (1.toDouble/points.size.toDouble)

          // Update gradient vector
          grad = new Vector(grad.components.updated(feature, grad.elementAt(feature) +  new_grad))
        }
      }
      // Update theta with complete gradient
      theta = theta - (grad * alpha)
    }

    println("==OPTIMAL THETA==")
    println(theta)

    println("==EVALUATION==")
    var a = -1;
    var correct_classified = 0;
    for(point <- points) {
      a = a + 1
      var prob = sigmoid(point*theta)
      println(point+": Probability: "+prob+" is: "+label(a))
      if(prob >= 0.5 && label(a) == 1 || prob < 0.5 && label(a) == 0)
        correct_classified = correct_classified + 1
    }
    println("\n CORRECT CLASSIFIED: "+correct_classified)
  }

  /**
   * Calculates the sigmoid function
   * @param x the x value (1st axis)
   * @return the value of the sigmoid function at the given x position
   */
  def sigmoid(x: Double): Double = {
    return 1.0 / (1.0 + math.pow(math.E, -x))
  }
}
