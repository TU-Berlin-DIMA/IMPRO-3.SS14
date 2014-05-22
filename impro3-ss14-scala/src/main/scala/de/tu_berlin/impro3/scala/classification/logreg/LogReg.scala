package de.tu_berlin.impro3.scala.classification.logreg

import _root_.net.sourceforge.argparse4j.inf.Subparser
import _root_.de.tu_berlin.impro3.scala.Algorithm
import de.tu_berlin.impro3.scala.core.Vector
import de.tu_berlin.impro3.scala.clustering.kmeans.KVector
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * This object is used to read and store the arguments given for
 * the execution of logistic regression with gradient descent.
 */
object LogReg {

  // argument names
  val KEY_GD = "gradient-descent"
  val KEY_ITERATIONS = "iterations"
  val KEY_TRAINING_SUBSET = "trainingsubset"
  val KEY_ALPHA = "alpha"

  class Config extends Algorithm.Config[LogReg] {

    // algorithm names
    override val CommandName = "log-reg"
    override val Name = "Logistic Regression Classifier"

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)

      // add options (prefixed with --)
      parser.addArgument(s"--${LogReg.KEY_GD}")
        .`type`[String](classOf[String])
        .dest(LogReg.KEY_GD)
        .metavar("GD")
        .help("type of gradient descent: \"batch\" or \"stochastic\"")

      // add defaults for options
      parser.setDefault(LogReg.KEY_GD, new String("batch"))

      // add options (prefixed with --)
      parser.addArgument(s"--${LogReg.KEY_ITERATIONS}")
        .`type`[Integer](classOf[Integer])
        .dest(LogReg.KEY_ITERATIONS)
        .metavar("I")
        .help("\"batch\" and \"stochastic\": number of iterations for gradient descent")

      // add defaults for options
      parser.setDefault(LogReg.KEY_ITERATIONS, new Integer(100))

      // add options (prefixed with --)
      parser.addArgument(s"--${LogReg.KEY_ALPHA}")
        .`type`[Integer](classOf[Integer])
        .dest(LogReg.KEY_ALPHA)
        .metavar("A")
        .help("\"batch\" and \"stochastic\": learning rate alpha")

      // add defaults for options
      parser.setDefault(LogReg.KEY_ALPHA, new Integer(100))

      // add options (prefixed with --)
      parser.addArgument(s"--${LogReg.KEY_TRAINING_SUBSET}")
        .`type`[Integer](classOf[Integer])
        .dest(LogReg.KEY_TRAINING_SUBSET)
        .metavar("S")
        .help("\"stochastic\": number of elements that should be used for training (elements will be chosen randomly), default: all elements of the training set")

      // add defaults for options
      parser.setDefault(LogReg.KEY_TRAINING_SUBSET, new Integer(0))
    }
  }

}

/**
 * This is an implementation of logistic regression using batch or stochastic gradient descent.
 *
 * For batch gradient descent:
 *   It can be used to determine an optimal theta for the sigmoid function g(z)=(1/(1+e^(-z))) were z is used to be
 *   replaced according to the hypothesis h_theta(x)=(1/(1+e^(-theta*x))).
 *
 * For stochastic gradient descent:
 *  It can be used to determine a good estimation for theta especially when using large data sets.
 *
 * @param args Contains the following arguments:
 *             - type of gradient descent (--gradient-descent "batch" or "stochastic")
 *
 *             For "stochastic":
 *             - number of elements to build a random training subset from (--trainingsubset)
 *
 *             For "batch" and "stochastic":
 *             - learning rate alpha (in hundredth) (--alpha)
 *             - number of iterations (--iterations)
 *             - and standard arguments (--dimensions, INPUT, OUTPUT) as described in the super class
 */
class LogReg(args: Map[String, Object]) extends Algorithm(args) {

  // type of gradient descent
  val gd_type = arguments.get(LogReg.KEY_GD).get.asInstanceOf[String]

  // read list of point vectors
  val points: ArrayBuffer[Vector] = {
    val p = ArrayBuffer.newBuilder[Vector]
    for (i <- iterator()) p += new Vector(i._1)
    p.result()
  }

  // read list of labels
  val label: ArrayBuffer[Int] = {
    val p = ArrayBuffer.newBuilder[Int]
    for (i <- iterator()) p += Integer.parseInt(i._3)
    p.result()
  }

  // initialize theta
  var theta: Vector = new Vector(List.fill(points.head.elements)(0.0))

  // algorithm unspecific parameters
  val alpha =  arguments.get(LogReg.KEY_ALPHA).get.asInstanceOf[Int] / 100.toDouble;
  val number_of_iterations = arguments.get(LogReg.KEY_ITERATIONS).get.asInstanceOf[Int];

  def run(): Unit = {

    if(gd_type == "batch")
      batchGradientDescent()
    else if (gd_type == "stochastic")
      stochasticGradientDescent()
    else
      println("\n ERROR: UNKNOWN GRADIENT DESCENT")

    // optional used for plotting to debug

    var i = -1;
    for(point <- points) {
      i = i + 1
      print("{"+point.elementAt(0)+","+label(i)+"},")
    }


    println("\n==THETA==")
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

  def batchGradientDescent() : Unit = {

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
  }

  def stochasticGradientDescent() : Unit = {

    // algorithm specific parameters
    var training_subset = arguments.get(LogReg.KEY_TRAINING_SUBSET).get.asInstanceOf[Int];

    // default
    if(training_subset <= 0) {
      training_subset = points.length
    }

    training_subset = training_subset - 1

    val r = Random

    // randomly shuffle training (sub)set
    for(i <- 0 to training_subset) {
      val point_a = points(i)
      val label_a = label(i)

      val new_pos = r.nextInt(points.length)

      val point_b = points(new_pos)
      val label_b = label(new_pos)

      points.update(i, point_b)
      label.update(i, label_b)
      points.update(new_pos, point_a)
      label.update(new_pos, label_a)
    }

    var grad: Vector = new Vector(List.fill(points.head.elements)(0.0));

    for(j <- 1 to number_of_iterations) {

      // For every point of the training (sub)set
      for (i <- 0 to training_subset) {
        val point = points(i)

        // For all features
        for (feature <- 0 to points.head.elements - 1) {

          // Update gradient vector
          grad = new Vector(grad.components.updated(feature, (sigmoid(point * theta) - label(i)) * point.elementAt(feature)))
        }
        // Update theta with complete gradient
        theta = theta - (grad * alpha)
      }
    }
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
