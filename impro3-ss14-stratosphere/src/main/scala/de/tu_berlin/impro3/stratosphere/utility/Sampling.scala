package de.tu_berlin.impro3.stratosphere.utility

import scala.util.Random

import _root_.eu.stratosphere.client.LocalExecutor
import _root_.eu.stratosphere.api.common.{Program, ProgramDescription}

import _root_.eu.stratosphere.api.scala._
import _root_.eu.stratosphere.api.scala.operators._

class Sampling extends Program with ProgramDescription with Serializable {

  val SEED: Long = 123456;

  def getScalaPlan(higgsInput: String, higgsOutput: String, size: Double) = {

    Random.setSeed(SEED)

    val higgsData = TextFile(higgsInput).filter( x => Random.nextDouble < size )

    val output = higgsData.write(higgsOutput, DelimitedOutputFormat({ line => line }))

    new ScalaPlan(Seq(output), "Sampling")
  }


  /**
   * The program entry point for the packaged version of the program.
   *
   * @param args The command line arguments, including, consisting of the following parameters:
   *             <input> <output> <chance>"
   * @return The program plan of the sample example program.
   */
  override def getPlan(args: String*) = {
    getScalaPlan(args(0), args(1), args(2).toDouble)
  }

  override def getDescription() = {
    "Parameters: <input> <output> <chance>"
  }

}

/**
 * Entry point to make the example standalone runnable with the local executor
 */
object RunSampling {

  def main(args: Array[String]) {
    val sampling = new Sampling
    if (args.size < 2) {
      println(sampling.getDescription)
      return
    }
    val plan = sampling.getScalaPlan(args(0), args(1), args(2).toDouble)
    LocalExecutor.execute(plan)
  }
}
