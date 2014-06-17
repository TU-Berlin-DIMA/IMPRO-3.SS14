package de.tu_berlin.impro3.scala.clustering.kmeansplusplus

import scala.io.Source
import de.tu_berlin.impro3.scala.core.Vector
import de.tu_berlin.impro3.scala.Algorithm
import org.junit.Assert._
import org.junit.Test
import de.tu_berlin.impro3.scala.clustering.kmeanspp.KMeansPlusPlus
import de.tu_berlin.impro3.scala.common.AlgorithmTest


object KMeansPlusPlusTest {
	def validate(dimensions: Int, scale: Int, cardinality: Int, file: String) {
		val reader = Source.fromFile(file).bufferedReader()
		val calculatedCenters = {
			val builder = List.newBuilder[Vector]
			while (reader.ready()) {
				val line = reader.readLine().split( """\""" + Algorithm.DELIMITER)
				val vector = Vector((for (i <- 0 until dimensions) yield line(i).trim.toDouble).toList)
				builder += vector
			}
			builder.result()
		}

		for (dim <- 0 until Math.pow(2.0, dimensions).toInt) {
			val perfectCenter = Vector((for (i <- 0 until dimensions) yield if (((dim >> i) & 1) == 1) 1.0 * scale else 0.0).toList)
			val matched = calculatedCenters.filter(
				calculatedCenter => {
					calculatedCenter.euclideanDistance(perfectCenter) <= AlgorithmTest.Delta
				}
			)

			assertEquals("no matching center for: " + perfectCenter, 1, matched.size)
			println("matched center " + perfectCenter + " => " + matched.head + " with [" + matched.head.euclideanDistance(perfectCenter) + "]")
		}
	}
}

class KMeansPlusPlusTest extends AlgorithmTest {

	def getAlgorithm(args: Map[String, Object]) = {
		new KMeansPlusPlus(args)
	}

	@Test
	def test_3_100_1000() = {
		test(3, 100, 1000)(Map((KMeansPlusPlus.KEY_K, new Integer(Math.pow(2.0, 3).toInt))))
	}

	@Test
	def test_4_100_1000() = {
		test(4, 100, 1000)(Map((KMeansPlusPlus.KEY_K, new Integer(Math.pow(2.0, 4).toInt))))
	}

	@Test
	def test_5_100_1000() = {
		test(5, 100, 1000)(Map((KMeansPlusPlus.KEY_K, new Integer(Math.pow(2.0, 5).toInt))))
	}

	def validate(dimensions: Int, scale: Int, cardinality: Int, file: String) = KMeansPlusPlusTest.validate(dimensions, scale, cardinality, file)
}

