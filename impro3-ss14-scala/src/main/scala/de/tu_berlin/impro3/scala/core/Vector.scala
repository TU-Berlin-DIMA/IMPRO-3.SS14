package de.tu_berlin.impro3.scala.core

object Vector {
  def apply(components: Array[Double]): Vector = apply(components.toList)

  def apply(components: List[Double]): Vector = new Vector(components)

  def apply(components: Double*): Vector = {
    if (components.isEmpty) throw new IllegalArgumentException("components == 0")
    apply(components.toList)
  }
}

/**
 * Vector class with basic methods.
 *
 * All arithmetic methods require the other vector has the same number of elements.
 *
 * @param components a list of entries for this vector
 */
class Vector(val components: List[Double]) {
  val elements = components.size

  def euclideanDistance(other: Vector) = {
    checkDimension(other)
    Math.sqrt((for ((a, b) <- components zip other.components) yield Math.pow(a - b, 2)).reduce(_ + _))
  }

  def elementAt(index: Int) = components(index)

  def +(other: Vector) = elementWise(other)(_ + _)

  def -(other: Vector) = elementWise(other)(_ - _)

  def *(other: Vector) = elementWise(other)(_ * _).components.reduce(_ + _)

  def *(r: Double) = new Vector(components.map(_ * r))

  def length() = Math.sqrt(components.map(x => x * x).reduce(_ + _))

  private def elementWise(other: Vector)(f: (Double, Double) => Double) = {
    checkDimension(other)
    new Vector(for ((a, b) <- components zip other.components) yield f(a, b))
  }

  private val checkDimension = (other: Vector) => if (elements != other.elements) throw new IllegalArgumentException("this#elements != other#elements")

  override def toString = {
    val strBuilder = new StringBuilder()
    components.addString(strBuilder, ", ")
    "(" + strBuilder.result() + ")"
  }

  def addString(b: StringBuilder, start: String, sep: String, end: String): StringBuilder = {
    components.addString(b, start, sep, end)
  }

  def addString(b: StringBuilder, sep: String): StringBuilder = addString(b, "", sep, "")

  def addString(b: StringBuilder): StringBuilder = addString(b, "")

  def canEqual(other: Any): Boolean = other.isInstanceOf[Vector]

  override def equals(other: Any): Boolean = other match {
    case that: Vector =>
      (that canEqual this) &&
        elements == that.elements &&
        components == that.components
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(elements, components)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}