package de.tu_berlin.impro3.spark.classification.logreg;

/**
 * A Vector that represents the gradient of a given point
 */
@SuppressWarnings("serial")
public class Gradient extends Vector {

    public Gradient() {
        // default constructor needed for instantiation during serialization
    }

    public Gradient(int size) {
        super(size);
    }
}
