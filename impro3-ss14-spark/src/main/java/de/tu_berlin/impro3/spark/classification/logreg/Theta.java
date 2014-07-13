package de.tu_berlin.impro3.spark.classification.logreg;

/**
 * A Vector that represents the Theta that the optimization is working on
 */
@SuppressWarnings("serial")
public class Theta extends Vector {

    public Theta() {
        // default constructor needed for instantiation during serialization
    }

    public Theta(int size) {
        super(size);
    }
}
