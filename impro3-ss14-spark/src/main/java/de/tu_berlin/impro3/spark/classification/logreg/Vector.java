package de.tu_berlin.impro3.spark.classification.logreg;

import java.io.Serializable;

/**
 * This is a convenience class that represents a vector of doubles
 */
@SuppressWarnings("serial")
public class Vector implements Serializable {

    double[] f0;

    public Vector() {
        // default constructor needed for instantiation during serialization
    }

    /**
     * Constructs a new Vector of given size
     */
    public Vector(int size) {
        double[] components = new double[size];
        for (int i = 0; i < size; i++) {
            components[i] = 0.0;
        }
        setComponents(components);
    }

    /**
     * Returns the double vector that represents the components of this Vector
     */
    public double[] getComponents() {
        return this.f0;
    }

    /**
     * Returns the component at position i
     */
    public double getComponent(int i) {
        return this.f0[i];
    }

    /**
     * Sets the component at position i with the given value
     */
    public void setComponent(int i, double value) {
        this.f0[i] = value;
    }

    /**
     * Sets the whole component vector
     */
    public void setComponents(double[] components) {
        this.f0 = components;
    }

}
