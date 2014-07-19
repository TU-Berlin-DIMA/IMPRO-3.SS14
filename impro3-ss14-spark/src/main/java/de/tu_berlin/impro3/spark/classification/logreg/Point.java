package de.tu_berlin.impro3.spark.classification.logreg;

/**
 * This class represents a labeled point in space. It consists of a feature vector (represented as
 * array of doubles) and an Integer label.
 */
public class Point {

    Integer f0;

    double[] f1;

    /**
     * Returns the vector of features
     * 
     * @return vector of features
     */
    public double[] getFeatures() {
        return this.f1;
    }

    /**
     * Returns the feature at position i
     * 
     * @return feature at position i
     */
    public double getFeature(int i) {
        return this.f1[i];
    }

    /**
     * Sets the feature vector
     */
    public void setFeatures(double[] features) {
        this.f1 = features;
    }

    /**
     * Returns the label
     * 
     * @return label
     */
    public Integer getLabel() {
        return this.f0;
    }

    /**
     * Sets the label
     * 
     * @param label The label
     */
    public void setLabel(Integer label) {
        this.f0 = label;
    }

}
