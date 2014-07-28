package de.tu_berlin.impro3.stratosphere.clustering.kmeanspp.util;

import java.io.Serializable;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;

/**
 * The Generic function to be passed to KMeans++ algorithm, which can be used to define the input
 * format, get the distance matrix and calculate the prototype of new cluster
 * 
 * @param <T> genric type
 */
public interface GenericFunctions<T> extends Serializable {

    /**
     * User defined input format.
     * 
     * @param env execution enviroment
     * @return input dataset
     */
    public DataSet<T> getDataSet(ExecutionEnvironment env);

    /**
     * User defined Addition function for input data. Together with div function to calculate the
     * average.
     * 
     * @param in1 input1
     * @param in2 input2
     * @return sum
     */
    public T add(T in1, T in2);

    /**
     * User defined division function for input data.
     * 
     * @param in1 sum
     * @param val count
     * @return average
     */
    public T div(T in1, long val);


    /**
     * User defined distance measurement.
     * 
     * @param in1 input1
     * @param in2 input2
     * @return distance
     */
    public double distance(T in1, T in2);
}
