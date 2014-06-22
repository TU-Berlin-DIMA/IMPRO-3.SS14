package de.tu_berlin.impro3.stratosphere.clustering.kmeanspp.util;

import java.io.Serializable;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;

public interface GenericFunctions<T> extends Serializable  {
	
	public DataSet<T> getDataSet(ExecutionEnvironment env);
	
	public T add(T in1, T in2);

	public T div(T in1, long val);

	public double distance(T in1, T in2);
}
