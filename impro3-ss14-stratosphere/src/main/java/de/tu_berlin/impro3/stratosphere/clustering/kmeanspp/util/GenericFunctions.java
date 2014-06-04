package de.tu_berlin.impro3.stratosphere.clustering.kmeanspp.util;

import eu.stratosphere.api.java.tuple.Tuple2;

import java.io.Serializable;

/**
 * Created by qml_moon on 28/05/14.
 */
public interface GenericFunctions<T> extends Serializable {

	public Tuple2<T, T> add(Tuple2<T, T> in1, Tuple2<T, T> in2);

	public Tuple2<T, T> div(Tuple2<T, T> in1, long val);

	public double distance(Tuple2<T, T> in1, Tuple2<T, T> in2);
}
