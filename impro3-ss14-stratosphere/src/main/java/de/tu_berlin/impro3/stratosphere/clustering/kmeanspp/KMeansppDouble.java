package de.tu_berlin.impro3.stratosphere.clustering.kmeanspp;

import de.tu_berlin.impro3.stratosphere.clustering.kmeanspp.util.GenericFunctions;
import eu.stratosphere.api.java.tuple.Tuple2;

import java.io.Serializable;

/**
 * Created by qml_moon on 28/05/14.
 */
public class KMeansppDouble {

	/**
	 * A simple two-dimensional point.
	 */
	public static class DoubleFunction implements GenericFunctions<Double> {

		public DoubleFunction(){
		}

		@Override
		public Tuple2<Double, Double> add(Tuple2<Double, Double> in1, Tuple2<Double, Double> in2) {
			return new Tuple2<Double, Double>(in1.f0 + in2.f0, in1.f1 + in2.f1);
		}

		@Override
		public Tuple2<Double, Double> div(Tuple2<Double, Double> in1, long val) {
			return new Tuple2<Double, Double>(in1.f0 / val, in1.f1 / val);
		}

		@Override
		public double distance(Tuple2<Double, Double> in1, Tuple2<Double, Double> in2) {
			return Math.sqrt((in1.f0 - in2.f0)*(in1.f0 - in2.f0) + (in1.f1 - in2.f1)*(in1.f1 - in2.f1));
		}
	}

	public static void main(String[] args) throws Exception {
		KMeansppGeneric<Double> kmp = new KMeansppGeneric<Double>(args);
		kmp.run(Double.class, new DoubleFunction());
	}
}
