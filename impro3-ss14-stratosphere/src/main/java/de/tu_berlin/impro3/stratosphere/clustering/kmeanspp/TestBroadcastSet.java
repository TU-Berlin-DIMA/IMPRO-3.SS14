package de.tu_berlin.impro3.stratosphere.clustering.kmeanspp;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;

/**
 * Created by qml_moon on 30/06/14.
 */
public class TestBroadcastSet {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Integer, Integer>> point = env.readCsvFile(args[1])
			.fieldDelimiter(' ')
			.types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> initial = point.reduce(new PickOnePoint());

		DataSet<Tuple2<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>> d = point.crossWithTiny(initial);

		d.print();

		env.setDegreeOfParallelism(Integer.parseInt(args[0]));

		env.execute();
	}


	public static final class PickOnePoint extends ReduceFunction<Tuple3<Integer, Integer, Integer>> {

		private static final long serialVersionUID = -4173293469519514256L;

		@Override
		public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> point, Tuple3<Integer, Integer, Integer> point2) throws Exception {
			return point;
		}
	}
}
