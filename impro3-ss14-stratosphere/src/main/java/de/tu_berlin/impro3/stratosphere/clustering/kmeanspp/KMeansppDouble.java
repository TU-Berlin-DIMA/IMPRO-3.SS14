package de.tu_berlin.impro3.stratosphere.clustering.kmeanspp;

import java.io.Serializable;

import de.tu_berlin.impro3.stratosphere.clustering.kmeanspp.util.GenericFunctions;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapFunction;

public class KMeansppDouble {
	
	public static class MyPoint implements Serializable {
		
		private static final long serialVersionUID = -8646398807053064759L;
		
		public double x, y;

		public MyPoint() {
		}

		public MyPoint(double x, double y) {
			this.x = x;
			this.y = y;
		}
		
		@Override
		public String toString() {
			return x + "|" + y;
		}
	}
	
	public static final class StringPointConverter extends MapFunction<String, MyPoint> {

		private static final long serialVersionUID = -7253773731266749932L;

		@Override
		public MyPoint map(String value) throws Exception {
			String[] tmp = value.split("\\|");
			return new MyPoint(Double.parseDouble(tmp[1]), Double.parseDouble(tmp[2]));
		}
	}

	public static class MyFunctions implements GenericFunctions<MyPoint> {

		private static final long serialVersionUID = 5510454279473390773L;
		
		private String pointsPath;
		
		public MyFunctions(String filePath) {
			this.pointsPath = filePath;
		}

		@Override
		public DataSet<MyPoint> getDataSet(ExecutionEnvironment env) {
			return env.readTextFile(pointsPath).map(new StringPointConverter());
		}

		@Override
		public MyPoint add(MyPoint in1, MyPoint in2) {
			return new MyPoint(in1.x + in2.x, in1.y + in2.y);
		}

		@Override
		public MyPoint div(MyPoint in1, long val) {
			in1.x /= val;
			in1.y /= val;
			return in1;
		}

		@Override
		public double distance(MyPoint in1, MyPoint in2) {
			double xdiff = in1.x - in2.x;
			double ydiff = in1.y - in2.y;
			return Math.sqrt(xdiff * xdiff + ydiff * ydiff);
		}
	}
	
	public static String getDescription() {
		return "Parameters: <numSubTasks> <inputPath> <outputDirectory> <numClusters> <maxIterations>";
	}
	

	public static void main(String[] args) throws Exception {
		if(args.length < 5) {
			System.out.println(getDescription());
			return;
		}
		int dop = Integer.parseInt(args[0]);
		int k = Integer.parseInt(args[3]);
		int itrs = Integer.parseInt(args[4]);

		KMeansppGeneric<MyPoint> kmp = new KMeansppGeneric<MyPoint>(dop, args[2], k, itrs);
		kmp.run(new MyFunctions(args[1]));
	}

}
