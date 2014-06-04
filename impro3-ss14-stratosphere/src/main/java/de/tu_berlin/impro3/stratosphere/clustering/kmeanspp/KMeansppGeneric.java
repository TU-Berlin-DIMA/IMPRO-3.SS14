package de.tu_berlin.impro3.stratosphere.clustering.kmeanspp;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;

import de.tu_berlin.impro3.stratosphere.clustering.kmeanspp.util.GenericFunctions;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.util.Collector;
import sun.net.www.content.text.Generic;

public class KMeansppGeneric<T> implements Serializable {

	public KMeansppGeneric(String[] args) {
		if (args.length < 6) {
			System.out.println(GetDescription());
			return;
		}
		dop = Integer.parseInt(args[0]);
		pointsPath = args[1];
		outputPath = args[2];
		numPoints = Integer.parseInt(args[3]);
		numClusters = Integer.parseInt(args[4]);
		numIterations = Integer.parseInt(args[5]);
	}

	public void run(Class<T> typeClass, GenericFunctions<T> function) throws Exception {


		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<Tuple2<T, T>> points = env.readCsvFile(pointsPath)
			.fieldDelimiter('|')
			.includeFields("FTT")
			.types(typeClass, typeClass);

		DataSet<Tuple3<Integer, T, T>> initial = points.reduce(new PickOnePoint()).map(new PointCentroidConverter());

		IterativeDataSet<Tuple3<Integer, T, T>> addition = initial.iterate(numClusters - 1);

		DataSet<Tuple2<Tuple2<T, T>, Double>> distance = points.map(new SelectNearestDistance(function)).withBroadcastSet(addition, "centroids");

		DataSet<Tuple2<Tuple2<T, T>, Double>> sum = distance.aggregate(Aggregations.SUM, 1);

		DataSet<Tuple3<Integer, T, T>> newCentroid = distance.reduceGroup(new PickWithDistance())
			.withBroadcastSet(sum.cross(addition), "sumcrosscentroid")
			.union(addition)
			.map(new DummyMap());


		DataSet<Tuple3<Integer, T, T>> centroids = addition.closeWith(newCentroid);

		// set number of bulk iterations for KMeans algorithm
		IterativeDataSet<Tuple3<Integer, T, T>> loop = centroids.iterate(numIterations);

		DataSet<Tuple3<Integer, T, T>> newCentroids = points
			// compute closest centroid for each point
			.map(new SelectNearestCenter(function)).withBroadcastSet(loop, "centroids")
				// count and sum point coordinates for each centroid
			.map(new CountAppender())
			.groupBy(0).reduce(new CentroidAccumulator(function))
				// compute new centroids from point counts and coordinate sums
			.map(new CentroidAverager(function));

		// feed new centroids back into next iteration
		DataSet<Tuple3<Integer, T, T>> finalCentroids = loop.closeWith(newCentroids);

		DataSet<Tuple2<Integer, Tuple2<T, T>>> clusteredPoints = points
			// assign points to final clusters
			.map(new SelectNearestCenter(function)).withBroadcastSet(finalCentroids, "centroids");

		// emit result
		clusteredPoints.writeAsCsv(new Path(outputPath, "points").toString(), "\n", "|");
		finalCentroids.writeAsCsv(new Path(outputPath, "centers").toString(), "\n", "|");
		// execute program
		env.setDegreeOfParallelism(dop);
		env.execute("KMeans++");

	}

	public static String GetDescription() {
		return "Parameters: <numSubStasks> <dataPoints> <output> <pointsNum> <clustersNum>  <numIterations>";
	}




	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************


	/** Converts a Tuple3<Integer, Double,Double> into a Centroid. */
	public final class PointCentroidConverter extends MapFunction<Tuple2<T, T>, Tuple3<Integer, T, T>> {

		@Override
		public Tuple3<Integer, T, T> map(Tuple2<T, T> p) throws Exception {
			return new Tuple3<Integer, T, T>(1, p.f0, p.f1);
		}
	}

	public final class DummyMap extends MapFunction<Tuple3<Integer, T, T>, Tuple3<Integer, T, T>> {

		@Override
		public Tuple3<Integer, T, T> map(Tuple3<Integer, T, T> p) throws Exception {
			return p;
		}
	}
	public final class PickOnePoint extends ReduceFunction<Tuple2<T, T>> {

		@Override
		public Tuple2<T, T> reduce(Tuple2<T, T> point, Tuple2<T, T> point2) throws Exception {
			return point;
		}
	}

	public final class PickWithDistance extends GroupReduceFunction<Tuple2<Tuple2<T, T>, Double>, Tuple3<Integer, T, T>> {
		private double sum;
		private int maxId = 0;

		@Override
		public void open(Configuration parameters) throws Exception {
			Collection<Tuple2<Tuple2<Tuple2<T, T>, Double>, Tuple3<Integer, T, T>>> sumWrapper;
			sumWrapper = getRuntimeContext().getBroadcastVariable("sumcrosscentroid");
			for (Tuple2<Tuple2<Tuple2<T, T>, Double>, Tuple3<Integer, T, T>> c: sumWrapper) {
				sum = c.f0.f1;
				if (c.f1.f0 > maxId) {
					maxId = c.f1.f0;
				}
			}
		}

		@Override
		public void reduce(Iterator<Tuple2<Tuple2<T, T>, Double>> distance, Collector<Tuple3<Integer, T, T>> out) throws Exception {

			Random r = new Random();
			double pos = r.nextDouble() * sum;

			while (distance.hasNext()) {
				Tuple2<Tuple2<T, T>, Double> d = distance.next();
				pos -= d.f1;
				if (pos < 0) {
					out.collect(new Tuple3<Integer, T, T>(maxId + 1, d.f0.f0, d.f0.f1));
					return;
				}
			}
		}
	}

	/** Determines the closest cluster center for a data point. */
	public final class SelectNearestCenter extends MapFunction<Tuple2<T, T>, Tuple2<Integer, Tuple2<T, T>>> {

		private GenericFunctions<T> function;
		private Collection<Tuple3<Integer, T, T>> centroids;

		public SelectNearestCenter(GenericFunctions<T> function) {
			this.function = function;
		}
		/** Reads the centroid values from a broadcast variable into a collection. */
		@Override
		public void open(Configuration parameters) throws Exception {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
		}

		@Override
		public Tuple2<Integer, Tuple2<T, T>> map(Tuple2<T, T> p) throws Exception {

			double minDistance = Double.MAX_VALUE;
			int closestCentroidId = -1;

			// check all cluster centers
			for (Tuple3<Integer, T, T> centroid : centroids) {
				// compute distance

				double distance = function.distance(p, new Tuple2<T, T>(centroid.f1, centroid.f2));

				// update nearest cluster if necessary
				if (distance < minDistance) {
					minDistance = distance;
					closestCentroidId = centroid.f0;
				}
			}

			// emit a new record with the center id and the data point.
			return new Tuple2<Integer, Tuple2<T, T>>(closestCentroidId, p);
		}
	}

	/** Determines the closest cluster distance for a data point. */
	public final class SelectNearestDistance extends MapFunction<Tuple2<T, T>, Tuple2<Tuple2<T, T>, Double>> {

		private Collection<Tuple3<Integer, T, T>> centroids;
		private GenericFunctions<T> function;

		public SelectNearestDistance(GenericFunctions<T> function) {
			this.function = function;
		}

		/** Reads the centroid values from a broadcast variable into a collection. */
		@Override
		public void open(Configuration parameters) throws Exception {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
		}

		@Override
		public Tuple2<Tuple2<T, T>, Double> map(Tuple2<T, T> p) throws Exception {

			double minDistance = Double.MAX_VALUE;

			// check all cluster centers
			for (Tuple3<Integer, T, T> centroid : centroids) {
				// compute distance

				double distance = function.distance(p, new Tuple2<T, T>(centroid.f1, centroid.f2));

				// update nearest cluster if necessary
				if (distance < minDistance) {
					minDistance = distance;
				}
			}
			return new Tuple2<Tuple2<T, T>, Double>(p, minDistance * minDistance);
		}
	}

	/** Appends a count variable to the tuple. */
	public final class CountAppender extends MapFunction<Tuple2<Integer, Tuple2<T, T>>, Tuple3<Integer, Tuple2<T, T>, Long>> {

		@Override
		public Tuple3<Integer, Tuple2<T, T>, Long> map(Tuple2<Integer, Tuple2<T, T>> t) {
			return new Tuple3<Integer, Tuple2<T, T>, Long>(t.f0, t.f1, 1L);
		}
	}

	/** Sums and counts point coordinates. */
	public final class CentroidAccumulator extends ReduceFunction<Tuple3<Integer, Tuple2<T, T>, Long>> {

		private GenericFunctions<T> function;

		public CentroidAccumulator(GenericFunctions<T> function) {
			this.function = function;
		}
		@Override
		public Tuple3<Integer, Tuple2<T, T>, Long> reduce(Tuple3<Integer, Tuple2<T, T>, Long> val1, Tuple3<Integer, Tuple2<T, T>, Long> val2) {
			return new Tuple3<Integer, Tuple2<T, T>, Long>(val1.f0, function.add(val1.f1, val2.f1), val1.f2 + val2.f2);
		}
	}

	/** Computes new centroid from coordinate sum and count of points. */
	public final class CentroidAverager extends MapFunction<Tuple3<Integer, Tuple2<T, T>, Long>, Tuple3<Integer, T, T>> {

		private GenericFunctions<T> function;

		public CentroidAverager(GenericFunctions<T> function) {
			this.function = function;
		}
		@Override
		public Tuple3<Integer, T, T> map(Tuple3<Integer, Tuple2<T, T>, Long> value) {
			Tuple2<T, T> tmp = function.div(value.f1, value.f2);
			return new Tuple3<Integer, T, T>(value.f0, tmp.f0, tmp.f1);
		}
	}



	private static String pointsPath = null;
	private static String outputPath = null;
	private static int dop;
	private static int numPoints;
	private static int numClusters;
	private static int numIterations = 10;



}
