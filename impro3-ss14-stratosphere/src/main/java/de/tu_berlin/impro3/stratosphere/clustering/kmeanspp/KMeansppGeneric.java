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

public class KMeansppGeneric<T> implements Serializable {

	private static final long serialVersionUID = 3203582521287201233L;

	public KMeansppGeneric(int dop, String outputPath, int numClusters, int maxIterations) {
		this.dop = dop;
		this.outputPath = outputPath;
		this.numClusters = numClusters;
		this.numIterations = maxIterations;
	}

	public void run(GenericFunctions<T> function) throws Exception {


		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<T> points = function.getDataSet(env);
		
		// ========================== K-Means++ procedure ==================================

		DataSet<Tuple2<Integer, T>> initial = points.reduce(new PickOnePoint()).map(new PointCentroidConverter());

		IterativeDataSet<Tuple2<Integer, T>> addition = initial.iterate(numClusters - 1);

		DataSet<Tuple2<T, Double>> distance = points.map(new SelectNearestDistance(function)).withBroadcastSet(addition, "centroids");

		DataSet<Tuple2<T, Double>> sum = distance.aggregate(Aggregations.SUM, 1);

		DataSet<Tuple2<Integer, T>> newCenter = distance.reduceGroup(new PickWithDistance())
			.withBroadcastSet(sum.cross(addition), "sumcrosscentroid");

		DataSet<Tuple2<Integer, T>> centers = addition.reduceGroup(new AddNewCenter())
			.withBroadcastSet(newCenter, "newcentroid");

		DataSet<Tuple2<Integer, T>> initCenters = addition.closeWith(centers);
		initCenters.writeAsCsv(new Path(outputPath, "initcenters").toString(), "\n", "|");

		// ========================== standard K-Means procedure ==================================

		// set number of bulk iterations for KMeans algorithm
		IterativeDataSet<Tuple2<Integer, T>> iteration = initCenters.iterate(numIterations);

		DataSet<Tuple2<Integer, T>> kmeansTmpCenters = points
			// compute closest centroid for each point
			.map(new SelectNearestCenter(function)).withBroadcastSet(iteration, "centroids")
				// count and sum point coordinates for each centroid
			.map(new CountAppender())
			.groupBy(0).reduce(new CentroidAccumulator(function))
				// compute new centroids from point counts and coordinate sums
			.map(new CentroidAverager(function));

		// feed new centroids back into next iteration
		DataSet<Tuple2<Integer, T>> finalCentroids = iteration.closeWith(
				kmeansTmpCenters);

		DataSet<Tuple2<Integer, T>> clusteredPoints = points
			// assign points to final clusters
			.map(new SelectNearestCenter(function)).withBroadcastSet(finalCentroids, "centroids");

		// emit result
		clusteredPoints.writeAsCsv(new Path(outputPath, "points").toString(), "\n", "|");
		finalCentroids.writeAsCsv(new Path(outputPath, "centers").toString(), "\n", "|");
		// execute program
		env.setDegreeOfParallelism(dop);
		env.execute("KMeans++");

	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************


	/** Converts a Tuple3<Integer, Double,Double> into a Centroid. */
	public final class PointCentroidConverter extends MapFunction<T, Tuple2<Integer, T>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 6696423823857438943L;

		@Override
		public Tuple2<Integer, T> map(T p) throws Exception {
			return new Tuple2<Integer, T>(1, p);
		}
	}

	public final class PickOnePoint extends ReduceFunction<T> {

		private static final long serialVersionUID = -4173293469519514256L;

		@Override
		public T reduce(T point, T point2) throws Exception {
			return point;
		}
	}

	public final class PickWithDistance extends GroupReduceFunction<Tuple2<T, Double>, Tuple2<Integer, T>> {
		
		private static final long serialVersionUID = 2351745306551566932L;
		private double sum;
		private int maxId = 0;

		@Override
		public void open(Configuration parameters) throws Exception {
			Collection<Tuple2<Tuple2<T, Double>, Tuple2<Integer, T>>> sumWrapper;
			sumWrapper = getRuntimeContext().getBroadcastVariable("sumcrosscentroid");
			for (Tuple2<Tuple2<T, Double>, Tuple2<Integer, T>> c: sumWrapper) {
				sum = c.f0.f1;
				if (c.f1.f0 > maxId) {
					maxId = c.f1.f0;
				}
			}
		}

		@Override
		public void reduce(Iterator<Tuple2<T, Double>> distance, Collector<Tuple2<Integer, T>> out) throws Exception {

			Random r = new Random();
			double pos = r.nextDouble() * sum;

			while (distance.hasNext()) {
				Tuple2<T, Double> d = distance.next();
				pos -= d.f1;
				if (pos < 0) {
					out.collect(new Tuple2<Integer, T>(maxId + 1, d.f0));
					return;
				}
			}
		}
	}

	public final class AddNewCenter extends GroupReduceFunction<Tuple2<Integer, T>, Tuple2<Integer, T>> {

		private Collection<Tuple2<Integer, T>> centroid;


		@Override
		public void open(Configuration parameters) throws Exception {
			Collection<Tuple2<Tuple2<T, Double>, Tuple2<Integer, T>>> sumWrapper;
			centroid = getRuntimeContext().getBroadcastVariable("newcentroid");

		}

		@Override
		public void reduce(Iterator<Tuple2<Integer, T>> values, Collector<Tuple2<Integer, T>> out) throws Exception {
			while (values.hasNext()) {
				out.collect(values.next());
			}
			out.collect(centroid.iterator().next());
		}
	}
	/** Determines the closest cluster center for a data point. */
	public final class SelectNearestCenter extends MapFunction<T, Tuple2<Integer, T>> {

		private static final long serialVersionUID = 3830298690315137146L;
		private GenericFunctions<T> function;
		private Collection<Tuple2<Integer, T>> centroids;

		public SelectNearestCenter(GenericFunctions<T> function) {
			this.function = function;
		}
		/** Reads the centroid values from a broadcast variable into a collection. */
		@Override
		public void open(Configuration parameters) throws Exception {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
		}

		@Override
		public Tuple2<Integer, T> map(T p) throws Exception {

			double minDistance = Double.MAX_VALUE;
			int closestCentroidId = -1;

			// check all cluster centers
			for (Tuple2<Integer, T> centroid : centroids) {
				// compute distance

				double distance = function.distance(p, centroid.f1);

				// update nearest cluster if necessary
				if (distance < minDistance) {
					minDistance = distance;
					closestCentroidId = centroid.f0;
				}
			}

			// emit a new record with the center id and the data point.
			return new Tuple2<Integer, T>(closestCentroidId, p);
		}
	}

	/** Determines the closest cluster distance for a data point. */
	public final class SelectNearestDistance extends MapFunction<T, Tuple2<T, Double>> {

		private static final long serialVersionUID = -8867196536007432104L;
		private Collection<Tuple2<Integer, T>> centroids;
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
		public Tuple2<T, Double> map(T p) throws Exception {

			double minDistance = Double.MAX_VALUE;

			// check all cluster centers
			for (Tuple2<Integer, T> centroid : centroids) {
				// compute distance

				double distance = function.distance(p, centroid.f1);

				// update nearest cluster if necessary
				if (distance < minDistance) {
					minDistance = distance;
				}
			}
			return new Tuple2<T, Double>(p, minDistance * minDistance);
		}
	}

	/** Appends a count variable to the tuple. */
	public final class CountAppender extends MapFunction<Tuple2<Integer, T>, Tuple3<Integer, T, Long>> {

		private static final long serialVersionUID = -3743500564243984677L;

		@Override
		public Tuple3<Integer, T, Long> map(Tuple2<Integer, T> t) {
			return new Tuple3<Integer, T, Long>(t.f0, t.f1, 1L);
		}
	}

	/** Sums and counts point coordinates. */
	public final class CentroidAccumulator extends ReduceFunction<Tuple3<Integer, T, Long>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 3668504702360578838L;
		private GenericFunctions<T> function;

		public CentroidAccumulator(GenericFunctions<T> function) {
			this.function = function;
		}
		@Override
		public Tuple3<Integer, T, Long> reduce(Tuple3<Integer, T, Long> val1, Tuple3<Integer, T, Long> val2) {
			return new Tuple3<Integer, T, Long>(val1.f0, function.add(val1.f1, val2.f1), val1.f2 + val2.f2);
		}
	}

	/** Computes new centroid from coordinate sum and count of points. */
	public final class CentroidAverager extends MapFunction<Tuple3<Integer, T, Long>, Tuple2<Integer, T>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -439825558772265542L;
		private GenericFunctions<T> function;

		public CentroidAverager(GenericFunctions<T> function) {
			this.function = function;
		}
		@Override
		public Tuple2<Integer, T> map(Tuple3<Integer, T, Long> value) {
			T tmp = function.div(value.f1, value.f2);
			return new Tuple2<Integer, T>(value.f0, tmp);
		}
	}

	private String outputPath = null;
	private int dop;
	private int numClusters;
	private int numIterations;
	
}
