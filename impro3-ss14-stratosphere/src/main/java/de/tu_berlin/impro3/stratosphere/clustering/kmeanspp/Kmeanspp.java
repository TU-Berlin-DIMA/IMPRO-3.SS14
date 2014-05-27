package de.tu_berlin.impro3.stratosphere.clustering.kmeanspp;

import java.util.Collection;
import java.util.Iterator;
import java.util.Random;

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

public class Kmeanspp {

	public static void main(String[] args) throws Exception {

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


		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<Point> points = env.readCsvFile(pointsPath)
			.fieldDelimiter('|')
			.includeFields("FTT")
			.types(Double.class, Double.class)
			.map(new TuplePointConverter());

		DataSet<Centroid> initial = points.reduce(new PickOnePoint()).map(new PointCentroidConverter());

		IterativeDataSet<Centroid> addition = initial.iterate(numClusters - 1);

		DataSet<Tuple2<Point, Double>> distance = points.map(new SelectNearestDistance()).withBroadcastSet(addition, "centroids");

		DataSet<Tuple2<Point, Double>> sum = distance.aggregate(Aggregations.SUM, 1);

		DataSet<Centroid> newCentroid = distance.reduceGroup(new PickWithDistance())
			.withBroadcastSet(sum.cross(addition), "sumcrosscentroid")
			.union(addition)
			.map(new DummyMap());


		DataSet<Centroid> centroids = addition.closeWith(newCentroid);

		// set number of bulk iterations for KMeans algorithm
		IterativeDataSet<Centroid> loop = centroids.iterate(numIterations);

		DataSet<Centroid> newCentroids = points
			// compute closest centroid for each point
			.map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
				// count and sum point coordinates for each centroid
			.map(new CountAppender())
			.groupBy(0).reduce(new CentroidAccumulator())
				// compute new centroids from point counts and coordinate sums
			.map(new CentroidAverager());

		// feed new centroids back into next iteration
		DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

		DataSet<Tuple2<Integer, Point>> clusteredPoints = points
			// assign points to final clusters
			.map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

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

	/**
	 * A simple two-dimensional point.
	 */
	public static class Point extends Tuple2<Double, Double> {


		public Point() {}

		public Point(double x, double y) {
			super(x, y);
		}

		public Point add(Point other) {
			f0 += other.f0;
			f1 += other.f1;
			return this;
		}

		public Point div(long val) {
			f0 /= val;
			f1 /= val;
			return this;
		}

		public double euclideanDistance(Point other) {
			return Math.sqrt((f0-other.f0)*(f0-other.f0) + (f1-other.f1)*(f1-other.f1));
		}

		@Override
		public String toString() {
			return f0 + "|" + f1;
		}
	}

	/**
	 * A simple two-dimensional centroid, basically a point with an ID.
	 */
	public static class Centroid extends Tuple3<Integer, Double, Double> {

		public Centroid() {}

		public Centroid(int id, double x, double y) {
			super(id, x, y);
		}

		public Centroid(int id, Point p) {
			super(id, p.f0, p.f1);
		}
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/** Converts a Tuple2<Double,Double> into a Point. */
	public static final class TuplePointConverter extends MapFunction<Tuple2<Double, Double>, Point> {

		@Override
		public Point map(Tuple2<Double, Double> t) throws Exception {
			return new Point(t.f0, t.f1);
		}
	}

	/** Converts a Tuple3<Integer, Double,Double> into a Centroid. */
	public static final class PointCentroidConverter extends MapFunction<Point, Centroid> {

		@Override
		public Centroid map(Point p) throws Exception {
			return new Centroid(1, p.f0, p.f1);
		}
	}

	public static final class DummyMap extends MapFunction<Centroid, Centroid> {

		@Override
		public Centroid map(Centroid p) throws Exception {
			return p;
		}
	}
	public static final class PickOnePoint extends ReduceFunction<Point> {

		@Override
		public Point reduce(Point point, Point point2) throws Exception {
			return point;
		}
	}

	public static final class PickWithDistance extends GroupReduceFunction<Tuple2<Point, Double>, Centroid> {
		private double sum;
		private int maxId = 0;

		@Override
		public void open(Configuration parameters) throws Exception {
			Collection<Tuple2<Tuple2<Point, Double>, Centroid>> sumWrapper;
			sumWrapper = getRuntimeContext().getBroadcastVariable("sumcrosscentroid");
			for (Tuple2<Tuple2<Point, Double>, Centroid> c: sumWrapper) {
				sum = c.f0.f1;
				if (c.f1.f0 > maxId) {
					maxId = c.f1.f0;
				}
			}
		}

		@Override
		public void reduce(Iterator<Tuple2<Point, Double>> distance, Collector<Centroid> out) throws Exception {

			Random r = new Random();
			double pos = r.nextDouble() * sum;
			while (distance.hasNext()) {
				Tuple2<Point, Double> d = distance.next();
				pos -= d.f1;
				if (pos < 0) {
					out.collect(new Centroid(maxId + 1, d.f0));
					return;
				}
			}
		}
	}

	/** Determines the closest cluster center for a data point. */
	public static final class SelectNearestCenter extends MapFunction<Point, Tuple2<Integer, Point>> {
		private Collection<Centroid> centroids;

		/** Reads the centroid values from a broadcast variable into a collection. */
		@Override
		public void open(Configuration parameters) throws Exception {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
		}

		@Override
		public Tuple2<Integer, Point> map(Point p) throws Exception {

			double minDistance = Double.MAX_VALUE;
			int closestCentroidId = -1;

			// check all cluster centers
			for (Centroid centroid : centroids) {
				// compute distance
				double distance = p.euclideanDistance(new Point(centroid.f1, centroid.f2));

				// update nearest cluster if necessary
				if (distance < minDistance) {
					minDistance = distance;
					closestCentroidId = centroid.f0;
				}
			}

			// emit a new record with the center id and the data point.
			return new Tuple2<Integer, Point>(closestCentroidId, p);
		}
	}

	/** Determines the closest cluster distance for a data point. */
	public static final class SelectNearestDistance extends MapFunction<Point, Tuple2<Point, Double>> {
		private Collection<Centroid> centroids;

		/** Reads the centroid values from a broadcast variable into a collection. */
		@Override
		public void open(Configuration parameters) throws Exception {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
		}

		@Override
		public Tuple2<Point, Double> map(Point p) throws Exception {

			double minDistance = Double.MAX_VALUE;

			// check all cluster centers
			for (Centroid centroid : centroids) {
				// compute distance
				double distance = p.euclideanDistance(new Point(centroid.f1, centroid.f2));

				// update nearest cluster if necessary
				if (distance < minDistance) {
					minDistance = distance;
				}
			}
			return new Tuple2<Point, Double>(p, minDistance * minDistance);
		}
	}

	/** Appends a count variable to the tuple. */
	public static final class CountAppender extends MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

		@Override
		public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
			return new Tuple3<Integer, Point, Long>(t.f0, t.f1, 1L);
		}
	}

	/** Sums and counts point coordinates. */
	public static final class CentroidAccumulator extends ReduceFunction<Tuple3<Integer, Point, Long>> {

		@Override
		public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
			return new Tuple3<Integer, Point, Long>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
		}
	}

	/** Computes new centroid from coordinate sum and count of points. */
	public static final class CentroidAverager extends MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

		@Override
		public Centroid map(Tuple3<Integer, Point, Long> value) {
			return new Centroid(value.f0, value.f1.div(value.f2));
		}
	}



	private static String pointsPath = null;
	private static String outputPath = null;
	private static int dop;
	private static int numPoints;
	private static int numClusters;
	private static int numIterations = 10;



}
