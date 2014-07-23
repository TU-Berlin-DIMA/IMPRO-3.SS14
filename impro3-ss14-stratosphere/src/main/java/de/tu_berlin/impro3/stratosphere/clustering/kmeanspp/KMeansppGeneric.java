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
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.util.Collector;

/**
 * Generic KMeans++ implementation.
 * 
 * @param <T> User defined data type.
 */
public class KMeansppGeneric<T> implements Serializable {

    private static final long serialVersionUID = 3203582521287201233L;

    private String outputPath = null;

    private int numClusters;

    private int numIterations;

    public KMeansppGeneric(String outputPath, int numClusters, int maxIterations) {
        this.outputPath = outputPath;
        this.numClusters = numClusters;
        this.numIterations = maxIterations;
    }

    /**
     * KMeans++ plan Implementation.
     * 
     * @param function user defined function for input, prototype calculation and distance measure.
     * @throws Exception
     */
    public void run(GenericFunctions<T> function) throws Exception {


        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataSet<T> points = function.getDataSet(env);

        /**
         * Due to the network buffer bug on the current version of stratosphere, we have to use this
         * mirror datasource trick in order to avoid potential self cross. A simple example program
         * to illustrate the bug is here {@link TestBroadcastSet}
         */
        DataSet<T> points2 = function.getDataSet(env);

        DataSet<T> points3 = function.getDataSet(env);

        DataSet<T> points4 = function.getDataSet(env);


        // ========================== K-Means++ procedure ==================================

        // pick one point as the initial centroid.
        DataSet<Tuple2<Integer, T>> initial = points.reduce(new PickOnePoint()).map(new PointCentroidConverter());


        // add the next k - 1 centroid one by one, based on the distance.
        IterativeDataSet<Tuple2<Integer, T>> addition = initial.iterate(numClusters - 1);

        // compute the distance square of the data points to the nearest centroid.
        DataSet<Tuple2<T, Double>> distance = points2.map(new SelectNearestDistance(function)).withBroadcastSet(addition, "centroids");

        DataSet<Tuple2<T, Double>> distance2 = points3.map(new SelectNearestDistance(function)).withBroadcastSet(addition, "centroids");

        // get the sum of the distance square.
        DataSet<Tuple2<T, Double>> sum = distance.aggregate(Aggregations.SUM, 1);

        // random pick a new centroid based on the value of distance square.
        DataSet<Tuple2<Integer, T>> newCenter =
                distance2.reduceGroup(new PickWithDistance()).withBroadcastSet(sum.cross(addition), "sumcrosscentroid");

        // union the new centroid,
        // we use reduceGroup instead because union operator doesn't work well with broadcastSet
        DataSet<Tuple2<Integer, T>> centers = addition.reduceGroup(new AddNewCenter()).withBroadcastSet(newCenter, "newcentroid");

        DataSet<Tuple2<Integer, T>> initCenters = addition.closeWith(centers);

        // ========================== standard K-Means procedure ==================================

        // set number of bulk iterations for KMeans algorithm
        IterativeDataSet<Tuple2<Integer, T>> iteration = initCenters.iterate(numIterations);

        //@formatter:off
        DataSet<Tuple2<Integer, T>> kmeansTmpCenters = points2
            .map(new SelectNearestCenter(function)).withBroadcastSet(iteration, "centroids") // compute closest centroid for each point
            .map(new CountAppender()) // count and sum point coordinates for each centroid
            .groupBy(0).reduce(new CentroidAccumulator(function)) // compute new centroids from point counts and coordinate sums
            .map(new CentroidAverager(function));
        //@formatter:on

        // feed new centroids back into next iteration
        DataSet<Tuple2<Integer, T>> finalCentroids = iteration.closeWith(kmeansTmpCenters);

        // assign points to final clusters
        DataSet<Tuple2<Integer, T>> clusteredPoints = points4.map(new SelectNearestCenter(function)).withBroadcastSet(finalCentroids, "centroids");

        // emit result
        finalCentroids.writeAsCsv(new Path(outputPath, "centers").toString(), "\n", "|", FileSystem.WriteMode.OVERWRITE);
        clusteredPoints.writeAsCsv(new Path(outputPath, "clusters").toString(), "\n", "|", FileSystem.WriteMode.OVERWRITE);
        // execute program
        env.execute("KMeans++");

    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Converts a Tuple3<Integer, Double,Double> into a Centroid.
     */
    public final class PointCentroidConverter extends MapFunction<T, Tuple2<Integer, T>> {

        private static final long serialVersionUID = 6696423823857438943L;

        @Override
        public Tuple2<Integer, T> map(T p) throws Exception {
            return new Tuple2<>(1, p);
        }
    }

    /**
     * Pick One Point from the input data as the initial centroid.
     */
    public final class PickOnePoint extends ReduceFunction<T> {

        private static final long serialVersionUID = -4173293469519514256L;

        @Override
        public T reduce(T point, T point2) throws Exception {
            return point;
        }
    }

    /**
     * Randomly pick a point based on the distance.
     */
    public final class PickWithDistance extends GroupReduceFunction<Tuple2<T, Double>, Tuple2<Integer, T>> {

        private static final long serialVersionUID = 2351745306551566932L;

        private double sum;

        private int maxId = 0;

        @Override
        public void open(Configuration parameters) throws Exception {
            Collection<Tuple2<Tuple2<T, Double>, Tuple2<Integer, T>>> sumWrapper;
            sumWrapper = getRuntimeContext().getBroadcastVariable("sumcrosscentroid");
            for (Tuple2<Tuple2<T, Double>, Tuple2<Integer, T>> c : sumWrapper) {
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
                    out.collect(new Tuple2<>(maxId + 1, d.f0));
                    return;
                }
            }
        }
    }

    /**
     * Add the new centroid to the centroid dataset.
     */
    public final class AddNewCenter extends GroupReduceFunction<Tuple2<Integer, T>, Tuple2<Integer, T>> {

        private Collection<Tuple2<Integer, T>> centroid;


        @Override
        public void open(Configuration parameters) throws Exception {
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


    /**
     * Determines the closest cluster center for a data point.
     */
    public final class SelectNearestCenter extends MapFunction<T, Tuple2<Integer, T>> {

        private static final long serialVersionUID = 3830298690315137146L;

        private GenericFunctions<T> function;

        private Collection<Tuple2<Integer, T>> centroids;

        public SelectNearestCenter(GenericFunctions<T> function) {
            this.function = function;
        }

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

                double distance = function.distance(centroid.f1, p);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.f0;
                }
            }

            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestCentroidId, p);
        }
    }

    /**
     * Determines the closest cluster distance for a data point.
     */
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

                double distance = function.distance(centroid.f1, p);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                }
            }
            return new Tuple2<>(p, minDistance * minDistance);
        }
    }

    /**
     * Appends a count variable to the tuple.
     */
    public final class CountAppender extends MapFunction<Tuple2<Integer, T>, Tuple3<Integer, T, Long>> {

        private static final long serialVersionUID = -3743500564243984677L;

        @Override
        public Tuple3<Integer, T, Long> map(Tuple2<Integer, T> t) {
            return new Tuple3<>(t.f0, t.f1, 1L);
        }
    }

    /**
     * Sums and counts point coordinates.
     */
    public final class CentroidAccumulator extends ReduceFunction<Tuple3<Integer, T, Long>> {


        private static final long serialVersionUID = 3668504702360578838L;

        private GenericFunctions<T> function;

        public CentroidAccumulator(GenericFunctions<T> function) {
            this.function = function;
        }

        @Override
        public Tuple3<Integer, T, Long> reduce(Tuple3<Integer, T, Long> val1, Tuple3<Integer, T, Long> val2) {
            return new Tuple3<>(val1.f0, function.add(val1.f1, val2.f1), val1.f2 + val2.f2);
        }
    }

    /**
     * Computes new centroid from coordinate sum and count of points.
     */
    public final class CentroidAverager extends MapFunction<Tuple3<Integer, T, Long>, Tuple2<Integer, T>> {


        private static final long serialVersionUID = -439825558772265542L;

        private GenericFunctions<T> function;

        public CentroidAverager(GenericFunctions<T> function) {
            this.function = function;
        }

        @Override
        public Tuple2<Integer, T> map(Tuple3<Integer, T, Long> value) {
            T tmp = function.div(value.f1, value.f2);
            return new Tuple2<>(value.f0, tmp);
        }
    }



}
