package de.tu_berlin.impro3.spark.clustering.kmeanspp;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import de.tu_berlin.impro3.core.Algorithm;


public class KMeansppBagOfWords extends Algorithm {

    public final static String CENTERS_OUTPUT_PATH = File.separator + "centers";

    public final static String CLUSTER_OUTPUT_PATH = File.separator + "clusters";

    private final JavaSparkContext sc;

    private final String dataPath;

    private final String outputPath;

    private final int k;

    private final int numIterations;

    @SuppressWarnings("unused")
    public KMeansppBagOfWords(Namespace ns) {
        this(new JavaSparkContext(new SparkConf().setAppName("kmeanspp")),
             ns.getInt(Command.KEY_K),
             ns.getInt(Command.KEY_ITERATIONS),
             ns.getString(Command.KEY_INPUT),
             ns.getString(Command.KEY_OUTPUT));
    }

    public KMeansppBagOfWords(final JavaSparkContext sc, final int k, final int numIterations, final String dataPath, final String outputPath) {
        this.sc = sc;
        this.dataPath = dataPath;
        this.outputPath = outputPath;
        this.k = k;
        this.numIterations = numIterations;
        // set conf parameters
        this.sc.getConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        this.sc.getConf().set("spark.kryo.registrator", "de.tu_berlin.impro3.spark.clustering.kmeanspp.MyKryoRegistrator");
    }


    public static class Command extends Algorithm.Command<KMeansppBagOfWords> {

        public final static String KEY_K = "algorithm.kmeanspp.k";

        public final static String KEY_ITERATIONS = "algorithm.kmeanspp.iterations";

        public Command() {
            super("kmeanspp", "K-Means++ (BOW data model)", KMeansppBagOfWords.class);
        }

        @Override
        public void setup(Subparser parser) {
            //@formatter:off
            parser.addArgument("k")
                  .type(Integer.class)
                  .dest(KEY_K)
                  .metavar("K")
                  .help("Number of clusters to be formed");
            parser.addArgument("iterations")
                  .type(Integer.class)
                  .dest(KEY_ITERATIONS)
                  .metavar("ITERATIONS")
                  .help("Number of iterations to be performed before the standard K-Means terminates");
            //@formatter:on     

            super.setup(parser);
        }
    }

    @Override
    public void run() {
        JavaRDD<String> dataLines = sc.textFile(dataPath);

        JavaRDD<DocPoint> points =
                dataLines.flatMapToPair(new ParseSingleLine()).reduceByKey(new AggregateWordsInDoc()).map(new ConvertToDoc()).cache();

        // get the first center
        JavaPairRDD<Integer, DocPoint> kppCenters = sc.parallelize(points.takeSample(false, 1)).mapToPair(new PointToCenter(1));

        for (int i = 2; i <= k; ++i) {
            Broadcast<List<Tuple2<Integer, DocPoint>>> brCenters = sc.broadcast(kppCenters.collect());
            Accumulator<Double> distanceSum = sc.accumulator(0f);

            Tuple2<DocPoint, Double> newCenter = points
            // calculate the distance from each point to its closest center
            .map(new ComputeClosestDistance(brCenters, distanceSum))
                                                       // pick a new center according to its weight
                                                       // here we first filter a point set, and then
                                                       // pick the one with largest distance
                                                       .filter(new SamplePossibleCenters(distanceSum.value()))
                                                       .reduce(new PickNewCenter());

            // add new center to collection
            List<Tuple2<Integer, DocPoint>> centerWrapper = new ArrayList<>(1);
            centerWrapper.add(new Tuple2<>(i, newCenter._1()));
            kppCenters = kppCenters.union(sc.parallelizePairs(centerWrapper));

            brCenters.unpersist();
        }

        // ================================ Standard KMeans =============================

        JavaPairRDD<Integer, DocPoint> kCenters = kppCenters.cache();

        for (int i = 0; i < numIterations; ++i) {
            Broadcast<List<Tuple2<Integer, DocPoint>>> brCenters = sc.broadcast(kCenters.collect());

            kCenters = points
            // assign each point to its closest center, while appending a counter value for
            // calculating mean later
            .mapToPair(new AssignToClusterWithCounter(brCenters))
                             // aggregate points in each cluster
                             .reduceByKey(new InClusterAggregate())
                             // calculate the mean( the new center ) of each cluster
                             .mapToPair(new ComputeMean());
            brCenters.unpersist();
        }

        Broadcast<List<Tuple2<Integer, DocPoint>>> finalCenters = sc.broadcast(kCenters.collect());
        JavaPairRDD<Integer, DocPoint> finalClusters = points.mapToPair(new AssignToCluster(finalCenters));

        kCenters.saveAsTextFile(outputPath + CENTERS_OUTPUT_PATH);
        finalClusters.saveAsTextFile(outputPath + CLUSTER_OUTPUT_PATH);
    }

    /**
     * Map function for parsing a single line of the input file to a Document A single line
     * represents one dimension of the Document
     * 
     */
    public static final class ParseSingleLine implements PairFlatMapFunction<String, Integer, DocPoint> {

        private static final long serialVersionUID = -2868524609959061072L;

        @Override
        public Iterable<Tuple2<Integer, DocPoint>> call(String line) throws Exception {
            String[] fields = line.split(" ");

            ArrayList<Tuple2<Integer, DocPoint>> result = new ArrayList<>(1);

            if (fields.length == 3) {
                int docId = Integer.parseInt(fields[0]);
                DocPoint point = new DocPoint(docId);
                point.words.put(Integer.parseInt(fields[1]), Double.parseDouble(fields[2]));

                result.add(new Tuple2<>(docId, point));
            }
            return result;
        }
    }

    /**
     * Collect values of dimensions for each Document to get a 'Vector'
     * 
     */
    public static final class AggregateWordsInDoc implements Function2<DocPoint, DocPoint, DocPoint> {

        private static final long serialVersionUID = -7851383375104083219L;

        @Override
        public DocPoint call(DocPoint v1, DocPoint v2) throws Exception {
            v1.words.putAll(v2.words);
            return v1;
        }
    }

    /**
     * Map the key-value pair to a Document
     * 
     */
    public static final class ConvertToDoc implements Function<Tuple2<Integer, DocPoint>, DocPoint> {

        private static final long serialVersionUID = -7851383375104083219L;

        @Override
        public DocPoint call(Tuple2<Integer, DocPoint> v1) throws Exception {
            return v1._2();
        }
    }

    /**
     * Convert a Document Point to a center by assigning a center ID to it
     * 
     */
    public static final class PointToCenter implements PairFunction<DocPoint, Integer, DocPoint> {

        private static final long serialVersionUID = 8478673526611504467L;

        private int id;

        public PointToCenter(int id) {
            this.id = id;
        }

        @Override
        public Tuple2<Integer, DocPoint> call(DocPoint t) throws Exception {
            return new Tuple2<>(id, t);
        }
    }

    /**
     * Sample the next possible centers This function will return a set of possible candidates
     * rather than just one The probability for each point to become a candidate is proportional to
     * it's distance weight The size of the returned set is undetermined
     */
    public static final class SamplePossibleCenters implements Function<Tuple2<DocPoint, Double>, Boolean> {

        private static final long serialVersionUID = 3000581468716733391L;

        private double sum;

        private Random random;

        public SamplePossibleCenters(double sum) {
            this.sum = sum;
            this.random = new Random();
        }

        @Override
        public Boolean call(Tuple2<DocPoint, Double> v1) throws Exception {
            return (random.nextDouble() * sum < v1._2());
        }
    }

    /**
     * Pick the next center from the set of center candidates
     * 
     */
    public static final class PickNewCenter implements Function2<Tuple2<DocPoint, Double>, Tuple2<DocPoint, Double>, Tuple2<DocPoint, Double>> {

        private static final long serialVersionUID = -6910924787833425750L;

        @Override
        public Tuple2<DocPoint, Double> call(Tuple2<DocPoint, Double> v1, Tuple2<DocPoint, Double> v2) throws Exception {
            return v1._2() < v2._2() ? v2 : v1;
        }
    }

    /**
     * Assign each point to its closest center
     * 
     */
    public static final class AssignToCluster implements PairFunction<DocPoint, Integer, DocPoint> {

        private static final long serialVersionUID = -2507871417621958863L;

        Broadcast<List<Tuple2<Integer, DocPoint>>> brCenters;

        public AssignToCluster(Broadcast<List<Tuple2<Integer, DocPoint>>> brCenters) {
            this.brCenters = brCenters;
        }

        @Override
        public Tuple2<Integer, DocPoint> call(DocPoint v1) throws Exception {
            double minDistance = Double.MAX_VALUE;
            int centerId = 0;

            for (Tuple2<Integer, DocPoint> c : brCenters.getValue()) {
                double d = v1.distanceSquare(c._2());
                if (minDistance > d) {
                    minDistance = d;
                    centerId = c._1();
                }
            }
            return new Tuple2<>(centerId, v1);
        }
    }

    /**
     * Compute the squared distance from each point to its closest center
     * 
     */
    public static final class ComputeClosestDistance implements Function<DocPoint, Tuple2<DocPoint, Double>> {

        private static final long serialVersionUID = 4836454512753985468L;

        Broadcast<List<Tuple2<Integer, DocPoint>>> brCenters;

        Accumulator<Double> distanceSum;

        public ComputeClosestDistance(Broadcast<List<Tuple2<Integer, DocPoint>>> brCenters, Accumulator<Double> distanceSum) {
            this.brCenters = brCenters;
            this.distanceSum = distanceSum;
        }

        @Override
        public Tuple2<DocPoint, Double> call(DocPoint v1) throws Exception {
            double minDistance = Double.MAX_VALUE;
            for (Tuple2<Integer, DocPoint> c : brCenters.getValue()) {
                double d = v1.distanceSquare(c._2());
                if (minDistance > d) {
                    minDistance = d;
                }
            }
            distanceSum.add(minDistance);
            return new Tuple2<>(v1, minDistance);
        }
    }

    /**
     * Assign each point to its closest center, while appending a counter with value 1 for further
     * calculation
     * 
     */
    public static final class AssignToClusterWithCounter implements PairFunction<DocPoint, Integer, Tuple2<DocPoint, Long>> {

        private static final long serialVersionUID = -1827992899820378823L;

        Broadcast<List<Tuple2<Integer, DocPoint>>> brCenters;

        public AssignToClusterWithCounter(Broadcast<List<Tuple2<Integer, DocPoint>>> brCenters) {
            this.brCenters = brCenters;
        }

        @Override
        public Tuple2<Integer, Tuple2<DocPoint, Long>> call(DocPoint v1) throws Exception {
            double minDistance = Double.MAX_VALUE;
            int centerId = 0;

            for (Tuple2<Integer, DocPoint> c : brCenters.getValue()) {
                double d = v1.distanceSquare(c._2());
                if (minDistance > d) {
                    minDistance = d;
                    centerId = c._1();
                }
            }
            return new Tuple2<>(centerId, new Tuple2<>(v1, 1L));
        }
    }

    /**
     * Aggregate(sum) all the points in each cluster for calculating mean
     * 
     */
    public static final class InClusterAggregate implements Function2<Tuple2<DocPoint, Long>, Tuple2<DocPoint, Long>, Tuple2<DocPoint, Long>> {

        private static final long serialVersionUID = -5957650629994933259L;

        @Override
        public Tuple2<DocPoint, Long> call(Tuple2<DocPoint, Long> v1, Tuple2<DocPoint, Long> v2) throws Exception {
            if (v1._2() > 1) {
                v1._1().add(v2._1());
                return new Tuple2<>(v1._1(), v1._2() + v2._2());
            } else if (v2._2() > 1) {
                v2._1().add(v1._1());
                return new Tuple2<>(v2._1(), v1._2() + v2._2());
            } else {
                // only create a new object if necessary, good for saving memory
                return new Tuple2<>(DocPoint.add(v1._1(), v2._1()), v1._2() + v2._2());
            }
        }
    }

    /**
     * Calculate the mean(new center) of the cluster ( sum of points / number of points )
     * 
     */
    public static final class ComputeMean implements PairFunction<Tuple2<Integer, Tuple2<DocPoint, Long>>, Integer, DocPoint> {

        private static final long serialVersionUID = 3618969674267201301L;

        @Override
        public Tuple2<Integer, DocPoint> call(Tuple2<Integer, Tuple2<DocPoint, Long>> t) throws Exception {
            t._2()._1().div(t._2()._2());
            return new Tuple2<>(t._1(), t._2()._1());
        }
    }

}
