package de.tu_berlin.impro3.stratosphere.clustering.hac;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.StringTokenizer;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import de.tu_berlin.impro3.core.Algorithm;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.util.Collector;

@SuppressWarnings("serial")
public class HAC extends Algorithm {

    private final String inputPath;

    private final String outputPath;

    /** Iterations count (cluster = #documents - iterations) */
    private final int iterations;

    /** Used linkage method */
    private final LinkageMode linkage;

    public static enum LinkageMode {
        SINGLE,
        COMPLETE
    }

    public static class Command extends Algorithm.Command<HAC> {

        public static final String KEY_ITERATIONS = "algorithm.hac.iterations";

        public static final String KEY_LINKAGE = "algorithm.hac.linkage";

        public Command() {
            super("hac", "Hierarchical clustering", HAC.class);
        }

        @Override
        public void setup(Subparser parser) {
            //@formatter:off
            parser.addArgument("iterations")
                .type(Integer.class)
                .dest(KEY_ITERATIONS)
                .metavar("N")
                .help("Number of iterations (#documents - iterations = #cluster)");
            parser.addArgument("linkage")
                .type(String.class)
                .dest(KEY_LINKAGE)
                .metavar("L")
                .setDefault(LinkageMode.SINGLE)
                .help("Linkage mode (SINGLE or COMPLETE)");
            //@formatter:on

            super.setup(parser);
        }
    }

    public HAC(Namespace ns) {
        this(ns.getString(Command.KEY_INPUT),
             ns.getString(Command.KEY_OUTPUT),
             ns.getInt(Command.KEY_ITERATIONS),
             LinkageMode.valueOf(ns.getString(Command.KEY_LINKAGE)));
    }

    public HAC(String inputPath, String outputPath, int iterations, LinkageMode linkage) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.iterations = iterations;
        this.linkage = linkage;
    }

    /**
     * This class represents one DataPoint from the "Bag of Words" input file. Basically, one
     * DataPoint represents one line of the input file. Each DataPoint has a document ID, a term ID
     * and the count of the term in the document.
     */
    public static class DataPoint extends Tuple3<Integer, Integer, Integer> {

        public DataPoint() {}

        public DataPoint(Integer docid, Integer termid, Integer count) {
            this.f0 = docid;
            this.f1 = termid;
            this.f2 = count;
        }

        public Integer getDocID() {
            return this.f0;
        }

        public Integer getCount() {
            return this.f2;
        }
    }

    /**
     * This class represents a document from the "Bag of Words". There are two fields, a document ID
     * that identifies the document and a cluster ID that identifies the cluster the document
     * currently resides in. In the beginning, the cluster ID is always equal to the document ID.
     */
    public static class Document extends Tuple2<Integer, Integer> {

        public Document() {}

        public Document(Integer docid) {
            this.f0 = docid;
            this.f1 = docid;
        }

        public Integer getDocID() {
            return this.f1;
        }

        public void setClusterID(Integer id) {
            this.f0 = id;
        }

        public Integer getClusterID() {
            return this.f0;
        }
    }

    /**
     * A ClusterPair contains two cluster IDs and the similarity of those clusters.
     */
    public static class ClusterPair extends Tuple3<Double, Integer, Integer> implements Cloneable {

        public ClusterPair() {}

        @Override
        public ClusterPair clone() {
            return new ClusterPair(new Double(this.f0.doubleValue()), new Integer(this.f1.intValue()), new Integer(this.f2.intValue()));
        }

        public ClusterPair(Double sim, Integer cid0, Integer cid1) {
            this.f0 = sim;
            this.f1 = cid0;
            this.f2 = cid1;
        }

        public Double getSimilarity() {
            return this.f0;
        }

        public void setSimilarity(Double distance) {
            this.f0 = distance;
        }

        public Integer getCluster1() {
            return this.f1;
        }

        public Integer getCluster2() {
            return this.f2;
        }

        public void setCluster1(Integer clusterID) {
            this.f1 = clusterID;
        }

        public void setCluster2(Integer clusterID) {
            this.f2 = clusterID;
        }
    }

    /**
     * This reducer finds the ClusterPair with the lowest similarity value. This is useful for the
     * COMPLETE_LINKAGE linkage option.
     */
    public static class MinSimilarityReducer extends ReduceFunction<ClusterPair> {

        @Override
        public ClusterPair reduce(ClusterPair value1, ClusterPair value2) throws Exception {
            if (value1.getSimilarity().doubleValue() < value2.getSimilarity().doubleValue()) {
                return value1;
            }
            return value2;
        }
    }

    /**
     * This reducer finds the ClusterPair with the highest similarity value. This is useful for the
     * SINGLE_LINKAGE linkage option.
     */
    public static class MaxSimilarityReducer extends ReduceFunction<ClusterPair> {

        @Override
        public ClusterPair reduce(ClusterPair value1, ClusterPair value2) throws Exception {
            if (value1.getSimilarity().doubleValue() > value2.getSimilarity().doubleValue()) {
                return value1;
            }
            return value2;
        }
    }

    /**
     * This FlatMap gets the ClusterPair with the minimal or maximal similarity (according to the
     * linkage option) as a broadcast variable. The Cluster with the lower ClusterID of this
     * broadcast ClusterPair needs to be replaced by the higher cluster. Therefore, this flatmap
     * searches for ClusterPairs where cluster1 or cluster2 are equal to cluster1 of the broadcast
     * variable. If such a pair is found, the clusterID is replaced.
     */
    public static class ClusterPairUpdater extends FlatMapFunction<ClusterPair, ClusterPair> {

        private ClusterPair mergedPair;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Collection<ClusterPair> min = this.getRuntimeContext().getBroadcastVariable("mergedPair");
            Iterator<ClusterPair> it = min.iterator();
            if (it.hasNext()) {
                this.mergedPair = min.iterator().next();
            } else {
                this.mergedPair = null;
            }
        }

        @Override
        public void flatMap(ClusterPair value, Collector<ClusterPair> out) throws Exception {
            if (mergedPair == null) {
                out.collect(value);
                return;
            }

            // if broadcast clusterID is found, it needs to be replaced(merged)
            if (value.getCluster1().equals(mergedPair.getCluster1())) {
                value.setCluster1(mergedPair.getCluster2());
            } else if (value.getCluster2().equals(mergedPair.getCluster1())) {
                value.setCluster2(mergedPair.getCluster2());
            }

            // Cluster with the smaller ID should always be in first place.
            if (value.getCluster1().intValue() < value.getCluster2().intValue()) {
                out.collect(value);
            } else if (value.getCluster1().intValue() > value.getCluster2().intValue()) {
                // swap cluster ID's:
                Integer tmp = value.getCluster2();
                value.setCluster2(value.getCluster1());
                value.setCluster1(tmp);
                out.collect(value);
            }
        }
    }

    /**
     * This class represents the change that the clusters have gone through. This is necessary
     * because it is not yet possible to output data after every iteration. This way, we can keep
     * track of the Clusters in each iteration of the algorithm.
     */
    public static class ClusterHistory extends Tuple2<Integer, String> {

        public ClusterHistory() {
            f0 = 0;
            f1 = "";
        }

        public ClusterHistory(Integer oldid, Integer newid) {
            f0 = 0;
            f1 = "[" + oldid + "; " + newid + "]";
        }

        public void add(ClusterHistory newend) {
            f1 += newend.f1;
        }

        @Override
        public String toString() {
            return f1;
        }
    }

    /**
     * This Reducer creates all the Documents and inserts them into a initial cluster.
     */
    public static class ClusterAssignReducer extends GroupReduceFunction<DataPoint, Document> {

        @Override
        public void reduce(Iterator<DataPoint> values, Collector<Document> out) throws Exception {
            Document doc = new Document(values.next().getDocID());

            System.out.println("Inserting document " + doc.getDocID() + " into cluster " + doc.getClusterID());

            out.collect(doc);
        }
    }

    /**
     * Joins two ClusterHistories together.
     */
    public static class HistoryJoiner extends JoinFunction<ClusterHistory, ClusterHistory, ClusterHistory> {

        @Override
        public ClusterHistory join(ClusterHistory clusterHistory, ClusterHistory clusterHistory2) throws Exception {
            clusterHistory.add(clusterHistory2);
            return clusterHistory;
        }
    }

    /**
     * Outputs the product of two terms.
     */
    public static class SimilarityMap extends FlatMapFunction<Tuple2<DataPoint, DataPoint>, ClusterPair> {

        @Override
        public void flatMap(Tuple2<DataPoint, DataPoint> in, Collector<ClusterPair> out) throws Exception {
            if (in.f0.getDocID() < in.f1.getDocID()) {
                out.collect(new ClusterPair((double) in.f0.getCount() * in.f1.getCount(), in.f0.getDocID(), in.f1.getDocID()));
            }
        }
    }

    public static class HistoryUpdater extends MapFunction<ClusterPair, ClusterHistory> {

        @Override
        public ClusterHistory map(ClusterPair clusterPair) throws Exception {
            return new ClusterHistory(clusterPair.getCluster1(), clusterPair.getCluster2());
        }
    }

    public static void clusterDocuments(ExecutionEnvironment env,
                                        LinkageMode linkage,
                                        DataSet<ClusterPair> similarities,
                                        String outputfile,
                                        int iterations_max) {

        // Add empty history item to cluster merge history list
        ArrayList<ClusterHistory> startList = new ArrayList<>();
        startList.add(new ClusterHistory());
        DataSet<ClusterHistory> history = env.fromCollection(startList);

        // Differentiate between the linkage modes. In SINGLE link mode, the
        // highest similarity symbolizes the shorted "distance" between two
        // clusters. COMPLETE link needs the minimum similarity.
        ReduceFunction<ClusterPair> linkageReducer = null;
        if (linkage == LinkageMode.COMPLETE) {
            linkageReducer = new MinSimilarityReducer();
        } else if (linkage == LinkageMode.SINGLE) {
            linkageReducer = new MaxSimilarityReducer();
        }

        // Start of the iteration.
        DeltaIteration<ClusterHistory, ClusterPair> iteration = history.iterateDelta(similarities, iterations_max, 0);

        // Find merge candidate
        DataSet<ClusterPair> linkageSet = iteration.getWorkset().reduce(linkageReducer).name("LinkageReducer");

        // Update the cluster merge history
        DataSet<ClusterHistory> update = linkageSet.map(new HistoryUpdater()).name("HistoryUpdater");

        // Join with result history
        DataSet<ClusterHistory> delta = iteration.getSolutionSet().join(update).where(0).equalTo(0).with(new HistoryJoiner()).name("HistoryJoiner");

        // Merge cluster in similarity matrix
        DataSet<ClusterPair> feedback =
                iteration.getWorkset().flatMap(new ClusterPairUpdater()).name("ClusterPairUpdater").withBroadcastSet(linkageSet, "mergedPair");

        DataSet<ClusterHistory> result = iteration.closeWith(delta, feedback);

        if (!outputfile.isEmpty())
            result.writeAsText(outputfile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        else
            result.print();
    }


    /**
     * Initial points are read from the file. Layout: docID termID termCount (separated by space).
     */
    public static DataSet<DataPoint> readPoints(final ExecutionEnvironment env, final String inputPath) {
        // DataSet<DataPoint> points =
        // env.readCsvFile(filePath).fieldDelimiter(' ').tupleType(DataPoint.class);
        return env.readTextFile(inputPath).flatMap(new FlatMapFunction<String, DataPoint>() {

            @Override
            public void flatMap(String value, Collector<DataPoint> out) throws Exception {
                StringTokenizer tokenizer = new StringTokenizer(value, " ");

                // ignore first status lines
                if (tokenizer.countTokens() < 3)
                    return;

                ArrayList<Integer> tokens = new ArrayList<Integer>();
                while (tokenizer.hasMoreTokens()) {
                    tokens.add(Integer.valueOf((String) tokenizer.nextElement()));
                }

                out.collect(new DataPoint(tokens.get(0), tokens.get(1), tokens.get(2)));
            }
        }).name("DataPointsInit");
    }

    /**
     * Join the Points on their TermID. Then multiply the counts of the two terms and save them as
     * similarity in a ClusterPair, together with the two document IDs. By grouping on the two
     * document ids, we get the multiplied count of all terms that appear in both documents. By
     * computing the sum of the multiplied counts we get our similarity measure. This way, only
     * terms that are present in both documents are considered and terms that appear often in both
     * gain additional weight.
     */
    public static DataSet<ClusterPair> calculateSimilarities(DataSet<DataPoint> points) {
        return points.join(points)
                     .where(1)
                     .equalTo(1)
                     .name("TermIDJoin")
                     .flatMap(new SimilarityMap())
                     .name("SimilarityMap")
                     .groupBy(1, 2)
                     .aggregate(Aggregations.SUM, 0)
                     .name("SimilarityAggregration");
    }

    @Override
    public void run() throws Exception {
        doRun(this.inputPath, this.outputPath, this.iterations, this.linkage);
    }

    public static void doRun(final String inputPath, final String outputPath, final int iterations, final LinkageMode linkage) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<DataPoint> points = readPoints(env, inputPath);
        DataSet<Document> documents = points.groupBy(0).reduceGroup(new ClusterAssignReducer());
        DataSet<ClusterPair> similarities = calculateSimilarities(points);

        clusterDocuments(env, linkage, similarities, outputPath, iterations);

        env.execute();
        // System.out.println(env.getExecutionPlan());
    }
}
