package de.tu_berlin.impro3.stratosphere.clustering.canopy;

import java.util.*;

import de.tu_berlin.impro3.core.Algorithm;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.functions.*;
import eu.stratosphere.api.java.tuple.*;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.util.Collector;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

/**
 * Canopy clustering implementation for Apache Flink 0.5-rc1.
 */
@SuppressWarnings("serial")
public class Canopy extends Algorithm {

    final String inputPath;

    final String outputPath;

    final float t1;

    final float t2;

    final int maxIterations;

    @SuppressWarnings("unused")
    public Canopy(Namespace ns) {
        this(ns.getString(Command.KEY_INPUT),
             ns.getString(Command.KEY_OUTPUT),
             ns.getFloat(Command.KEY_T1),
             ns.getFloat(Command.KEY_T2),
             ns.getInt(Command.KEY_ITERATIONS));
    }

    public Canopy(final String inputPath, final String outputPath, final float t1, final float t2, final int maxIterations) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.t1 = t1;
        this.t2 = t2;
        this.maxIterations = maxIterations;
    }

    // --------------------------------------------------------------------------------------------
    // ------------------------------ Algorithm Command -------------------------------------------
    // --------------------------------------------------------------------------------------------


    public static class Command extends Algorithm.Command<Canopy> {

        public final static String KEY_T1 = "algorithm.canopy.t1";

        public final static String KEY_T2 = "algorithm.canopy.t2";

        public final static String KEY_ITERATIONS = "algorithm.canopy.num.iterations";

        public Command() {
            super("canopy", "Canopy Clustering", Canopy.class);
        }

        @Override
        public void setup(Subparser parser) {
            //@formatter:off
            parser.addArgument("t1") // "-t1"
                    .type(Float.class)
                    .dest(KEY_T1)
                    .metavar("T1")
                    .help("Threshold T1");
            parser.addArgument("t2") // "-t2"
                    .type(Float.class)
                    .dest(KEY_T2)
                    .metavar("T2")
                    .help("Threshold T2");
            parser.addArgument("iterations") //  "-i", "--iterations"
                    .type(Integer.class)
                    .dest(KEY_ITERATIONS)
                    .metavar("NUM-ITERATIONS")
                    .help("Number of iterations");
            //@formatter:on

            super.setup(parser);
        }
    }

    // --------------------------------------------------------------------------------------------
    // ------------------------------ Data Structures ---------------------------------------------
    // --------------------------------------------------------------------------------------------

    /**
     * Represents a Tuple5(docId, isCenter, isInSomeT2, "canopyCenters", "words") Purpose: Reduces
     * the amount of generics usage in the doRun method.
     */
    public static class Document extends Tuple5<Integer, Boolean, Boolean, String, String> {

        @SuppressWarnings("unused")
        public Document() {
            // default constructor needed for instantiation during serialization
        }

        public Document(Integer docId, Boolean isCenter, Boolean isInSomeT2, String canopyCenters, String words) {
            super(docId, isCenter, isInSomeT2, canopyCenters, words);
        }
    }


    // --------------------------------------------------------------------------------------------
    // --------------------------------- Algorithm ------------------------------------------------
    // --------------------------------------------------------------------------------------------

    @Override
    public void run() throws Exception {
        doRun(inputPath, outputPath, t1, t2, maxIterations);
    }

    /**
     * runs the canopy clustering program with the given parameters
     * 
     * @param inputPath Input path
     * @param outputPath Output path
     * @param t1 Threshold T1 (Less similarity)
     * @param t2 Threshold T2 (More similarity)
     * @throws Exception
     */
    private static void doRun(String inputPath, String outputPath, float t1, float t2, int maxIterations) throws Exception {

        // setup environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.setFloat("t1", t1);
        conf.setFloat("t2", t2);

        FilterFunction<Document> unmarkedFilter = new FilterUnmarked();

        // read the dataset and transform it to docs
        DataSet<Document> docs = env.readTextFile(inputPath).flatMap(new MassageBOW()).groupBy(0).reduceGroup(new DocumentReducer());
        // loop
        IterativeDataSet<Document> loop = docs.iterate(maxIterations);
        DataSet<Document> unmarked = loop.filter(unmarkedFilter);
        DataSet<Document> centerX = unmarked.reduce(new PickFirst());
        DataSet<Document> iterationX = loop.map(new MapToCenter()).withParameters(conf).withBroadcastSet(centerX, "center");
        DataSet<Document> loopResult = loop.closeWith(iterationX, unmarked);
        // create canopies
        DataSet<Tuple1<String>> canopies = loopResult.flatMap(new CreateInverted()).groupBy(0).reduceGroup(new CreateCanopies());

        canopies.writeAsCsv(outputPath, "\n", " ", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // execute canopy
        env.execute("Canopy");
    }

    /**
     * Maps lines of a uci bag-of-words dataset to a Tuple2. Ignores the word frequencies because
     * the distance metric for canopy clustering should be cheap (jaccard).
     */
    public static class MassageBOW extends FlatMapFunction<String, Tuple2<Integer, String>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<Integer, String>> out) throws Exception {
            String[] splits = value.split(" ");
            if (splits.length < 2) {
                return;
            }
            out.collect(new Tuple2<>(Integer.valueOf(splits[0]), splits[1]));
        }
    }

    /**
     * Creates Documents: Tuple5(docId, isCenter, isInSomeT2, "canopyCenters", "words")
     */
    public static class DocumentReducer extends GroupReduceFunction<Tuple2<Integer, String>, Document> {

        @Override
        public void reduce(Iterator<Tuple2<Integer, String>> values, Collector<Document> out) throws Exception {
            Tuple2<Integer, String> first = values.next();
            Integer docId = first.f0;
            StringBuilder builder = new StringBuilder(first.f1);
            while (values.hasNext()) {
                builder.append("-").append(values.next().f1);
            }
            out.collect(new Document(docId, false, false, "", builder.toString()));
        }
    }

    /**
     * Filter all documents that are not in some T1 or T2 threshold of a canopy center.
     */
    public static class FilterUnmarked extends FilterFunction<Document> {

        @Override
        public boolean filter(Document value) throws Exception {
            return !value.f2 && value.f3.isEmpty();
        }
    }

    /**
     * Reduces a DataSet<Document> to a single value (limit 1).
     */
    public static class PickFirst extends ReduceFunction<Document> {

        @Override
        public Document reduce(Document v1, Document v2) throws Exception {
            return v1;
        }
    }

    /**
     * Marks each document with the values "isCenter", "isInSomeT2" and updates the "canopyIds".
     */
    @FunctionAnnotation.ConstantFieldsExcept("1,2,3")
    public static class MapToCenter extends MapFunction<Document, Document> {

        private Document center;

        private float t1;

        private float t2;

        @Override
        public Document map(Document value) throws Exception {
            if (center != null) {
                final float similarity = computeJaccard(value.f4, center.f4);
                final boolean isEqual = value.f0.equals(center.f0);
                value.f1 = isEqual;
                value.f2 = isEqual || similarity > t2;
                if (!value.f3.contains(center.f0.toString() + ";") && (similarity > t1 || isEqual)) {
                    value.f3 += center.f0.toString() + ";";
                }
            }
            return value;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            List<Object> list = (List<Object>) getRuntimeContext().getBroadcastVariable("center");
            if (!list.isEmpty()) {
                center = (Document) list.get(0);
                t1 = parameters.getFloat("t1", 0.15f);
                t2 = parameters.getFloat("t2", 0.3f);
            }
        }
    }

    /**
     * Creates new tuples by splitting the "canopyIds" of the documents.
     */
    public static class CreateInverted extends FlatMapFunction<Document, Tuple2<String, Integer>> {

        @Override
        public void flatMap(Document value, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String clusterID : value.f3.split(";")) {
                out.collect(new Tuple2<>(clusterID, value.f0));
            }
        }
    }

    /**
     * Creates a comma separated string of canopies.
     */
    public static class CreateCanopies extends GroupReduceFunction<Tuple2<String, Integer>, Tuple1<String>> {

        @Override
        public void reduce(Iterator<Tuple2<String, Integer>> values, Collector<Tuple1<String>> out) throws Exception {
            StringBuilder builder = new StringBuilder();
            while (values.hasNext()) {
                builder.append(values.next().f1);
                if (values.hasNext()) {
                    builder.append(",");
                }
            }
            out.collect(new Tuple1<>(builder.toString()));
        }
    }


    // --------------------------------------------------------------------------------------------
    // ------------------------------ Utility methods ---------------------------------------------
    // --------------------------------------------------------------------------------------------

    /**
     * Computes the Jaccard coefficient from two strings. Ideally from two lists but that cant be
     * done in flink right now.
     * 
     * @return the similarity of two documents.
     */
    public static float computeJaccard(String first, String second) {
        float joint = 0;
        List<String> firstList = Arrays.asList(first.split("-"));
        List<String> secondList = Arrays.asList(second.split("-"));

        for (String s : firstList) {
            if (secondList.contains(s))
                joint++;
        }

        return (joint / ((float) firstList.size() + (float) secondList.size() - joint));
    }
}
