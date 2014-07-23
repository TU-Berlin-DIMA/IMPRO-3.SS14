package de.tu_berlin.impro3.stratosphere.clustering.kmeanspp;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import de.tu_berlin.impro3.core.Algorithm;
import de.tu_berlin.impro3.stratosphere.clustering.kmeanspp.util.GenericFunctions;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.util.Collector;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

/**
 * KMeans++ usage for bag of words
 */
public class KMeansppBagOfWords extends Algorithm {

    private String dataPath;

    private String outputPath;

    private int k;

    private int numIterations;

    @SuppressWarnings("unused")
    public KMeansppBagOfWords(Namespace ns) {
        this(ns.getInt(Command.KEY_K), ns.getInt(Command.KEY_ITERATIONS), ns.getString(Command.KEY_INPUT), ns.getString(Command.KEY_OUTPUT));
    }

    public KMeansppBagOfWords(final int k, final int numIterations, final String dataPath, final String outputPath) {
        this.dataPath = dataPath;
        this.outputPath = outputPath;
        this.k = k;
        this.numIterations = numIterations;
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

    // --------------------------------------------------------------------------------------------
    // ------------------------------ Data Structures ---------------------------------------------
    // --------------------------------------------------------------------------------------------


    /**
     * User defined class for bag of word.
     */
    public static class Document implements Serializable {

        private static final long serialVersionUID = -8646398807053061675L;

        public Map<String, Double> wordFreq = new HashMap<>();

        public Integer id;

        public Document() {
            id = -1;
        }

        public Document(Integer id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return Integer.toString(id);
        }
    }

    /**
     * Convert the input data into User defined type - Document.
     */
    public static final class RecordToDocConverter extends GroupReduceFunction<Tuple3<Integer, Integer, Double>, Document> {

        private static final long serialVersionUID = -8476366121490468956L;

        @Override
        public void reduce(Iterator<Tuple3<Integer, Integer, Double>> values, Collector<Document> out) throws Exception {
            if (values.hasNext()) {
                Tuple3<Integer, Integer, Double> elem = values.next();
                Document doc = new Document(elem.f0);
                doc.wordFreq.put(elem.f1.toString(), elem.f2);

                while (values.hasNext()) {
                    elem = values.next();
                    doc.wordFreq.put(elem.f1.toString(), elem.f2);
                }
                out.collect(doc);
            }
        }
    }

    public static final class FilterFirst3Lines extends FlatMapFunction<String, Tuple3<Integer, Integer, Double>> {

        @Override
        public void flatMap(String value, Collector<Tuple3<Integer, Integer, Double>> out) throws Exception {
            String[] splits = value.split(" ");
            if (splits.length > 2) {
                out.collect(new Tuple3<>(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]), Double.parseDouble(splits[2])));
            }
        }
    }

    /**
     * User defined function, including input data, average function and distance measure.
     */
    public static class MyFunctions implements GenericFunctions<Document> {

        private static final long serialVersionUID = 5510454279473390773L;

        private String pointsPath;

        public MyFunctions(String filePath) {
            this.pointsPath = filePath;
        }

        @Override
        public DataSet<Document> getDataSet(ExecutionEnvironment env) {
            return env.readTextFile(pointsPath).flatMap(new FilterFirst3Lines()).groupBy(0).reduceGroup(new RecordToDocConverter());
        }

        @Override
        public Document add(Document in1, Document in2) {
            for (Map.Entry<String, Double> itr : in2.wordFreq.entrySet()) {
                Double v1 = in1.wordFreq.get(itr.getKey());
                if (v1 == null) {
                    in1.wordFreq.put(itr.getKey(), itr.getValue());
                } else {
                    in1.wordFreq.put(itr.getKey(), itr.getValue() + v1);
                }
            }
            return in1;
        }

        @Override
        public Document div(Document in1, long val) {
            Document out = new Document(in1.id);
            for (Map.Entry<String, Double> itr : in1.wordFreq.entrySet()) {
                out.wordFreq.put(itr.getKey(), itr.getValue() / val);
            }
            return out;
        }

        @Override
        public double distance(Document in1, Document in2) {
            double sum = 0;
            Set<String> added = new HashSet<>();

            for (Map.Entry<String, Double> itr : in2.wordFreq.entrySet()) {
                Double v1 = in1.wordFreq.get(itr.getKey());
                double v2 = itr.getValue();
                if (v1 == null) {
                    sum += v2 * v2;
                } else {
                    sum += (v2 - v1) * (v2 - v1);
                    added.add(itr.getKey());
                }
            }

            for (Map.Entry<String, Double> itr : in1.wordFreq.entrySet()) {
                if (!added.contains(itr.getKey())) {
                    sum += itr.getValue() * itr.getValue();
                }
            }
            return Math.sqrt(sum);
        }

    }

    @Override
    public void run() throws Exception {
        KMeansppGeneric<Document> kmp = new KMeansppGeneric<>(this.outputPath, this.k, this.numIterations);
        kmp.run(new MyFunctions(this.dataPath));
    }
}
