/*
 * (C) Copyright TU-Berlin DIMA and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     - oresti
 *     - seekermrm
 */

package de.tu_berlin.impro3.stratosphere.clustering.canopy;

import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.functions.*;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Canopy clustering implementation for Apache Flink 0.5-rc1.
 */
@SuppressWarnings("serial")
public class Canopy implements ProgramDescription {

    /**
     * The Algorithm will converge before this.
     */
    private static final int maxIterations = 9999999;

    /**
     * Write as CSV?
     */
    private static final boolean fileOutput = true;

    /**
     * Main Function
     *
     * @param args [maxTasks] [input] [output] [T1] [T2]
     */
    public static void main(String args[]) throws Exception {
        Canopy c = new Canopy();

        if (args.length != 5) {
            System.err.println(c.getDescription());
            return;
        }
        Integer maxTasks = Integer.parseInt(args[0]);
        String input = args[1];
        String output = args[2];

        // setup environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.setString("t1", args[3]);//Doubles don't work in flink
        conf.setString("t2", args[4]);

        FilterFunction<Document> unmarkedFilter = new FilterUnmarked();

        // read the dataset and transform it to docs
        DataSet<Document> docs = env.readTextFile(input)
                .flatMap(new MassageBOW())
                .groupBy(0).reduceGroup(new DocumentReducer());
        // loop
        IterativeDataSet<Document> loop = docs
                .iterate(maxIterations);
        DataSet<Document> unmarked = loop
                .filter(unmarkedFilter);
        DataSet<Document> centerX = unmarked
                .reduce(new PickFirst());
        DataSet<Document> iterationX = loop
                .map(new MapToCenter()).withParameters(conf).withBroadcastSet(centerX, "center");
        DataSet<Document> loopResult = loop.closeWith(iterationX, unmarked);
        // create canopies
        DataSet<Tuple1<String>> canopies = loopResult
                .flatMap(new CreateInverted())
                .groupBy(0).reduceGroup(new CreateCanopies());

        if (fileOutput) {
            canopies.writeAsCsv(output, "\n", " ", FileSystem.WriteMode.OVERWRITE);
        } else {
            canopies.print();
        }

        // execute canopy
        env.setDegreeOfParallelism(maxTasks);
        env.execute("Canopy");
    }

    @Override
    public String getDescription() {
        return "Parameters: [maxTasks] [input] [output] [T1] [T2]";
    }


    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * Maps lines of a uci bag-of-words dataset to a Tuple2.
     * Ignores the word frequencies because the distance metric for canopy clustering should be cheap (jaccard).
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

    // creates: docId, isCenter, isInSomeT2, "canopyCenters", "words"
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
        private double t1;
        private double t2;

        @Override
        public Document map(Document value) throws Exception {
            if (center != null) {
                final double similarity = computeJaccard(value.f4, center.f4);
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
                t1 = Double.valueOf(parameters.getString("t1", "0.15"));
                t2 = Double.valueOf(parameters.getString("t2", "0.3"));
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


    // *************************************************************************
    //     HELPERS
    // *************************************************************************

    /**
     * Stupid class to get rid of the generics spam of Tuples.
     */
    public static class Document extends Tuple5<Integer, Boolean, Boolean, String, String> {
        @SuppressWarnings("unused")
        public Document() {
        }

        public Document(Integer docId, Boolean isCenter, Boolean isInSomeT2, String canopyCenters, String words) {
            super(docId, isCenter, isInSomeT2, canopyCenters, words);
        }
    }

    /**
     * Computes the Jaccard coefficient from two strings.
     * Ideally from two lists but that cant be done in flink right now.
     *
     * @return the similarity of two documents.
     */
    public static double computeJaccard(String first, String second) {
        double joint = 0;
        List<String> firstList = Arrays.asList(first.split("-"));
        List<String> secondList = Arrays.asList(second.split("-"));

        for (String s : firstList) {
            if (secondList.contains(s))
                joint++;
        }

        return (joint / ((double) firstList.size() + (double) secondList.size() - joint));
    }
}