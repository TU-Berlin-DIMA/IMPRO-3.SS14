package de.tu_berlin.impro3.stratosphere.clustering.hac;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.*;
import eu.stratosphere.api.java.tuple.Tuple2;

import java.util.*;

@SuppressWarnings("serial")
public class OutputFormatter {
	public static final String DELIMITER_1 = "]";
    public static final String DELIMITER_2 = ";";
    public static String filepath;

    public static void formatAndWriteString(String inputString, String outputPath) {
		filepath = outputPath;
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        HashSet<Integer> docIds = new HashSet<Integer>();

        String[] tuplesWithBrackets = inputString.split(DELIMITER_1);
        List<Tuple2<Integer, Integer>> mergesOrder = new ArrayList<Tuple2<Integer, Integer>>();
        for(String dirtyTuple : tuplesWithBrackets) {
            String[] strings = dirtyTuple.substring(1).split(DELIMITER_2);

            int val1 = Integer.parseInt(strings[0].trim());
            int val2 = Integer.parseInt(strings[1].trim());

            docIds.add(val1);
            docIds.add(val2);

            mergesOrder.add(new Tuple2<Integer, Integer>(val1,val2));
        }

        List<Tuple2<Integer, Integer>> clusterDocPairs = new ArrayList<Tuple2<Integer, Integer>>();
        for(Integer docId : docIds)
            clusterDocPairs.add(new Tuple2<Integer, Integer>(docId, docId));


        DataSet<Tuple2<Integer, Integer>> clusters = env.fromCollection(clusterDocPairs);

        int i = 0;
        for(final Tuple2<Integer, Integer> merge : mergesOrder) {
            i++;

            clusters.writeAsCsv(filepath + "/iteration_" + i).setParallelism(1);

            clusters =  clusters.map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

                @Override
                public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                    if(value.f0.equals(merge.f1)) {
                        return new Tuple2<Integer, Integer>(merge.f0, value.f1);
                    }
                    else {
                        return value;
                    }
                }
            });
        }

        clusters.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
