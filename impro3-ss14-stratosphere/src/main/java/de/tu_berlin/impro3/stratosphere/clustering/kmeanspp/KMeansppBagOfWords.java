package de.tu_berlin.impro3.stratosphere.clustering.kmeanspp;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import de.tu_berlin.impro3.stratosphere.clustering.kmeanspp.util.GenericFunctions;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.util.Collector;

/**
 * KMeans++ usage for bag of words
 */
public class KMeansppBagOfWords {

	/**
	 * User defined class for bag of word.
	 */
	public static class Document implements Serializable {
		
		private static final long serialVersionUID = -8646398807053061675L;
		
		public Map<String, Double> wordFreq;
		public Integer id;
		
		public Document() {
			id = -1;
		}

		public Document(Integer id) {
			this.id = id;
			this.wordFreq = new HashMap<String, Double>();
		}
		@Override
		public String toString() {
//			String result = "docId:"+id;
//			for(Map.Entry<String, Double> itr : wordFreq.entrySet()) {
//				result += "|(" + itr.getKey() + ":" + itr.getValue() + ")";
//			}
			String result = Integer.toString(id);
			return result;
		}
	}

	/**
	 * Convert the input data into User defined type - Document.
	 */
	public static final class RecordToDocConverter extends GroupReduceFunction<Tuple3<Integer, Integer, Double>, Document> {

		private static final long serialVersionUID = -8476366121490468956L;

		@Override
		public void reduce(Iterator<Tuple3<Integer, Integer, Double>> values,
				Collector<Document> out) throws Exception {
			if(values.hasNext()) {
				Tuple3<Integer, Integer, Double> elem = values.next();
				Document doc = new Document(elem.f0);
				doc.wordFreq.put(elem.f1.toString(), elem.f2);
				
				while(values.hasNext()) {
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
			String [] splits = value.split(" ");
			if (splits.length > 2) {
				out.collect(new Tuple3<Integer, Integer, Double>(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]), Double.parseDouble(splits[2])));
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
			return env.readTextFile(pointsPath)
					.flatMap(new FilterFirst3Lines())
					.groupBy(0).reduceGroup(new RecordToDocConverter());
		}

		@Override
		public Document add(Document in1, Document in2) {
			for(Map.Entry<String, Double> itr : in2.wordFreq.entrySet()) {
				Double v1 = in1.wordFreq.get(itr.getKey());
				if(v1 == null) {
					in1.wordFreq.put(itr.getKey(), itr.getValue());
				}
				else {
					in1.wordFreq.put(itr.getKey(), itr.getValue() + v1);
				}
			}
			return in1;
		}

		@Override
		public Document div(Document in1, long val) {
			Document out = new Document(in1.id);
			for(Map.Entry<String, Double> itr : in1.wordFreq.entrySet()) {
				out.wordFreq.put(itr.getKey(), itr.getValue()/val);
			}
			return out;
		}

		@Override
		public double distance(Document in1, Document in2) {
			double sum = 0;
			Set<String> added = new HashSet<String>();
			
			for(Map.Entry<String, Double> itr : in2.wordFreq.entrySet()) {
				Double v1 = in1.wordFreq.get(itr.getKey());
				double v2 = itr.getValue();
				if(v1 == null) {
					sum += v2 * v2;
				}
				else {
					sum += (v2 - v1) * (v2 - v1);
					added.add(itr.getKey());
				}
			}
			
			for(Map.Entry<String, Double> itr : in1.wordFreq.entrySet()) {
				if(!added.contains(itr.getKey())) {
					sum += itr.getValue() * itr.getValue();
				}
			}
			return Math.sqrt(sum);
		}

	}

	public static String getDescription() {
		return "Parameters: <numSubTasks> <inputPath> <outputDirectory> <numClusters>  <maxIterations>";
	}
	

	public static void main(String[] args) throws Exception {
		
		if(args.length < 4) {
			System.out.println(getDescription());
			return;
		}
		int k = Integer.parseInt(args[2]);
		int itrs = Integer.parseInt(args[3]);
		KMeansppGeneric<Document> kmp = new KMeansppGeneric<Document>(args[1], k, itrs);
		kmp.run(new MyFunctions(args[0]));
	}
}
