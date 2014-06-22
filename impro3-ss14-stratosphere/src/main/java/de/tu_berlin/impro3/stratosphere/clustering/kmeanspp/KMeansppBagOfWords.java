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
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.util.Collector;

public class KMeansppBagOfWords {
	
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
			String result = "docId:"+id;
			for(Map.Entry<String, Double> itr : wordFreq.entrySet()) {
				result += "|(" + itr.getKey() + ":" + itr.getValue() + ")";
			}
			return result;
		}
	}

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

	public static class MyFunctions implements GenericFunctions<Document> {

		private static final long serialVersionUID = 5510454279473390773L;
		
		private String pointsPath;
		
		public MyFunctions(String filePath) {
			this.pointsPath = filePath;
		}

		@Override
		public DataSet<Document> getDataSet(ExecutionEnvironment env) {
			return env.readCsvFile(pointsPath).fieldDelimiter(' ')
					.types(Integer.class, Integer.class, Double.class)
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
		
		if(args.length < 5) {
			System.out.println(getDescription());
			return;
		}
		int dop = Integer.parseInt(args[0]);
		int k = Integer.parseInt(args[3]);
		int itrs = Integer.parseInt(args[4]);
		KMeansppGeneric<Document> kmp = new KMeansppGeneric<Document>(dop, args[2], k, itrs);
		kmp.run(new MyFunctions(args[1]));
	}
}
